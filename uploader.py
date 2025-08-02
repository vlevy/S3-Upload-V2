#!/usr/bin/env python3
"""
Robust, resumable, operationally aware uploader for large local files to S3.

Behavior overview
=================
* Files < 250 MiB, upload with a single PUT Object
* Files >= 250 MiB, upload with 50 MiB multipart chunks
* Upload progress and state persist in a human-readable YAML file (`upload_status.yaml`)
* Supports dry-run and cleanup modes
* Emits detailed progress, percentage, and ETA after each successful operation
* Uses the default boto3 credential discovery chain
"""

from __future__ import annotations

import argparse
import fnmatch
import io
import logging
import math
import sys
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import IO, Any

import boto3
import botocore.config
import yaml

args: argparse.Namespace

# Constants
ONE_K = 1024
ONE_M = ONE_K * ONE_K
ONE_G = ONE_M * ONE_K
MULTIPART_PART_SIZE: int = 250 * ONE_M  # 250 MiB
SINGLE_PART_THRESHOLD: int = MULTIPART_PART_SIZE
RATE_CALCULATION_WINDOW_MINUTES: int = 5  # minutes of history for rate calc
RATE_LIMIT_BYTES_PER_SEC = 100 * ONE_G

# Checksum algorithm requested from Amazon S3
# CHECKSUM_ALGO picks the algorithm or disables integrity
# "SHA256", "CRC32C", "CRC64", … or None to turn it off completely
CHECKSUM_ALGO: str | None
CHECKSUM_ALGO = "SHA256"
CHECKSUM_ALGO = None  # Disable data integrity

# Typing helpers
StatusDict = dict[str, Any]


# ------------------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------------------


def configure_logging(level: str = "DEBUG", dry_run: bool = False) -> None:
    # If in dry-run mode, prefix all messages with "DRY-RUN"
    log_format = "%(asctime)s.%(msecs)03d | %(levelname)s | %(lineno)d | %(funcName)s | %(message)s"
    if dry_run:
        log_format = f"DRY-RUN | {log_format}"

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ------------------------------------------------------------------------------
# YAML status helpers
# ------------------------------------------------------------------------------


def load_status_file(path: Path) -> StatusDict:
    """
    Load the status of the upload from a file.

    Args:
        path: The path to the status file.
    """
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def save_status_file(status: StatusDict, path: Path) -> None:
    """
    Save the status of the upload to a file.

    Args:
        status: The status of the upload.
        path: The path to the status file.
    """
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(status, f, sort_keys=False)


def init_file_entry(status: StatusDict, file_name: str, size: int) -> None:
    """
    Initialize the status of a file.

    Args:
        status: The status of the upload.
        file_name: The key of the file.
        size: The size of the file.
    """
    if file_name not in status:
        status[file_name] = {
            "size": size,
            "uploaded_bytes": 0,
            "status": "pending",
        }


def add_to_dict_if(dest: dict[str, str], key: str, value: str | None) -> None:
    """
    Add a key to a dictionary only if the value is truthy.
    """
    if value:
        dest[key] = value


# --------------------------------------------------------------------------
# Progress / ETA helpers
# --------------------------------------------------------------------------


class ProgressTracker:
    """
    Track the progress of the upload.
    """

    def __init__(self, total_bytes: int) -> None:
        self.total_bytes: int = total_bytes
        self.start_time: datetime = datetime.now()
        self.uploaded_bytes: int = 0

        # (timestamp, uploaded_bytes) samples for rolling‑window rate
        self._history: deque[tuple[datetime, int]] = deque()
        self._history.append((self.start_time, 0))

    # ----------------------------- internals ------------------------------

    def _trim_history(self, now: datetime) -> None:
        """Discard samples older than RATE_CALCULATION_WINDOW_MINUTES."""
        cutoff = now - timedelta(minutes=RATE_CALCULATION_WINDOW_MINUTES)
        while len(self._history) > 1 and self._history[0][0] < cutoff:
            self._history.popleft()

    def _current_rate(self) -> float:
        """
        Return bytes‑per‑second averaged over either
        * the full elapsed time (until the window fills), or
        * the most recent RATE_CALCULATION_WINDOW_MINUTES.
        """
        now = datetime.now()
        self._trim_history(now)

        first_ts, first_bytes = self._history[0]
        age = (now - first_ts).total_seconds()
        if age == 0:  # pathological edge
            return 0.0
        bytes_delta = self.uploaded_bytes - first_bytes
        return bytes_delta / age

    # ------------------------------ updates -------------------------------

    def update(self, delta: int) -> None:
        """Advance counters and record a history sample."""
        self.uploaded_bytes += delta
        self._history.append((datetime.now(), self.uploaded_bytes))

    # -------------------------- remaining time ----------------------------

    def time_remaining(self, completed: int, total: int) -> str:
        """Return HH:MM:SS remaining, based on rolling‑window job rate."""
        if completed == 0:
            return "--:--:--"
        rate = self._current_rate()
        remaining = (total - completed) / rate if rate else 0
        return str(timedelta(seconds=int(remaining)))

    def time_remaining_total(self) -> str:
        """Remaining time for the whole job."""
        return self.time_remaining(self.uploaded_bytes, self.total_bytes)

    # ------------------------------ ETA -----------------------------------

    def estimated_completion_time(self, completed: int, total: int) -> str:
        """Return expected finish time as 'Weekday HH:MM'."""
        if completed == 0:
            return "-- --- --:--"
        rate = self._current_rate()
        remaining = (total - completed) / rate if rate else 0
        eta_dt = datetime.now() + timedelta(seconds=int(remaining))
        return eta_dt.strftime("%A %H:%M")

    def eta_total(self) -> str:
        """ETA for the whole job."""
        return self.estimated_completion_time(self.uploaded_bytes, self.total_bytes)


# ------------------------------------------------------------------------------
# S3 upload helpers
# ------------------------------------------------------------------------------


def upload_small_file(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    file_name: str,
    path: Path,
    status: StatusDict,
    progress: ProgressTracker,
    dry_run: bool,
    status_file: Path,
) -> None:
    """
    Upload a small file to S3.

    Args:
        s3_client: The S3 client.
        bucket: The bucket to upload the file to.
        key: The key of the file.
    """
    size = path.stat().st_size

    if dry_run:
        # simulate network latency: none, but walk the same state changes
        logging.debug(f"DRY-RUN simulate small upload for {file_name}")
    else:
        logging.debug(f"Uploading small file {file_name}")
        extra = {
            "ACL": "private",
            "StorageClass": args.storage_class,
        }
        add_to_dict_if(extra, "ChecksumAlgorithm", CHECKSUM_ALGO)

        s3_client.upload_file(
            Filename=str(path),
            Bucket=bucket,
            Key=file_name,
            ExtraArgs=extra,
        )
    status[file_name]["status"] = "completed"
    status[file_name]["uploaded_bytes"] = size
    progress.update(size)
    save_status_file(status, status_file)

    log_file_progress(file_name, size, size, progress)


def upload_large_file(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    file_name: str,
    path: Path,
    status: StatusDict,
    progress: ProgressTracker,
    dry_run: bool,
    status_file: Path,
) -> None:
    """
    Upload a large file to S3, streaming each part through a ThrottledReader
    that smooths bandwidth without changing the 250 MiB part granularity.
    """
    entry = status[file_name]
    size = entry["size"]
    part_count = math.ceil(size / MULTIPART_PART_SIZE)
    uploaded_parts = entry.get("parts", {})
    file_uploaded = entry.get("uploaded_bytes", 0)

    # Create or reuse multipart upload
    if entry.get("upload_id") is None:
        if dry_run:
            upload_id = f"dry-run-{int(datetime.now().timestamp())}"
            logging.debug(f"Create fake multipart upload {file_name} id={upload_id}")
        else:
            logging.debug(f"Initiating multipart upload for {file_name}")
            create_kwargs = {
                "Bucket": bucket,
                "Key": file_name,
                "StorageClass": args.storage_class,
            }
            add_to_dict_if(create_kwargs, "ChecksumAlgorithm", CHECKSUM_ALGO)
            upload_id = s3_client.create_multipart_upload(**create_kwargs)["UploadId"]
        entry["upload_id"] = upload_id
        entry["parts"] = {}
        save_status_file(status, status_file)

    upload_id = entry["upload_id"]

    for part_number in range(1, part_count + 1):
        if part_number in uploaded_parts:
            continue

        offset = (part_number - 1) * MULTIPART_PART_SIZE
        part_size = min(MULTIPART_PART_SIZE, size - offset)

        if dry_run:
            time.sleep(0.1)
            etag = f"dry-run-etag-{part_number}"
        else:
            logging.debug(f"Uploading part {part_number}/{part_count} for {file_name}")
            with path.open("rb") as f:
                f.seek(offset)
                # buffer in 4 MiB windows so ThrottledReader can sleep frequently
                buffered = io.BufferedReader(f, buffer_size=4 * ONE_M)
                # enforce both rate and max-bytes for this part
                reader = ThrottledReader(
                    buffered,
                    max_bytes=part_size,
                )
                resp = s3_client.upload_part(
                    Bucket=bucket,
                    Key=file_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=reader,
                    ContentLength=part_size,
                    **({"ChecksumAlgorithm": CHECKSUM_ALGO} if CHECKSUM_ALGO else {}),
                )
                etag = resp["ETag"]

        entry["parts"][part_number] = etag
        file_uploaded += part_size
        entry["uploaded_bytes"] = file_uploaded
        save_status_file(status, status_file)

        progress.update(part_size)
        log_file_progress(
            file_name,
            file_uploaded,
            size,
            progress,
            part_number,
            part_count,
        )

    if dry_run:
        logging.debug(f"Complete multipart upload for {file_name}")
    else:
        part_info = {
            "Parts": [
                {"PartNumber": n, "ETag": e} for n, e in sorted(entry["parts"].items())
            ]
        }
        complete_kwargs = {
            "Bucket": bucket,
            "Key": file_name,
            "UploadId": upload_id,
            "MultipartUpload": part_info,
        }
        add_to_dict_if(complete_kwargs, "ChecksumAlgorithm", CHECKSUM_ALGO)
        s3_client.complete_multipart_upload(**complete_kwargs)

    entry["status"] = "completed"
    save_status_file(status, status_file)


def log_file_progress(
    file_name: str,
    file_uploaded: int,
    file_size: int,
    progress: ProgressTracker,
    part_number: int | None = None,
    part_count: int | None = None,
) -> None:
    """
    Log the progress of the upload, including effective bytes per second.
    """
    file_pct = file_uploaded / file_size * 100
    total_pct = progress.uploaded_bytes / progress.total_bytes * 100

    file_remaining = progress.time_remaining(file_uploaded, file_size)
    file_eta = progress.estimated_completion_time(file_uploaded, file_size)
    total_remaining = progress.time_remaining_total()
    total_eta = progress.eta_total()

    effective_rate = progress._current_rate()

    part_text = f"{part_number}/{part_count}" if part_number and part_count else ""
    logging.info(
        f'FILE "{file_name}" {part_text} {file_pct:.2f}% '
        f"{file_remaining} left, {file_eta} ETA | "
        f"JOB {total_pct:.2f}% {total_remaining} left, {total_eta} ETA | "
        f"Rate: {effective_rate / (ONE_M):.2f} MiB/s"
    )


# ------------------------------------------------------------------------------
# Cleanup helper
# ------------------------------------------------------------------------------


def cleanup_multipart_uploads(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    local_keys: set[str],
    dry_run: bool,
) -> None:
    """
    Abort multipart uploads for keys not present locally.

    Args:
        s3_client: The S3 client.
        bucket: The bucket to scan for stale multipart uploads.
        local_keys: The keys of the files that are present locally.
    """
    logging.info(f"Scanning bucket {bucket} for stale multipart uploads")
    paginator = s3_client.get_paginator("list_multipart_uploads")
    aborted = 0

    for page in paginator.paginate(Bucket=bucket):
        for upload in page.get("Uploads", []):
            key = upload["Key"]
            upload_id = upload["UploadId"]
            if key not in local_keys:
                logging.info(f"Aborting stale upload {key} (UploadId {upload_id})")
                if not dry_run:
                    s3_client.abort_multipart_upload(
                        Bucket=bucket,
                        Key=key,
                        UploadId=upload_id,
                    )
                aborted += 1

    if aborted == 0:
        logging.info("No stale multipart uploads found")
    else:
        logging.info(f"Finished cleanup, {aborted} multipart uploads aborted")


# ------------------------------------------------------------------------------
# Job planner
# ------------------------------------------------------------------------------


def collect_files(
    root: Path,
    recursive: bool,
    include_pat: str,
    exclude_pats: list[str],
) -> list[Path]:
    """
    Return all files under *root* that match *include_pat* and
    do **not** match any pattern in *exclude_pats*.
    """
    iterator = (
        root.rglob(include_pat or "*") if recursive else root.glob(include_pat or "*")
    )
    files: list[Path] = []

    for path in iterator:
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        if any(fnmatch.fnmatch(rel, pat) for pat in exclude_pats):
            continue
        files.append(path)

    return sorted(files)


def prepare_job(
    directory: Path,
    status_path: Path,
    recursive: bool,
    include_pat: str,
    exclude_pats: list[str],
) -> tuple[list[Path], StatusDict]:
    """
    Build the pending‑file list, applying include/exclude filters.
    """
    logging.info(f"Gathering file list for {directory}")
    all_files = collect_files(directory, recursive, include_pat, exclude_pats)

    status = load_status_file(status_path)
    for file_path in all_files:
        file_name = str(file_path.relative_to(directory).as_posix())
        init_file_entry(status, file_name, file_path.stat().st_size)

    # Include only files matching the specifier, even if
    # there are other unfinished files in the status file
    pending_files = sorted(
        directory / key
        for key, meta in status.items()
        if meta["status"] != "completed" and key in [f.name for f in all_files]
    )
    return pending_files, status


def summarize_plan(
    pending_files: list[Path],
    status: StatusDict,
    directory: Path,
) -> tuple[int, int]:
    """
    Summarize the plan for the upload.

    Args:
        pending_files: The files that are pending upload.
        status: The status of the upload.
        directory: The directory to upload.
    """
    total_bytes = sum(
        status[str(f.relative_to(directory).as_posix())]["size"] for f in pending_files
    )

    # Subtract the size of the already uploaded parts of pending files
    for file in pending_files:
        file_status = status[str(file.relative_to(directory).as_posix())]
        total_bytes -= file_status["uploaded_bytes"]

    return len(pending_files), total_bytes


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


def main() -> None:
    """
    Main function.
    """
    global args
    parser = argparse.ArgumentParser(description="Upload a directory tree to S3")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket name")
    parser.add_argument("--directory", required=True, help="Local directory to upload")
    parser.add_argument(
        "--log-level",
        default="DEBUG" if "--dry-run" in sys.argv else "INFO",
        help="Logging level (DEBUG, INFO, WARNING...)",
    )
    parser.add_argument(
        "--storage-class",
        default="STANDARD",
        choices=[
            "STANDARD",
            "STANDARD_IA",
            "INTELLIGENT_TIERING",
            "GLACIER",
            "DEEP_ARCHIVE",
        ],
        help="S3 storage class for new objects",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate uploads and update status without touching S3",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Abort multipart uploads for keys not present locally",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recurse into sub‑directories when gathering files",
    )
    parser.add_argument(
        "--include",
        default="*",
        metavar="GLOB",
        help="Glob pattern to include (default: '*')",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="GLOB",
        help="Glob pattern(s) to exclude; can be given multiple times",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="Show the final list of files to upload, then continue",
    )

    # Parse arguments
    args = parser.parse_args()
    configure_logging(args.log_level, args.dry_run)

    # Initialize S3 client
    config = botocore.config.Config(
        retries={
            "mode": "adaptive",  # adaptive == exponential back‑off + client‑side rate limiting
            "max_attempts": 10,  # bump from AWS default (3 or 4)
        },
        connect_timeout=10,  # seconds
        read_timeout=60,
    )

    s3_client = boto3.client("s3", config=config)

    # Get root directory and create the status file path
    root_dir = Path(args.directory).expanduser().resolve()
    if not root_dir.is_dir():
        logging.error(
            f"Specified directory {root_dir} does not exist or is not a directory"
        )
        sys.exit(1)

    status_file_name: str = (
        "upload_status.yaml" if not args.dry_run else "upload_status_dry_run.yaml"
    )
    status_file_path = (root_dir / status_file_name).resolve()

    # Prepare the job
    pending_files, status = prepare_job(
        root_dir,
        status_file_path,
        recursive=args.recursive,
        include_pat=args.include,
        exclude_pats=args.exclude,
    )
    pending_count, pending_bytes = summarize_plan(pending_files, status, root_dir)

    if args.list_files:
        logging.info("Files to be uploaded:")
        for i, p in enumerate(pending_files):
            logging.info(f'  {i + 1}. "{p.relative_to(root_dir).as_posix()}"')
        # continue with normal processing

    if args.cleanup:
        # Cleanup multipart uploads
        cleanup_multipart_uploads(
            s3_client,
            args.bucket,
            {str(p.relative_to(root_dir).as_posix()) for p in pending_files},
            args.dry_run,
        )
        return

    if pending_count == 0 and not args.cleanup:
        logging.info("All files already uploaded, exiting")
        return

    if pending_count == 0:
        logging.info("No pending uploads, exiting")
        return

    logging.info(
        f"Starting upload, {pending_count} pending files, "
        f"{pending_bytes / 1024 / 1024 / 1024:.2f} GiB total"
    )

    # Initialize progress tracker
    progress = ProgressTracker(pending_bytes)

    # Upload files
    for local_path in pending_files:
        file_name = str(local_path.relative_to(root_dir).as_posix())
        file_size = local_path.stat().st_size

        try:
            if file_size < SINGLE_PART_THRESHOLD:
                upload_small_file(
                    s3_client,
                    args.bucket,
                    file_name,
                    local_path,
                    status,
                    progress,
                    args.dry_run,
                    status_file_path,
                )
            else:
                upload_large_file(
                    s3_client,
                    args.bucket,
                    file_name,
                    local_path,
                    status,
                    progress,
                    args.dry_run,
                    status_file_path,
                )
        except Exception:  # noqa: BLE001
            logging.exception(f"Upload failed for {file_name}, will retry on next run")
            save_status_file(status, status_file_path)
            raise

    if not args.dry_run:
        logging.info("All uploads completed successfully")
    else:
        logging.info("Completed, status file reflects simulated uploads")


class ThrottledReader:
    """
    Wrap a file-like object and enforce a max read rate (bytes/sec),
    optionally limiting total bytes read.
    """

    def __init__(self, fp: IO[bytes], max_bytes: int | None = None) -> None:
        self.fp = fp
        self.max_bytes = max_bytes
        self.when_started = datetime.now()
        self.bytes_sent = 0

    def read(self, num_bytes_to_read: int = -1) -> bytes:
        if self.max_bytes is not None:
            bytes_left = self.max_bytes - self.bytes_sent
            if bytes_left <= 0:
                return b""
            if not (0 < num_bytes_to_read < bytes_left):
                num_bytes_to_read = bytes_left

        #        logging.info(f"Reading {num_bytes_to_read} bytes")
        chunk = self.fp.read(num_bytes_to_read)
        chunk_len = len(chunk)
        if chunk_len == 0:
            return chunk

        self.bytes_sent += chunk_len
        elapsed = (datetime.now() - self.when_started).total_seconds()
        expected_duration = self.bytes_sent / RATE_LIMIT_BYTES_PER_SEC
        if expected_duration > elapsed:
            sleep_time = expected_duration - elapsed
            logging.info(f"Sleeping for {sleep_time:.2f} seconds to limit rate")
            time.sleep(sleep_time)

        return chunk

    # clamp so boto3 *thinks* the file is only `max_bytes` long
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_END:
            offset = self.max_bytes + offset
        elif whence == io.SEEK_CUR:
            offset = self.pos + offset
        self.pos = max(0, min(offset, self.max_bytes))
        self.fp.seek(self.start_offset + self.pos)
        return self.pos

    def tell(self) -> int:
        return self.pos

    def __len__(self) -> int:  # extra hint for botocore
        return self.max_bytes

    def seekable(self) -> bool:
        return True


if __name__ == "__main__":
    try:
        # Run the main function
        main()
    except KeyboardInterrupt:
        logging.warning("Interrupted by Ctrl-C")
        sys.exit(130)  # 130 is the exit code for Ctrl-C
