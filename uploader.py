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
import logging
import math
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import boto3
import botocore
import yaml

# Constants
SINGLE_PART_THRESHOLD: int = 250 * 1024 * 1024  # 250 MiB
MULTIPART_PART_SIZE: int = 50 * 1024 * 1024  # 50 MiB

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


def init_file_entry(status: StatusDict, key: str, size: int) -> None:
    """
    Initialize the status of a file.

    Args:
        status: The status of the upload.
        key: The key of the file.
        size: The size of the file.
    """
    if key not in status:
        status[key] = {
            "size": size,
            "uploaded_bytes": 0,
            "status": "pending",
        }


# ------------------------------------------------------------------------------
# Progress / ETA helpers
# ------------------------------------------------------------------------------


class ProgressTracker:
    """
    Track the progress of the upload.
    """

    def __init__(self, total_bytes: int) -> None:
        """
        Initialize the progress tracker.

        Args:
            total_bytes: The total size of the upload.
        """
        self.total_bytes: int = total_bytes
        self.start_time: float = time.time()
        self.uploaded_bytes: int = 0

    def update(self, delta: int) -> None:
        """
        Update the progress tracker.

        Args:
            delta: The amount of bytes uploaded.
        """
        self.uploaded_bytes += delta

    @staticmethod
    def _eta(start_time: float, completed: int, total: int) -> str:
        """
        Calculate the ETA for the upload.

        Args:
            start_time: The time the upload started.
            completed: The number of bytes uploaded.
            total: The total size of the upload.
        """
        if completed == 0:
            return "--:--:--"
        elapsed = time.time() - start_time
        rate = completed / elapsed
        remaining = (total - completed) / rate if rate else 0
        return str(timedelta(seconds=int(remaining)))

    def eta_total(self) -> str:
        """
        Calculate the ETA for the total upload.
        """
        return self._eta(self.start_time, self.uploaded_bytes, self.total_bytes)


# ------------------------------------------------------------------------------
# S3 upload helpers
# ------------------------------------------------------------------------------


def upload_small_file(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    key: str,
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
        logging.debug(f"DRY-RUN simulate small upload for {key}")
    else:
        logging.debug(f"Uploading small file {key}")
        s3_client.upload_file(
            Filename=str(path),
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ACL": "private"},
        )

    status[key]["status"] = "completed"
    status[key]["uploaded_bytes"] = size
    progress.update(size)
    save_status_file(status, status_file)

    _log_file_progress(key, size, size, progress)


def upload_large_file(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    key: str,
    path: Path,
    status: StatusDict,
    progress: ProgressTracker,
    dry_run: bool,
    status_file: Path,
) -> None:
    """
    Upload a large file to S3.

    Args:
        s3_client: The S3 client.
        bucket: The bucket to upload the file to.
        key: The key of the file.
    """

    entry = status[key]
    size = entry["size"]
    part_count = math.ceil(size / MULTIPART_PART_SIZE)
    uploaded_parts: dict[int, str] = entry.get("parts", {})
    file_uploaded = entry.get("uploaded_bytes", 0)  # running byte counter

    # Create or reuse an upload ID
    if entry.get("upload_id") is None:
        if dry_run:
            upload_id = f"dry-run-{int(time.time())}"
            logging.debug(f"Create fake multipart upload {key} id={upload_id}")
        else:
            logging.debug(f"Initiating multipart upload for {key}")
            upload_id = s3_client.create_multipart_upload(
                Bucket=bucket,
                Key=key,
            )["UploadId"]
        entry["upload_id"] = upload_id
        entry["parts"] = {}
        save_status_file(status, status_file)

    upload_id = entry["upload_id"]

    # Loop over every missing part
    for part_number in range(1, part_count + 1):
        if part_number in uploaded_parts:
            continue

        offset = (part_number - 1) * MULTIPART_PART_SIZE
        bytes_left = size - offset
        part_size = min(MULTIPART_PART_SIZE, bytes_left)

        if dry_run:
            time.sleep(0.25)  # simulate negligible latency
            etag = f"dry-run-etag-{part_number}"
        else:
            logging.debug(f"Uploading part {part_number}/{part_count} for {key}")
            with path.open("rb") as f:
                f.seek(offset)
                data = f.read(part_size)
            etag = s3_client.upload_part(
                Bucket=bucket,
                Key=key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=data,
            )["ETag"]

        # record completed part
        entry["parts"][part_number] = etag

        file_uploaded += part_size  # advance running total
        entry["uploaded_bytes"] = file_uploaded

        save_status_file(status, status_file)

        progress.update(part_size)
        _log_file_progress(
            key,
            file_uploaded,
            size,
            progress,
            part_number,
            part_count,
        )

    # Complete multipart upload
    if dry_run:
        logging.debug(f"Complete multipart upload for {key}")
    else:
        part_info = {
            "Parts": [
                {"PartNumber": n, "ETag": etag}
                for n, etag in sorted(entry["parts"].items())
            ]
        }
        s3_client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload=part_info,
        )

    entry["status"] = "completed"
    save_status_file(status, status_file)


def _log_file_progress(
    key: str,
    file_uploaded: int,
    file_size: int,
    progress: ProgressTracker,
    part_number: int | None = None,
    part_count: int | None = None,
) -> None:
    """
    Log the progress of the upload.

    Args:
        key: The key of the file.
        file_uploaded: The number of bytes uploaded.
        file_size: The size of the file.
    """
    file_pct = file_uploaded / file_size * 100
    total_pct = progress.uploaded_bytes / progress.total_bytes * 100
    file_eta = ProgressTracker._eta(progress.start_time, file_uploaded, file_size)
    total_eta = progress.eta_total()

    part_text = (
        f" part {part_number}/{part_count}," if part_number and part_count else ""
    )
    logging.info(
        f"{key}{part_text} {file_pct:.2f}% complete, file ETA {file_eta} | "
        f"job {total_pct:.2f}% complete, job ETA {total_eta}"
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


def prepare_job(directory: Path, status_path: Path) -> tuple[list[Path], StatusDict]:
    """
    Prepare the job for the upload.

    Args:
        directory: The directory to upload.
        status_path: The path to the status file.
    """
    logging.info(f"Gathering file list for {directory}")
    all_files = [f for f in directory.glob("*") if f.is_file()]
    status = load_status_file(status_path)

    for file_path in all_files:
        rel_key = str(file_path.relative_to(directory).as_posix())
        init_file_entry(status, rel_key, file_path.stat().st_size)

    pending_files = [
        directory / key for key, meta in status.items() if meta["status"] != "completed"
    ]
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
    parser = argparse.ArgumentParser(description="Upload a directory tree to S3")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket name")
    parser.add_argument("--directory", required=True, help="Local directory to upload")
    parser.add_argument(
        "--log-level",
        default="DEBUG" if "--dry-run" in sys.argv else "INFO",
        help="Logging level (DEBUG, INFO, WARNING...)",
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

    # Parse arguments
    args = parser.parse_args()
    configure_logging(args.log_level, args.dry_run)

    # Initialize S3 client
    s3_client = boto3.client("s3")

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
    pending_files, status = prepare_job(root_dir, status_file_path)
    pending_count, pending_bytes = summarize_plan(pending_files, status, root_dir)

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
        f"{pending_bytes / 1024 / 1024:.2f} MiB total"
    )

    # Initialize progress tracker
    progress = ProgressTracker(pending_bytes)

    # Upload files
    for local_path in pending_files:
        key = str(local_path.relative_to(root_dir).as_posix())
        file_size = local_path.stat().st_size

        try:
            if file_size < SINGLE_PART_THRESHOLD:
                upload_small_file(
                    s3_client,
                    args.bucket,
                    key,
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
                    key,
                    local_path,
                    status,
                    progress,
                    args.dry_run,
                    status_file_path,
                )
        except Exception:  # noqa: BLE001
            logging.exception(f"Upload failed for {key}, will retry on next run")
            save_status_file(status, status_file_path)
            raise

    if not args.dry_run:
        logging.info("All uploads completed successfully")
    else:
        logging.info("Completed, status file reflects simulated uploads")


if __name__ == "__main__":
    # Run the main function
    main()
