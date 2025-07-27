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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import botocore
import yaml

# Constants
SINGLE_PART_THRESHOLD: int = 250 * 1024 * 1024  # 250 MiB
MULTIPART_PART_SIZE: int = 50 * 1024 * 1024  # 50 MiB
STATUS_FILE_NAME: str = "upload_status.yaml"

# Typing helpers
StatusDict = dict[str, Any]


# ------------------------------------------------------------------------------
# Logging configuration
# ------------------------------------------------------------------------------


def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s.%(msecs)03d - %(levelname)s - "
        "[%(pathname)s:%(lineno)d - %(funcName)s()] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ------------------------------------------------------------------------------
# YAML status helpers
# ------------------------------------------------------------------------------


def load_status_file(path: Path) -> StatusDict:
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}


def save_status_file(status: StatusDict, path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(status, f)


def init_file_entry(status: StatusDict, key: str, size: int) -> None:
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
    def __init__(self, total_bytes: int) -> None:
        self.total_bytes: int = total_bytes
        self.start_time: float = time.time()
        self.uploaded_bytes: int = 0

    def update(self, delta: int) -> None:
        self.uploaded_bytes += delta

    @staticmethod
    def _eta(start_time: float, completed: int, total: int) -> str:
        if completed == 0:
            return "--:--:--"
        elapsed = time.time() - start_time
        rate = completed / elapsed
        remaining = (total - completed) / rate if rate else 0
        return str(timedelta(seconds=int(remaining)))

    def eta_total(self) -> str:
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
) -> None:
    size = path.stat().st_size
    if dry_run:
        logging.info(f"DRY-RUN single-part upload {key} ({size} bytes)")
        return

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
    _log_file_progress(key, size, size, progress)


def upload_large_file(
    s3_client: botocore.client.BaseClient,
    bucket: str,
    key: str,
    path: Path,
    status: StatusDict,
    progress: ProgressTracker,
    dry_run: bool,
) -> None:
    size = path.stat().st_size
    part_count = math.ceil(size / MULTIPART_PART_SIZE)

    entry = status[key]
    uploaded_parts: dict[int, str] = entry.get("parts", {})
    uploaded_bytes = entry.get("uploaded_bytes", 0)

    if dry_run:
        pending = part_count - len(uploaded_parts)
        logging.info(
            f'DRY-RUN multipart upload "{key}", {pending}/{part_count} parts pending'
        )
        return

    if entry.get("upload_id") is None:
        logging.debug(f"Initiating multipart upload for {key}")
        upload_id = s3_client.create_multipart_upload(Bucket=bucket, Key=key)[
            "UploadId"
        ]
        entry["upload_id"] = upload_id
        entry["parts"] = {}
        save_status_file(status, Path(STATUS_FILE_NAME))

    upload_id = entry["upload_id"]

    for part_number in range(1, part_count + 1):
        if str(part_number) in uploaded_parts:
            continue

        offset = (part_number - 1) * MULTIPART_PART_SIZE
        bytes_left = size - offset
        part_size = min(MULTIPART_PART_SIZE, bytes_left)

        with path.open("rb") as f:
            f.seek(offset)
            data = f.read(part_size)

        logging.debug(f"Uploading part {part_number}/{part_count} for {key}")
        response = s3_client.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=data,
        )
        etag = response["ETag"]
        entry["parts"][str(part_number)] = etag
        entry["uploaded_bytes"] = (
            uploaded_bytes + part_number * MULTIPART_PART_SIZE
            if part_number < part_count
            else size
        )
        save_status_file(status, Path(STATUS_FILE_NAME))

        uploaded_bytes = entry["uploaded_bytes"]
        progress.update(part_size)
        _log_file_progress(
            key,
            uploaded_bytes,
            size,
            progress,
            part_number,
            part_count,
        )

    part_info = {
        "Parts": [
            {"PartNumber": int(n), "ETag": etag}
            for n, etag in sorted(entry["parts"].items(), key=lambda x: int(x[0]))
        ]
    }
    s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload=part_info,
    )
    entry["status"] = "completed"
    save_status_file(status, Path(STATUS_FILE_NAME))


def _log_file_progress(
    key: str,
    file_uploaded: int,
    file_size: int,
    progress: ProgressTracker,
    part_number: int | None = None,
    part_count: int | None = None,
) -> None:
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
    total_bytes = sum(
        status[str(f.relative_to(directory).as_posix())]["size"] for f in pending_files
    )
    return len(pending_files), total_bytes


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload a directory tree to S3")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket name")
    parser.add_argument("--directory", required=True, help="Local directory to upload")
    parser.add_argument(
        "--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING...)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze and log actions without uploading or writing S3 objects",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Abort multipart uploads for keys not present locally",
    )

    args = parser.parse_args()
    configure_logging(args.log_level)

    root_dir = Path(args.directory).expanduser().resolve()
    if not root_dir.is_dir():
        logging.error(
            f"Specified directory {root_dir} does not exist or is not a directory"
        )
        sys.exit(1)

    s3_client = boto3.client("s3")
    status_file = root_dir / STATUS_FILE_NAME

    pending_files, status = prepare_job(root_dir, status_file)
    pending_count, pending_bytes = summarize_plan(pending_files, status, root_dir)

    if pending_count == 0 and not args.cleanup:
        logging.info("All files already uploaded, exiting")
        return

    if args.cleanup:
        cleanup_multipart_uploads(
            s3_client,
            args.bucket,
            {str(p.relative_to(root_dir).as_posix()) for p in pending_files},
            args.dry_run,
        )
        return

    if pending_count == 0:
        logging.info("No pending uploads, exiting")
        return

    logging.info(
        f"Starting upload, {pending_count} pending files, "
        f"{pending_bytes / 1024 / 1024:.2f} MiB total"
    )

    progress = ProgressTracker(pending_bytes)

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
                )
            save_status_file(status, status_file)
        except Exception:  # noqa: BLE001
            logging.exception(f"Upload failed for {key}, will retry on next run")
            save_status_file(status, status_file)
            raise

    if not args.dry_run:
        logging.info("All uploads completed successfully")


if __name__ == "__main__":
    main()
