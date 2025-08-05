# S3 Uploader

A resilient, **resume-capable** command-line tool for moving very large files or entire directory trees to Amazon S3.  
It is designed for day-to-day operational use where interruptions, bandwidth caps, and long-running transfers are expected.

---

## Features

| Capability                      | Details                                                                                                                                                    |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Single-part / multipart**     | Files **< 250 MiB** go as a single `PUT Object`; larger files upload in **250 MiB parts**.                                                                 |
| **Resumable**                   | Upload state (bytes sent, multipart IDs, ETAGs) is kept in a human-readable `upload_status.yaml` so the program can restart after failure or stop/restart. |
| **Bandwidth limiting**          | `-L / --limit` sets a per-uploader cap (MiB/s). Default is **5 MiB/s**.                                                                                    |
| **Time-based unlimited window** | `-U / --unlimited-span HH:MM-HH:MM` disables throttling during the given daily window (spans over midnight are supported).                                 |
| **Dry-run mode**                | `--dry-run` touches no AWS resources but exercises all code paths and updates a separate status file.                                                      |
| **Cleanup helper**              | `--cleanup` aborts multipart uploads in the bucket that no longer correspond to local files.                                                               |
| **Progress logging**            | Per-file percentage, overall ETA, rolling throughput, and remaining time are logged in real time.                                                          |
| **Filtering**                   | `--include/--exclude` glob patterns and `--recursive` traversal make it easy to select files.                                                              |
| **Credential handling**         | Relies on the default **boto3 credential chain** (env-vars, shared config, IAM role, etc.).                                                                |

---

## Requirements

- Python **3.9 +**
- **boto3** = 1.34
- **PyYAML** = 6.0

```bash
pip install -r requirements.txt
# or simply
pip install boto3 pyyaml
```
