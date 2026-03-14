"""
utils/gcs_storage.py
~~~~~~~~~~~~~~~~~~~~
Google Cloud Storage helpers for persisting the SQLite database
across Cloud Run container restarts.

Cloud Run containers use an ephemeral filesystem: every cold start or
re-deployment wipes /app/data/sentinel.db.  Syncing to GCS solves this:

    Pipeline start  →  download_db()  pulls the latest DB from GCS
    Pipeline end    →  upload_db()    pushes the updated DB back to GCS

The GCS bucket name comes from the GCS_BUCKET environment variable.
Cloud Run service accounts have built-in GCS access when the bucket is
in the same project — no extra key files needed.

Both functions are no-ops (returning False) when:
  - google-cloud-storage is not installed
  - GCS_BUCKET is empty / not set
  - The bucket or blob is inaccessible

They log a warning rather than raising, so the pipeline continues even
when GCS is unavailable (e.g. local development without credentials).
"""

import logging
from pathlib import Path

logger = logging.getLogger("sentinel.gcs_storage")

# GCS object key for the database file.
# Must stay consistent between upload and download.
_GCS_BLOB_NAME = "data/sentinel.db"


def download_db(bucket_name: str, local_path: Path) -> bool:
    """
    Download sentinel.db from *bucket_name* to *local_path*.

    Returns True on success, False if the blob does not exist yet or on
    any error (network, auth, missing library).  Creates the parent
    directory of *local_path* if it does not exist.
    """
    if not bucket_name:
        return False

    try:
        from google.cloud import storage as gcs  # noqa: PLC0415
    except ImportError:
        logger.warning("GCS: google-cloud-storage not installed – skipping download")
        return False

    try:
        client = gcs.Client()
        blob   = client.bucket(bucket_name).blob(_GCS_BLOB_NAME)

        if not blob.exists():
            logger.info(
                "GCS: %s not found in bucket %s – starting with empty DB",
                _GCS_BLOB_NAME, bucket_name,
            )
            return False

        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(local_path))
        size_mb = round(local_path.stat().st_size / 1_048_576, 2)
        logger.info(
            "GCS: downloaded gs://%s/%s → %s  (%s MB)",
            bucket_name, _GCS_BLOB_NAME, local_path, size_mb,
        )
        return True

    except Exception as exc:
        logger.exception("GCS: download failed (bucket=%s): %s", bucket_name, exc)
        return False


def upload_db(bucket_name: str, local_path: Path) -> bool:
    """
    Upload *local_path* to *bucket_name* as sentinel.db.

    Returns True on success, False if the file does not exist or on any
    error (network, auth, missing library).
    """
    if not bucket_name:
        return False

    if not local_path.exists():
        logger.warning("GCS: upload skipped – %s does not exist", local_path)
        return False

    try:
        from google.cloud import storage as gcs  # noqa: PLC0415
    except ImportError:
        logger.warning("GCS: google-cloud-storage not installed – skipping upload")
        return False

    try:
        client = gcs.Client()
        blob   = client.bucket(bucket_name).blob(_GCS_BLOB_NAME)
        blob.upload_from_filename(
            str(local_path),
            content_type="application/octet-stream",
        )
        size_mb = round(local_path.stat().st_size / 1_048_576, 2)
        logger.info(
            "GCS: uploaded %s → gs://%s/%s  (%s MB)",
            local_path, bucket_name, _GCS_BLOB_NAME, size_mb,
        )
        return True

    except Exception as exc:
        logger.exception("GCS: upload failed (bucket=%s): %s", bucket_name, exc)
        return False
