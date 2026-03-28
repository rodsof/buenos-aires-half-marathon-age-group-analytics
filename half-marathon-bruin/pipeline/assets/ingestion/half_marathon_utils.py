from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Iterable, Iterator

import kagglehub
import pandas as pd
from google.cloud import storage

DEFAULT_DATASET = "nicolsvrancovich/buenos-aires-half-marathon-21k-results-20222025"



def ensure_gcs_bucket(
    bucket_name: str,
    location: str = "us-central1",
    project_id: str | None = None,
    client: storage.Client | None = None,
) -> storage.Bucket:
    """Return an existing bucket or create it if it does not exist."""
    storage_client = client or storage.Client(project=project_id)

    bucket = storage_client.lookup_bucket(bucket_name)
    if bucket is not None:
        return bucket

    new_bucket = storage.Bucket(storage_client, name=bucket_name)
    return storage_client.create_bucket(new_bucket, location=location)

def download_dataset(dataset: str = DEFAULT_DATASET) -> Path:
    """Download a Kaggle dataset and return the local directory path."""
    return Path(kagglehub.dataset_download(dataset))


def iter_dataset_files(
    dataset_dir: Path | str,
    include_extensions: Iterable[str] | None = None,
) -> Iterator[Path]:
    """Yield files in a dataset directory, optionally filtered by extension."""
    root = Path(dataset_dir)
    normalized_extensions = None

    if include_extensions is not None:
        normalized_extensions = {
            ext.lower() if ext.startswith(".") else f".{ext.lower()}"
            for ext in include_extensions
        }

    for file_path in sorted(root.rglob("*")):
        if not file_path.is_file():
            continue
        if normalized_extensions and file_path.suffix.lower() not in normalized_extensions:
            continue
        yield file_path


def _build_blob_name(local_file: Path, base_dir: Path, prefix: str = "") -> str:
    relative_path = local_file.relative_to(base_dir).as_posix()
    clean_prefix = prefix.strip("/")

    if clean_prefix:
        return f"{clean_prefix}/{relative_path}"
    return relative_path


def save_to_gcs(
    local_path: Path | str,
    bucket_name: str,
    blob_name: str,
    client: storage.Client | None = None,
) -> str:
    """Upload one local file to GCS and return its gs:// URI."""
    local_file = Path(local_path)
    storage_client = client or storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_file))
    return f"gs://{bucket_name}/{blob_name}"


def upload_directory_to_gcs(
    local_dir: Path | str,
    bucket_name: str,
    prefix: str = "",
    include_extensions: Iterable[str] | None = None,
    client: storage.Client | None = None,
) -> list[str]:
    """Upload all matching files in a directory to GCS and return uploaded URIs."""
    base_dir = Path(local_dir)
    storage_client = client or storage.Client()
    uploaded_uris: list[str] = []

    for local_file in iter_dataset_files(base_dir, include_extensions=include_extensions):
        blob_name = _build_blob_name(local_file, base_dir, prefix)
        uploaded_uris.append(
            save_to_gcs(
                local_path=local_file,
                bucket_name=bucket_name,
                blob_name=blob_name,
                client=storage_client,
            )
        )

    return uploaded_uris


def read_csvs_from_gcs(
    bucket_name: str,
    prefix: str = "",
    client: storage.Client | None = None,
) -> pd.DataFrame:
    """Download all CSV files under a GCS prefix and return a single DataFrame."""
    storage_client = client or storage.Client()
    bucket = storage_client.bucket(bucket_name)
    clean_prefix = prefix.strip("/")

    blobs = [
        b
        for b in bucket.list_blobs(prefix=clean_prefix)
        if b.name.lower().endswith(".csv")
    ]

    if not blobs:
        raise FileNotFoundError(
            f"No CSV files found at gs://{bucket_name}/{clean_prefix}"
        )

    frames = [pd.read_csv(BytesIO(blob.download_as_bytes())) for blob in blobs]
    return pd.concat(frames, ignore_index=True)