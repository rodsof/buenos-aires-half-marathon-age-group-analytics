from __future__ import annotations

import json
import os
from io import BytesIO
from pathlib import Path
from typing import Iterable, Iterator

import kagglehub
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

DEFAULT_DATASET = "nicolsvrancovich/buenos-aires-half-marathon-21k-results-20222025"


def build_storage_client(
    project_id: str | None = None,
    client: storage.Client | None = None,
) -> storage.Client:
    """Construye un cliente de GCS usando la conexión de Bruin 'gcp'."""
    if client is not None:
        return client

    raw_conn = os.environ.get("gcp")
    if not raw_conn:
        raise RuntimeError(
            "No se encontró la conexión 'gcp' en variables de entorno. "
            "Agregá `secrets: - key: gcp` en el asset."
        )

    conn = json.loads(raw_conn)

    # Caso 1: la conexión trae el JSON completo de la service account
    if conn.get("service_account_json"):
        info = conn["service_account_json"]
        if isinstance(info, str):
            info = json.loads(info)

        creds = service_account.Credentials.from_service_account_info(info)
        return storage.Client(
            project=project_id or conn.get("project_id") or info.get("project_id"),
            credentials=creds,
        )

    # Caso 2: la conexión trae una ruta a archivo
    if conn.get("service_account_file"):
        creds = service_account.Credentials.from_service_account_file(
            conn["service_account_file"]
        )
        return storage.Client(
            project=project_id or conn.get("project_id"),
            credentials=creds,
        )

    raise RuntimeError(
        "La conexión 'gcp' no contiene ni `service_account_json` ni `service_account_file`."
    )


def ensure_gcs_bucket(
    bucket_name: str,
    location: str = "us-central1",
    project_id: str | None = None,
    client: storage.Client | None = None,
) -> storage.Bucket:
    storage_client = build_storage_client(project_id=project_id, client=client)

    bucket = storage_client.lookup_bucket(bucket_name)
    if bucket is not None:
        return bucket

    new_bucket = storage.Bucket(storage_client, name=bucket_name)
    return storage_client.create_bucket(new_bucket, location=location)


def download_dataset(dataset: str = DEFAULT_DATASET) -> Path:
    return Path(kagglehub.dataset_download(dataset))


def iter_dataset_files(
    dataset_dir: Path | str,
    include_extensions: Iterable[str] | None = None,
) -> Iterator[Path]:
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
        if (
            normalized_extensions
            and file_path.suffix.lower() not in normalized_extensions
        ):
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
    project_id: str | None = None,
) -> str:
    local_file = Path(local_path)
    storage_client = build_storage_client(project_id=project_id, client=client)
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
    project_id: str | None = None,
) -> list[str]:
    base_dir = Path(local_dir)
    storage_client = build_storage_client(project_id=project_id, client=client)
    uploaded_uris: list[str] = []

    for local_file in iter_dataset_files(
        base_dir, include_extensions=include_extensions
    ):
        blob_name = _build_blob_name(local_file, base_dir, prefix)
        uploaded_uris.append(
            save_to_gcs(
                local_path=local_file,
                bucket_name=bucket_name,
                blob_name=blob_name,
                client=storage_client,
                project_id=project_id,
            )
        )

    return uploaded_uris


def read_csvs_from_gcs(
    bucket_name: str,
    prefix: str = "",
    client: storage.Client | None = None,
    project_id: str | None = None,
) -> pd.DataFrame:
    storage_client = build_storage_client(project_id=project_id, client=client)
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
