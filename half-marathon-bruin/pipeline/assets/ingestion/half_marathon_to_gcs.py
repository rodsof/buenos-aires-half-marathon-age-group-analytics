"""@bruin
name: ingestion.half_marathon_gcs
type: python
connection: gcp
secrets:
    - key: gcp
      inject_as: gcp
@bruin"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Ensure local reusable utilities are importable regardless of execution cwd.
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from half_marathon_utils import (
    DEFAULT_DATASET,
    download_dataset,
    ensure_gcs_bucket,
    upload_directory_to_gcs,
)


def _read_bruin_vars() -> dict[str, str]:
    raw_vars = os.getenv("BRUIN_VARS", "{}")
    try:
        return json.loads(raw_vars)
    except json.JSONDecodeError:
        return {}


def materialize() -> list[str]:
    vars_payload = _read_bruin_vars()

    dataset = vars_payload.get("dataset", os.getenv("KAGGLE_DATASET", DEFAULT_DATASET))
    bucket_name = vars_payload.get("gcs_bucket", os.getenv("GCS_BUCKET", "")).strip()
    gcs_prefix = vars_payload.get(
        "gcs_prefix", os.getenv("GCS_PREFIX", "raw/half_marathon_21k")
    )

    if not bucket_name:
        raise ValueError("GCS bucket name is required via gcs_bucket or GCS_BUCKET.")

    local_dataset_dir = download_dataset(dataset)
    gcs_location = vars_payload.get(
        "gcs_location", os.getenv("GCS_LOCATION", "us-central1")
    )
    gcp_project_id = (
        vars_payload.get("gcp_project_id", os.getenv("GCP_PROJECT_ID", "")) or None
    )

    ensure_gcs_bucket(
        bucket_name=bucket_name,
        location=gcs_location,
        project_id=gcp_project_id,
    )

    return upload_directory_to_gcs(
        local_dir=local_dataset_dir,
        bucket_name=bucket_name,
        prefix=gcs_prefix,
    )


if __name__ == "__main__":
    print(json.dumps({"uploaded_files": materialize()}, indent=2))
