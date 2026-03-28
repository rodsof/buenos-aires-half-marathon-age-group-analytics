"""@bruin

name: setup.ensure_staging_dataset
connection: gcp

secrets:
  - key: gcp
    inject_as: gcp

@bruin"""

from __future__ import annotations

import json
import os
from google.cloud import bigquery
from google.oauth2 import service_account


def _read_bruin_vars() -> dict[str, str]:
    raw_vars = os.getenv("BRUIN_VARS", "{}")
    try:
        return json.loads(raw_vars)
    except json.JSONDecodeError:
        return {}


def materialize() -> dict[str, str]:
    vars_payload = _read_bruin_vars()

    project_id = vars_payload.get("gcp_project_id", "moonlit-state-486723-r7")
    dataset_id = vars_payload.get("target_dataset", "staging")
    location = vars_payload.get("gcs_location", "us-central1")

    # Get credentials from Bruin connection
    raw_conn = os.environ.get("gcp")
    if not raw_conn:
        raise RuntimeError("No gcp connection found in environment")

    conn = json.loads(raw_conn)

    # Handle service account JSON
    if conn.get("service_account_json"):
        info = conn["service_account_json"]
        if isinstance(info, str):
            info = json.loads(info)
        creds = service_account.Credentials.from_service_account_info(info)
    elif conn.get("service_account_file"):
        creds = service_account.Credentials.from_service_account_file(
            conn["service_account_file"]
        )
    else:
        raise RuntimeError("No service account credentials found in gcp connection")

    # Create BigQuery client
    client = bigquery.Client(project=project_id, credentials=creds)

    # Create dataset
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location

    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_ref} ready at location {location}")
    except Exception as e:
        raise RuntimeError(f"Failed to create dataset {dataset_ref}: {e}")

    return {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "location": location,
        "status": "ready",
    }


if __name__ == "__main__":
    print(json.dumps(materialize(), indent=2))
