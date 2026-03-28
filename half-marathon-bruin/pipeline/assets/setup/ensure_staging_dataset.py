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
import subprocess


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

    cmd = [
        "bq",
        f"--location={location}",
        "mk",
        "-d",
        f"{project_id}:{dataset_id}",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    # bq mk returns non-zero if dataset already exists.
    # Treat that case as success to keep setup idempotent.
    if result.returncode != 0:
        stderr = result.stderr or ""
        already_exists = (
            "Already Exists" in result.stdout or "already exists" in result.stdout
        )
        if not already_exists:
            raise RuntimeError(
                f"Failed to create dataset {project_id}:{dataset_id}\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )
        else:
            print(f"Dataset {project_id}:{dataset_id} already exists. Continuing...")

    return {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "location": location,
        "status": "ready",
    }


if __name__ == "__main__":
    print(json.dumps(materialize(), indent=2))
