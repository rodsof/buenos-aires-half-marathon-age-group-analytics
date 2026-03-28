# Terraform Configuration for Buenos Aires Half-Marathon Analytics

This Terraform configuration automates the provisioning of GCP infrastructure for the half-marathon data pipeline.

## What Gets Provisioned

- **BigQuery Datasets**: `staging` and `raw` for data warehouse
- **GCS Bucket**: `buenos-aires-half-marathon` for data lake storage
- **Service Account**: `bruin-half-marathon` with appropriate IAM roles
- **Service Account Key**: JSON credentials file for local development

## Prerequisites

1. **GCP Setup**:
   - GCP project created: `moonlit-state-486723-r7`
   - gcloud CLI installed and configured
   - Terraform installed (v1.0+)

2. **Authentication**:
   ```bash
    gcloud auth activate-service-account --key-file=credentials.json
   ```

## Usage

### 1. Initialize Terraform

```bash
cd terraform
terraform init
```

### 2. Review & Plan

```bash
terraform plan
```

Review the output to see all resources that will be created.

### 3. Apply Configuration

```bash
terraform apply
```

This will:
- Create BigQuery datasets
- Create GCS bucket
- Create service account with IAM roles

### 4. Export Credentials Path

```bash
export GOOGLE_APPLICATION_CREDENTIALS=$(pwd)/../credentials.json
```

### 5. Verify Setup

```bash
gcloud auth activate-service-account --key-file=../credentials.json
bq ls --project_id=moonlit-state-486723-r7
```

## Files

- `main.tf` - Resource definitions
- `variables.tf` - Variable declarations with defaults
- `outputs.tf` - Output values and summaries
- `.gitignore` - Prevents credential/state leaks

## Next Steps

After `terraform apply`:

1. Update your README with outputs:
   ```bash
   terraform output terraform_setup_summary
   ```

2. Run your Bruin pipeline:
   ```bash
   cd ../..
   uv run bruin run half-marathon-bruin/pipeline/pipeline.yml
   ```

## Cleanup

To destroy all provisioned resources:

```bash
terraform destroy
```

⚠️ **Warning**: This will delete datasets and buckets. Use with caution.

## Customization

Edit `variables.tf` to change:
- `gcp_project_id` - Your GCP project
- `gcp_region` - Default region (us-central1)
- `gcs_bucket_name` - Bucket name must be globally unique
- `service_account_id` - Service account identifier
