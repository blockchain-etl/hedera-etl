# Setup guide

## Install required software

- gcloud - [follow official installation instructions](https://cloud.google.com/sdk/docs/install)
- terraform - [follow official installation instructions for your system](https://developer.hashicorp.com/terraform/install)

## Deploy project

1. Create project in [your GCP](https://console.cloud.google.com/)
2. Create [service account](https://console.cloud.google.com/iam-admin/serviceaccounts) with admin permissions to project and create key & download it – save in safe place
3. Export `GOOGLE_APPLICATION_CREDENTIALS` variable with path to downloaded service account key

```bash
export GOOGLE_APPLICATION_CREDENTIALS={{path}}
```

4. Review terraform variables. Create `terraform.tfvars` file with following content and desired values and put in `terraform/global` and `terraform/dev/infra`

```
project_id = "project ID in GCP"
region = "project region in GCP"
project_name = "your project name"
# if you want receive alerts on your mail configure following variable, you can provide as many emails as you need
email_notifications = {
  Jonh = "john.doe@example.com"
  support = "support@example.com"
}
# these most likely should remain default unless you know what you do
# used to discovery dataflow jobs for Alert Policies
dataflow_job_regex = "hederaetlapplication.*"
```

5. Go to `terraform/global` directory, reviev & set desired values `variables.tf`, next init terraform and create bucket for terraform state files

```bash
cd terraform/global
terraform init
terraform apply
```

6. Go to `terraform/dev/infra`, change bucket in backend section in [providers.tf](dev/infra/providers.tf) file to correct value (should follow `${project_id}-tf-state` schema) reviev & set desired values `variables.tf` and up infrastructure

```bash
cd terraform/dev/infra
terraform init
terraform apply
```

Alternatively you can use [provided script](../scripts/prepare-terraform.sh) to automate in interactive way

## CI

You can use both Gitlab CI or Github Actions. For both you need setup following variables (secrets in Github case)

- `GCP_CREDENTIALS` - json credentials for GCP
- `TFVARS` - content of terraform.tfvars file as described above, in Gitlab this variable should be set as file type
- `GCP_PROJECT_ID` - project ID in GCP
- `STATE_BUCKET_PREFIX` - prefix for state bucket, usually should be same as project_name in terraform.tfvars
