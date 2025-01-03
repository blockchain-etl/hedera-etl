# Setup guide

1. Create project in [your GCP](https://console.cloud.google.com/)
2. Create [service account](https://console.cloud.google.com/iam-admin/serviceaccounts) with admin permissions to project and create key & download it – save in safe place
3. Export `GOOGLE_APPLICATION_CREDENTIALS` variable with path to downloaded service account key

```bash
export GOOGLE_APPLICATION_CREDENTIALS={{path}}
```

4. Go to `terraform/global` directory, reviev & set desired values `variables.tf`, next init terraform and create bucket for terraform state files (TODO – make it optional)

```bash
cd terraform/global
terraform init
terraform apply
```

5. Go to `terraform/dev/infra`, reviev & set desired values `variables.tf` and up infrastructure

```bash
cd terraform/dev/infra
terraform init
terraform apply
```

Alternatively you can use [provided script](../scripts/automate-terraform.sh) to automate this - WIP

# Short guide to deploy terraform:

```
export GOOGLE_APPLICATION_CREDENTIALS={{path_to_your_credentials_file}}
cd terraform/global # create gcs backend
terraform apply
cd ../dev/infra
terraform apply
```
