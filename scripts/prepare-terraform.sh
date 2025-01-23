#!/bin/bash

set -eu # fail on any error
cd $(dirname "$0")

read -p "This script will configure service account to use with Terraform, setup needed variables and init & deploy terraform code. We assume that you already have some GCP project with linked Billing account [Press enter to continue]"
# set env
read -p "Enter your project id [${PROJECT_ID:-myproject}]: " project_id
PROJECT_ID=${project_id:-myproject}
read -p "Enter your service account name [${SERVICE_ACCOUNT_NAME:-terraform}]: " service_account_name
SERVICE_ACCOUNT_NAME=${service_account_name:-terraform}
SERVICE_ACCOUNT_DESCRIPTION=${SERVICE_ACCOUNT_DESCRIPTION:-Service account for terraform}
SERVICE_ACCOUNT_DISPLAY_NAME=${SERVICE_ACCOUNT_DISPLAY_NAME:-terraform}
read -p "Enter your location for Google credentials file [${KEY_FILE:-~/.terraform-gcp-key.json}]: " key_file
export GOOGLE_APPLICATION_CREDENTIALS=${key_file:-~/.terraform-gcp-key.json}
read -p "Enter your GCP region [${gcp_region:-europe-central2}]: " gcp_region
GCP_REGION=${gcp_region:-europe-central2}
read -p "Enter your project name [${PROJECT_ID}]: " project_name
PROJECT_NAME=${project_name:-$PROJECT_ID}
read -p "Enter your environment name [${env_name:-dev}]: " env_name
ENV_NAME=${env_name:-dev}

# create GCP project & service account for Terraform
# gcloud projects create ${PROJECT_ID}

# service account for Terraform
gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
  --description="${SERVICE_ACCOUNT_DESCRIPTION}" \
  --display-name="${SERVICE_ACCOUNT_DISPLAY_NAME}"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/owner"

gcloud iam service-accounts keys create ${KEY_FILE} \
  --iam-account=${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

# create bucket for terraform state
cd ../terraform/global
# write variables
cat >terraform.tfvars <<EOL
project_id = ${PROJECT_ID}
region = ${GCP_REGION}
project_name = ${PROJECT_NAME}
env_name = ${ENV_NAME}
EOL
# check & deploy
terraform fmt -check
terraform init
terraform validate
terraform apply

# make sure bucket exist before further actions
gcloud storage ls gs://${PROJECT_ID}-tf-state

# deploy core resources
cd $(dirname "$0")
cd ../terraform/dev/infra
# set correct state bucket name
sed -i "s/changeme/${{ PROJECT_NAME }}-tf-state/g" providers.tf
# write variables
cat >terraform.tfvars <<EOL
project_id = ${PROJECT_ID}
region = ${GCP_REGION}
project_name = ${PROJECT_NAME}
env_name = ${ENV_NAME}
EOL
# check & deploy
terraform fmt -check
terraform init
terraform validate
terraform apply

echo 'Deployment finished, from now for future updates you only need run "terraform apply" from terraform/dev/infra directory'
