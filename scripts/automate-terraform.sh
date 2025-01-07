#!/bin/bash

set -eu

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

gcloud projects create ${PROJECT_ID}

gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
  --description="${SERVICE_ACCOUNT_DESCRIPTION}" \
  --display-name="${SERVICE_ACCOUNT_DISPLAY_NAME}"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/owner"

gcloud iam service-accounts keys create ${KEY_FILE} \
  --iam-account=${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

# to remove
# cd terraform/global
# terraform init
# terraform apply -auto-approve

cd ../dev/infra
# write variables
cat >terraform.tfvars <<EOL
project_id = ${PROJECT_ID}
region = ${GCP_REGION}
project_name = ${PROJECT_NAME}
env_name = ${ENV_NAME}
EOL
# init & deploy
terraform init
terraform apply -auto-approve
