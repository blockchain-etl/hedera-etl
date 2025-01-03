#!/bin/bash

# set env
# TODO – make qa for variables
PROJECT_ID=${PROJECT_ID:-myproject}
SERVICE_ACCOUNT_NAME=${SERVICE_ACCOUNT_NAME:-terraform}
SERVICE_ACCOUNT_DESCRIPTION=${SERVICE_ACCOUNT_DESCRIPTION:-Service account for terraform}
SERVICE_ACCOUNT_DISPLAY_NAME=${SERVICE_ACCOUNT_DISPLAY_NAME:-terraform}
export GOOGLE_APPLICATION_CREDENTIALS=${KEY_FILE:-~/.terraform-gcp-key.json}

gcloud projects create ${PROJECT_ID}

gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
  --description="${SERVICE_ACCOUNT_DESCRIPTION}" \
  --display-name="${SERVICE_ACCOUNT_DISPLAY_NAME}"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/owner"

gcloud iam service-accounts keys create ${KEY_FILE} \
  --iam-account=${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com


  # created key [e44da1202f82f8f4bdd9d92bc412d1d8a837fa83] of type [json] as
  # [/usr/home/username/KEY_FILE] for
  # [SA_NAME@PROJECT_ID.iam.gserviceaccount.com]

cd terraform/global
terraform init
terraform apply -auto-approve

cd ../dev/infra
terraform init
terraform apply -auto-approve
