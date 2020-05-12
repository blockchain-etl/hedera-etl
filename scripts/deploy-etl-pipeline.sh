#!/usr/bin/env bash

# Deploys ETL-Pipeline to GCP Dataflow
# This script assumes that resources have been allocated using setup-gcp-resources.sh
# Usage: PROJECT_ID=... DEPLOYMENT_NAME=<testnet/mainnet/etc> KEYS_DIR=... deploy-etl-pipeline.sh

if [[ "${KEYS_DIR}" == "" ]]; then
  echo "KEYS_DIR is not set"
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. ${SCRIPT_DIR}/common.sh

JSON_KEY=${KEYS_DIR}/${SA_ETL_BIGQUERY}.json
if [[ ! -f "${JSON_KEY}" ]]; then
  echo "Couldn't find ${JSON_KEY}. Makes sure KEYS_DIR is correctly set up."
  exit 1
fi
export GOOGLE_APPLICATION_CREDENTIALS=${JSON_KEY}

cd ${SCRIPT_DIR}/../hedera-etl-dataflow

echo "Building and uploading pipeline templates to GCS"

PIPELINE_FOLDER=gs://${BUCKET_NAME}/pipelines/etl-bigquery
mvn clean compile exec:java \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=${PIPELINE_FOLDER}/staging \
 --tempLocation=${PIPELINE_FOLDER}/temp \
 --templateLocation=${PIPELINE_FOLDER}/template \
 --runner=DataflowRunner"

echo "Staring Dataflow job"

SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION_ETL_BIGQUERY}"
gcloud dataflow jobs run etl-pipeline-`date +"%Y%m%d-%H%M%S%z"` \
  --gcs-location=${PIPELINE_FOLDER}/template \
  --service-account-email=${SA_ETL_BIGQUERY}@${PROJECT_ID}.iam.gserviceaccount.com \
  --parameters "inputSubscription=${SUBSCRIPTION},outputTransactionsTable=${BQ_TRANSACTIONS_TABLE},outputErrorsTable=${BQ_ERRORS_TABLE}"
