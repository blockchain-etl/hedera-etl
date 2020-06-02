#!/usr/bin/env bash

# Deploys ETL-Pipeline to GCP Dataflow
# This script assumes that resources have been allocated using setup-gcp-resources.sh
#
# Usage: PROJECT_ID=... DEPLOYMENT_NAME=<testnet/mainnet/etc> KEYS_DIR=... deploy-etl-pipeline.sh
# Note that KEYS_DIR should be absolute path.
# Optional parameters:
# - ETL_TO_GCS: Set to 'true' to start ETL pipeline which will read from PubSub and write to GCS.

set -e

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

cd ${SCRIPT_DIR}/../hedera-etl-bigquery

echo "Building and uploading template (to GCS) for ETL BigQuery"

PIPELINE_FOLDER=${BUCKET_PIPELINES}/etl-bigquery
mvn clean compile exec:java \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=${PIPELINE_FOLDER}/staging \
 --tempLocation=${PIPELINE_FOLDER}/temp \
 --templateLocation=${PIPELINE_FOLDER}/template \
 --runner=DataflowRunner"

echo "Staring ETL BigQuery on Dataflow"

SUBSCRIPTION="projects/${PROJECT_ID}/subscriptions/${PUBSUB_SUBSCRIPTION_ETL_BIGQUERY}"
gcloud dataflow jobs run ${JOB_NAME_ETL_BIGQUERY} \
  --gcs-location=${PIPELINE_FOLDER}/template \
  --service-account-email=${SA_ETL_BIGQUERY}@${PROJECT_ID}.iam.gserviceaccount.com \
  --project=${PROJECT_ID} \
  --parameters \
inputSubscription=${SUBSCRIPTION},\
outputTransactionsTable=${BQ_TRANSACTIONS_TABLE},\
outputErrorsTable=${BQ_ERRORS_TABLE}

if [[ "${ETL_TO_GCS}" == "true" ]]; then
  TEMPLATE_LOCATION=gs://dataflow-templates/2020-03-31-01_RC00/Cloud_PubSub_to_GCS_Text
  TOPIC="projects/${PROJECT_ID}/topics/${PUBSUB_TOPIC_NAME}"
  gcloud dataflow jobs run ${JOB_NAME_ETL_GCS} \
    --gcs-location=${TEMPLATE_LOCATION} \
    --service-account-email=${SA_ETL_GCS}@${PROJECT_ID}.iam.gserviceaccount.com \
    --project=${PROJECT_ID} \
    --parameters \
inputTopic=${TOPIC},\
outputDirectory=${BUCKET_ETL_GCS}/,\
outputFilenamePrefix=transactions-,\
outputFilenameSuffix=.txt
fi
