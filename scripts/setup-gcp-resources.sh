#!/usr/bin/env bash
# Creates GCP resources like Service Accounts, GCS buckets, BigQuery datasets and tables, etc for various components of
# hedera-etl
# For more details, refer to docs/deployment.md.
#
# Usage: PROJECT_ID=... DEPLOYMENT_NAME=<testnet/mainnet/etc> setup-gcp-resources.sh
# Optionally, KEYS_DIR can be set to specify the directory where service accounts' keys would be downloaded. Default to
# './${DEPLOYMENT_NAME}-keys'

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. ${SCRIPT_DIR}/common.sh

#### Functions ####
create_service_account_with_roles()
{
  local sa_name=$1
  local roles=$2
  local description="$3"

  # Create service account
  gcloud iam service-accounts create ${sa_name} \
    --project=${PROJECT_ID} \
    --description="${description}"

  # Assign roles to the service account
  for role in ${roles}; do
    gcloud projects add-iam-policy-binding ${PROJECT_ID} \
      --member serviceAccount:${sa_name}@${PROJECT_ID}.iam.gserviceaccount.com \
      --role ${role} > /dev/null # Project's complete IAM policy is dumped to console otherwise
    echo "Assigned role ${role} to ${sa_name}"
  done
}

create_service_account_key()
{
  local sa_name=$1
  local key_filename=${KEYS_DIR}/${sa_name}.json
  # Download service account's key
  gcloud iam service-accounts keys create ${key_filename} \
    --iam-account=${sa_name}@${PROJECT_ID}.iam.gserviceaccount.com
}

#### Base resources ####
mkdir -p ${KEYS_DIR}

# Create BigQuery dataset and tables
bq mk --project_id=${PROJECT_ID} ${NAME}
DATASET_NAME=${BQ_DATASET} ${SCRIPT_DIR}/create-tables.sh

# Create PubSub topic for transactions
gcloud pubsub topics create ${PUBSUB_TOPIC_NAME} --project=${PROJECT_ID}

# Create GCS bucket for dataflow pipelines
gsutil mb -p ${PROJECT_ID} gs://${BUCKET_NAME}

#### Resources for ETL to BigQuery ####
gcloud pubsub subscriptions create ${PUBSUB_SUBSCRIPTION_ETL_BIGQUERY} \
  --project=${PROJECT_ID} \
  --topic=${PUBSUB_TOPIC_NAME} \
  --message-retention-duration=7d \
  --expiration-period=never

create_service_account_with_roles \
  ${SA_ETL_BIGQUERY} \
  "roles/bigquery.dataEditor roles/dataflow.worker roles/pubsub.subscriber roles/storage.admin" \
  "For pubsub --> bigquery dataflow controller"

create_service_account_key ${SA_ETL_BIGQUERY}

#### Resources for Deduplication task ####
create_service_account_with_roles \
  ${SA_DEDUPLICATION} \
  "roles/bigquery.dataEditor roles/bigquery.jobUser roles/monitoring.metricWriter" \
  "For BigQuery deduplication task"

create_service_account_key ${SA_DEDUPLICATION}

#### Resources for Hedera Mirror Importer ####
create_service_account_with_roles \
  ${SA_IMPORTER} "roles/pubsub.publisher" "For hedera mirror node importer (publishes to PubSub)"

create_service_account_key ${SA_IMPORTER}
