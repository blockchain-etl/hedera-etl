#!/usr/bin/env bash

if [[ `which gcloud` == "" ]]; then
  echo "Couldn't find 'gcloud' cli. Make sure Cloud SDK is installed. https://cloud.google.com/sdk/docs#install_the_latest_cloud_sdk_version"
  exit 1
fi

if [[ "${PROJECT_ID}" == "" ]]; then
  echo "PROJECT_ID is not set"
  exit 1
fi

if [[ "${DEPLOYMENT_NAME}" == "" ]]; then
  echo "DEPLOYMENT_NAME is not set. It is needed to name GCP resources."
  exit 1
fi

NAME=${DEPLOYMENT_NAME}

: ${KEYS_DIR:=`pwd`/${NAME}-keys}

: ${BUCKET_PIPELINES:=gs://${PROJECT_ID}-${NAME}-pipelines} # Should be globally unique
: ${BUCKET_ETL_GCS:=gs://${PROJECT_ID}-${NAME}-transactions}

: ${PUBSUB_TOPIC_NAME:=${NAME}-transactions-topic}
: ${PUBSUB_SUBSCRIPTION_ETL_BIGQUERY:=${NAME}-etl-bigquery}

: ${BQ_DATASET:=${NAME}}
: ${BQ_TRANSACTIONS_TABLE:=${PROJECT_ID}:${BQ_DATASET}.transactions}
: ${BQ_TRANSACTION_TYPES_TABLE:=${PROJECT_ID}:${BQ_DATASET}.transaction_types}
: ${BQ_ERRORS_TABLE:=${PROJECT_ID}:${BQ_DATASET}.errors}
: ${BQ_DEDUPE_STATE_TABLE:=${PROJECT_ID}:${BQ_DATASET}.dedupe_state}

: ${SA_ETL_BIGQUERY:=${NAME}-etl-bigquery}
: ${SA_ETL_GCS:=${NAME}-etl-gcs}
: ${SA_DEDUPLICATION:=${NAME}-deduplication-bigquery}
: ${SA_IMPORTER:=${NAME}-importer}

NOW=`date +"%Y%m%d-%H%M%S%z"`
: ${JOB_NAME_ETL_BIGQUERY:=${NAME}-etl-bigquery-${NOW}}
: ${JOB_NAME_ETL_GCS:=${NAME}-etl-gcs-${NOW}}
