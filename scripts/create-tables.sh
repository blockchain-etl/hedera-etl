#!/usr/bin/env bash

# Creates BigQuery tables needed by Dataflow and Deduplication jobs.
#
# Usage: PROJECT_ID=... DEPLOYMENT_NAME=<testnet/mainnet/etc> create-tables.sh
# Optionally, table names can be set using BQ_TRANSACTIONS_TABLE, BQ_ERRORS_TABLE, BQ_DEDUPE_STATE_TABLE

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
. ${SCRIPT_DIR}/common.sh

bq mk \
  --table \
  --description "Hedera network transactions" \
  --time_partitioning_field consensusTimestampTruncated \
  --time_partitioning_type DAY \
  --clustering_fields transactionType \
  ${BQ_TRANSACTIONS_TABLE} \
  ${SCRIPT_DIR}/../hedera-etl-dataflow/src/main/resources/schema.json

bq mk \
  --table \
  --description "Hedera ETL Errors" \
  ${BQ_ERRORS_TABLE} \
  ${SCRIPT_DIR}/../hedera-etl-dataflow/src/main/resources/errors_schema.json

bq mk \
  --table \
  --description "BigQuery deduplication job state" \
  ${BQ_DEDUPE_STATE_TABLE} \
  ${SCRIPT_DIR}/../hedera-dedupe-bigquery/state-schema.json

