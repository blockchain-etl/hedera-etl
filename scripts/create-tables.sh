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
  ${SCRIPT_DIR}/../hedera-etl-bigquery/src/main/resources/transactions-schema.json

bq mk \
  --table \
  --description "Hedera ETL Errors" \
  ${BQ_ERRORS_TABLE} \
  ${SCRIPT_DIR}/../hedera-etl-bigquery/src/main/resources/errors-schema.json

bq mk \
  --table \
  --description "BigQuery deduplication job state" \
  ${BQ_DEDUPE_STATE_TABLE} \
  ${SCRIPT_DIR}/../hedera-etl-bigquery/src/main/resources/state-schema.json

bq mk \
  --table \
  --description "Transaction Types" \
  ${BQ_TRANSACTION_TYPES_TABLE} \
  ${SCRIPT_DIR}/../hedera-etl-bigquery/src/main/resources/transaction-types-schema.json

echo "INSERT INTO ${PROJECT_ID}.${DATASET_NAME}.transaction_types (id, name) VALUES \
(7, 'CONTRACTCALL'), \
(8, 'CONTRACTCREATEINSTANCE'), \
(9, 'CONTRACTUPDATEINSTANCE'), \
(10, 'CRYPTOADDLIVEHASH'), \
(11, 'CRYPTOCREATEACCOUNT'), \
(12, 'CRYPTODELETE'), \
(13, 'CRYPTODELETELIVEHASH'), \
(14, 'CRYPTOTRANSFER'), \
(15, 'CRYPTOUPDATEACCOUNT'), \
(16, 'FILEAPPEND'), \
(17, 'FILECREATE'), \
(18, 'FILEDELETE'), \
(19, 'FILEUPDATE'), \
(20, 'SYSTEMDELETE'), \
(21, 'SYSTEMUNDELETE'), \
(22, 'CONTRACTDELETEINSTANCE'), \
(23, 'FREEZE'), \
(24, 'CONSENSUSCREATETOPIC'), \
(25, 'CONSENSUSUPDATETOPIC'), \
(26, 'CONSENSUSDELETETOPIC'), \
(27, 'CONSENSUSSUBMITMESSAGE')" | bq query --nouse_legacy_sql



