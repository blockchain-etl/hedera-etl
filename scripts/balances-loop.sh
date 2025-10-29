#!/bin/bash

set -e
set -x

START_DATE="2025-09-09"
INCREMENT="+1 day"

PROJECT_ID="hedera-etl-bq"

# INGESTION_DATE=$(date +%Y-%m-%d)
REGION="us-central1"

export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"

## MAIN PROGRAM
while true; do
    if [ -e stop_balances_loop ]; then
        echo "Poison pill detected. Good bye"
        exit 0
    fi
    LATEST_TABLE="hedera_technical.balance_latest_$(date -d "${START_DATE} -1 day" +"%Y%m%d")"
    echo "Running balances for date ${START_DATE}"
    WINDOW_IN_MINUTES=10
    POLLING_WAIT=300
    python3 -m hedera-balances -p hedera-etl-bq -d hedera_restricted -x hedera_technical -w $POLLING_WAIT -m $WINDOW_IN_MINUTES -t native -e $START_DATE -l $LATEST_TABLE
    START_DATE=$(date -d "${START_DATE} +1 day" --iso)
    sleep 10s
done
