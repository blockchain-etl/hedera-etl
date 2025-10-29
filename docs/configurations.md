# Configurations

## Hedera ETL

Following configurations can be set for the ETL job:

| Name                                  | Default                                                                                          | Description                                                                                                                   |
|---------------------------------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `inputBucket`                         |                                                                                                  | GCS bucket with Record Files                                                                                                  |
| `inputNodes`                          | 0.0.3                                                                                            | List of nodes from which to ingest bytes                                                                                      |
| `ingestionDate`                       |                                                                                                  | Date from which ingest Record files                                                                                           |
| `startAboveFile`                      |                                                                                                  | Start ingestion process above this file (useful if you have to point to exact file the job had to reingest)                   |
| `lastValidHash`                       | 000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000 | Hash of the last ingested file                                                                                                |
| `restrictedAccessDataset`             |                                                                                                  | Output dataset for restricted access dataset                                                                                  |
| `openAccessDataset`                   |                                                                                                  | Output dataset for open access dataset                                                                                        |
| `disableMergeHistoryInput`            | false                                                                                            | Disable merge history input (useful for first-time ingestion)                                                                 |
| `enabledOutputs`                      |                                                                                                  | List of enabled output entities, restricted.tablename enables tables in restricted dataset and open.tablename in the open one |
| `filePollingTimeout` (streaming only) | 5                                                                                                | Name of transactions table                                                                                                    |
| `filePollingTimeout` (streaming only) | 5                                                                                                | Name of transactions table                                                                                                    |
| `startingTimestamp` (streaming only)  |                                                                                                  | Starting timestamp                                                                                                            |
| `rewindToTimestamp` (streaming only)  |                                                                                                  | Quickly rewind to this timestamp before starting ingestion one per second (by default job will rewind itself to now)          |
