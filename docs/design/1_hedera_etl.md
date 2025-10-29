## Design for Hedera-ETL

### Goal
- Mirror as much of Mirror Node REST API as possible as Big Query dataset
- If a transaction is in record stream, it should be in BigQuery
- Load pipeline should be able to support following scale:
  - 100 TPS (3 billion/year)
  - Avg. txn size = 2kb (16GB/day, 6TB/year)

### Schema

There are two datasets - one containing open access schema but less detailed and restricted one, containing much more
details. Schemas for both datasets are available at
[terraform/dev/infra/templates/bq-schemas](../../terraform/dev/infra/templates/bq-schemas).

- All tables are partitioned daily by either `created` or `modified` column, which corresponds to consensus timestamp
  truncated to microseconds.

### Ingestion

ETL extracts transactions from a Google Storage bucket containing Record Files, e.g. gs://hedera-mainnet-streams,
maps them to corresponding entities and saves them into BigQuery, either using Batch Loads or Streaming Write API,
depending on mode in which ETL is run.

There is also an input of historical data of stateful entities stored in BQ, updated after each batch ingestion

The job can ingest data in two modes, batch and streaming. Batch would be mainly used to ingest bulk of data at once,
one day at a time, once data ingestion would catch up to present time the user should run ETL in streaming mode, which
will ingest current data immediately. In streaming mode job lists files in GCS bucket for each passing minute, with
allowed lateness of 5 minutes by default, meaning that files for current wall time have 5 minutes to appear in GCS
bucket, otherwise they'll be ignored.

#### Invariants

##### At-least-once guarantee from Storage Write API inserts
Exactly-once delivery to BQ is provided by using Storage Write API in exactly once mode.
([ref](https://cloud.google.com/dataflow/docs/guides/write-to-bigquery#:~:text=writing%20to%20BigQuery%3A-,STORAGE_WRITE_API,-.%20In%20this%20mode)).


### Initial data load

To speed up processing, the job can be run in batch mode, which will ingest data for each day SEQUENTIALLY
