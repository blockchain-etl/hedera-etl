## Design for Hedera-ETL

### Goal
- Load Hedera transactions into BigQuery
- If a transaction is in record stream, it should be in BigQuery
- Load pipeline should be able to support following scale:
  - 100 TPS (3 billion/year)
  - Avg. txn size = 2kb (16GB/day, 6TB/year)

### Non goals
- Dataset for accounts' details

### Schema

Hedera transactions and receipts are protocol buffers (https://github.com/hashgraph/hedera-protobuf/tree/master/src/main/proto).
BigQuery's storage engine (ColumnIO) and processing engine (Dremel) were designed to work great for protocol buffers.
So the schema would be natural adaptation of Hedera's protobuf (with few deviations, mentioned below) and would look like:
```
transactions (table name, not part of column names)
├── consensusTimestampTruncated: int
├── consensusTimestamp: int
├── transactionType: int
├── entity:
│   ├── shardNum: int
│   ├── realmNum: int
│   ├── entityNum: int
│   └── type: int
├── transaction
│   └── body
│       ├── transactionID
│       │   ├── transactionValidStart: int
│       │   └── accountID: entity
│       ├── nodeAccountID: entity
│       ├── transactionFee: int
│       ├── transactionValidDuration
│       │   └── seconds: int
│       ├─── memo: string
│       ├── cryptoCreateAccount
│       │   ├── initialBalance: int
│       │   └── proxyAccountID: entity
│       ├── contractCall
│       │   ├── gas: int
│       │   ├── amount: int
│       │   └── functionParameters: bytes
│       ├── contractCreateInstance
│       │   ├── gas: int
│       │   ├── initialBalance: int
│       │   ├── proxyAccountID: entity
│       │   ├── constructorParameters: bytes
│       │   └── memo: bytes
│       ├── cryptoAddClaim
│       │   └── claim
│       │       └── hash: bytes
│       ├── consensusSubmitMessage
│       │   └── message: bytes
│       ├── fileCreate
│       │   └── contents: bytes
│       ├── fileAppend
│       │   └── contents: bytes
│       └── fileUpdate
│           └── contents: bytes
├── record
│   ├── receipt
│   │   ├── status: int
│   │   ├── topicRunningHash: bytes
│   │   └── topicSequenceNumber: int
│   ├── transactionHash: bytes
│   ├── transactionFee: int
│   ├── contractCallResult
│   │   ├── contractCallResult: bytes
│   │   ├── errorMessage: string
│   │   └── gasUsed: int
│   ├── contractCreateResult
│   │   ├── contractCallResult: bytes
│   │   ├── errorMessage: string
│   │   └── gasUsed: int
│   └── transferList
│       └── accountAmounts (repeated)
│           ├── accountID: entity
│           └── amount: int
└── nonFeeTransfers
    └── accountAmounts (repeated)
        ├── accountID: entity
        └── amount: int

entity
├── shardNum: int
├── realmNum: int
└── entityNum: int
```

- Table will be partitioned using `consensusTimestampTruncated`.
- One of the possibility was to flatten `transferList` and `nonFeeTransfers` fields. However, the repetition count
for those fields is not fixed and can be even 10+, so keeping them repeated is the only option.
- Max depth allowed in BigQuery schema is 16. Our current max depth is around 6.

Deviations in schema (from PB):
These additional fields have been added since they are often used in queries and would be otherwise cumbersome to filter on:
- consensusTimestamp, entity, nonFeeTransfers
- transactionType: No corresponding field in PB. Deduced based on which field in `oneof data{...}` is set.

### Ingestion

[Hedera Mirror Node](https://github.com/hashgraph/hedera-mirror-node) will be used to ingest transactions from record
stream(GCP Bucket/S3) into BigQuery table. New stream files will be processed immediately and uploaded to BQ table
using streaming inserts.

![Ingestion](../images/hedera_etl_ingestion.png)

#### Invariants

##### At-least-once guarantee from streaming inserts 
Ensuring exactly-once using streaming inserts is not possible
([ref](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataconsistency)). BigQuery's native
deduplication logic can be helpful here to large extent, but it doesn't guarantee exactly-once. In limited
experience of running above job for 10 hours, no errors were seen.
 
 
##### At-most-once guarantee from deduplication job
A deduplication job will run periodically (say every few minutes). It will detect duplicates using
`consensusTimestamp` and delete them from the table. \
One limitation here is: "Streaming inserts reside temporarily in the streaming buffer, which has different
availability characteristics than managed storage". So rows inserted via streaming can not be modified using `DELETE`,
`UPDATE` or `MERGE` ([ref](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-manipulation-language#limitations)).
Which means deduplication job can remove duplicates only ~30min after the insert. 

Two above invariants together guarantee exactly-once (for data older than say 1hr).

BigQuery service's limits worth noting:
- Max streaming row size: 1MB
- Max rows per streaming insert request: 10,000
- Max row inserts: 100k/sec (with deduplication)
- Price: $0.010 per 200MB (Individual rows are calculated using a 1 KB minimum size)

### Initial data load

There will be no special one-off process to load existing data. Mirror node will start processing
the stream from the start and will eventually catchup with latest files on the stream.
While this process will take longer (10+ days), it'll help us gain experience with BigQuery and
build trust in the new ETL process.

 ### Outstanding items
1. Code location?
  - Can we convert downloader, parser, etc in mirror node to libraries?
2. Finalize which transaction fields to store.
    - Should we add non-fee transfers to bigquery?
3. Make BQ queries corresponding to our REST APIs.

