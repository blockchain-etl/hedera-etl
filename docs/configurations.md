# Configurations

## Deduplication

Configurations can be set in `application.yml` file. For more options, refer this
[article](https://www.baeldung.com/spring-properties-file-outside-jar).

Following configurations can be set for deduplication job:

| Name | Default | Description |
|-----------------------------------------------|------------------|----------------------------------------|
| `hedera.dedupe.credentialsLocation`           |                  | Location of Json key file with appropriate permissions in format `file:/path/to/key.json` |
| `hedera.dedupe.datasetName`                   |                  | Name of BigQuery dataset containing the tables |
| `hedera.dedupe.fullFixedRate`                 | 84600000 (24 hr) | Rate at which full deduplication should be run. Format: milliseconds |
| `hedera.dedupe.incrementalFixedRate`          | 300000 (5 min)   | Rate at which incremental deduplication should be run. Format: milliseconds |
| `hedera.dedupe.incrementalProbeInterval`      | 300 (5 min)      | Initial interval used during incremental deduplication when probing for endTimestamp. Format: seconds |
| `hedera.dedupe.metricsEnabled`                | false            | Set to true to publish metrics to Stackdriver |
| `hedera.dedupe.projectId`                     |                  | Project containing the BigQuery tables |
| `hedera.dedupe.stateTableName`                | state            | Name of state table |
| `hedera.dedupe.transactionsTableName`         | transactions     | Name of transactions table |
