# Deployment

## Requirements:
1. BigQuery tables : transactions, errors, dedupe_state
1. PubSub topic for transactions
1. GCS bucket : Used for dataflow templates, staging and as temp location
1. ETL Pipeline from PubSub to BigQuery:
    1. PubSub subscription
    1. Service account with following roles: BigQuery Data Editor, Dataflow Worker, Pub/Sub Subscriber, and Storage
       Admin
1. Deduplication Task
    1. Service account with following roles: BigQuery Data Editor, BigQuery Job User, Monitoring Metric
       Writer
1. Mirror Importer
    1. Service account with following roles: PubSub Publisher
1. (Optional) ETL Pipeline from PubSub to GCS
    1. GCS Bucket: For output of pipeline
    1. Service account with following roles: Dataflow Worker, Pub/Sub Editor (for creating subscription), and
       Storage Admin

Resource creation can be automated using [setup-gcp-resources.sh](../scripts/setup-gcp-resources.sh).
[Google Cloud SDK](https://cloud.google.com/sdk/docs) is required to run the script.

## Steps

1. Deploy ETL pipeline

Use [deploy-etl-pipeline.sh](../scripts/deploy-etl-pipeline.sh) script to deploy the etl pipeline to GCP Dataflow.

1. Deploy Deduplication task

TODO

1. Deploy Hedera Mirror Node Importer to publish transactions to the pubsub topic. See
Mirror Nodes [installation](https://github.com/hashgraph/hedera-mirror-node/blob/master/docs/installation.md) and
[configuration](https://github.com/hashgraph/hedera-mirror-node/blob/master/docs/configuration.md#importer) for more
details.


