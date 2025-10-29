# Deployment

## Requirements:
1. Infrastructure (BQ tables, enabled APIs and so on)
2. Created docker repository in the cloud

## Steps

1. Deploy ETL pipeline Flex Template

Run
```sh
./gradlew deployFlexTemplate -Pflex.dockerImage=<region>-docker.pkg.dev/<GCP Project>/<Docker Repository Name>/hedera-etl -Pflex.templateFile=gs://<Bucket to store flex template description>/hedera-etl.json`
```

2. Run ETL
