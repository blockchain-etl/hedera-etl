# Short guide to deploy terraform:

```
export GOOGLE_APPLICATION_CREDENTIALS={{path_to_your_credentials_file}}
cd terraform/global # create gcs backend
terraform apply
cd ../dev/infra
terraform apply
```
