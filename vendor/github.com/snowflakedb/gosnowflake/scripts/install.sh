#!/bin/bash -e
if [[ -n "$SNOWFLAKE_AZURE" ]]; then
    openssl aes-256-cbc -k "$super_azure_secret_password" -in parameters_azure.json.enc -out parameters.json -d
elif [[ -n "$SNOWFLAKE_GCP" ]]; then
    openssl aes-256-cbc -k "$super_gcp_secret_password" -in parameters_gcp.json.enc -out parameters.json -d
else
    openssl aes-256-cbc -k "$super_secret_password" -in parameters.json.enc -out parameters.json -d
fi
openssl aes-256-cbc -k "$super_secret_password" -in rsa-2048-private-key.p8.enc -out rsa-2048-private-key.p8 -d
curl -L -s https://github.com/golang/dep/releases/download/v0.3.2/dep-linux-amd64 -o $GOPATH/bin/dep
chmod +x $GOPATH/bin/dep
dep ensure
