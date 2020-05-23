#!/usr/bin/env bash

# This script assumes that jar, deploy.sh and hedera-deduplication-bigquery.service files are all in same directory.

NAME="hedera-deduplication-bigquery"

INSTALL_DIR="/hedera/${NAME}"
mkdir -p ${INSTALL_DIR}

# Stop the service for upgrade, if present
systemctl stop "${NAME}" || true

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${SCRIPT_DIR}

DEST_JAR="${INSTALL_DIR}/${NAME}.jar"
echo "Copying new jar"
rm -rf ${DEST_JAR}
cp ${NAME}-*.jar ${DEST_JAR}

echo "Setting up systemd service"
cp ${NAME}.service /etc/systemd/system
systemctl daemon-reload
systemctl enable ${NAME}.service

echo "Installation completed successfully"

echo "Make sure ${INSTALL_DIR}/config/application.yml is setup correctly. It would look like:"
echo "hedera:
  dedupe:
    projectId: <required>
    datasetName: <required>
    credentialsLocation: <required>
    metricsEnabled: true"

echo "Then run 'systemctl start ${NAME}' to start deduplication service."
