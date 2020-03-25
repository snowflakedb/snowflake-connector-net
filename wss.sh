#!/usr/bin/env bash
#
# Run whitesource for components which need versioning

set -x
# Never ever fail for whitesource problems
set +e

SCAN_DIRECTORIES=${PWD}
if [[ -z "${PRODUCT_NAME}" ]]; then
   PRODUCT_NAME="Snowflake_Drivers_Connectors"
fi

PROJECT_NAME="DotNet-Connector"

DATE=$(date +'%m-%d-%Y')

curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar

WSS_CONFIG=wss-net.config

java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
   -c ${WSS_CONFIG} \
   -project ${PROJECT_NAME} \
   -product ${PRODUCT_NAME} \
   -projectVersion ${DATE} \
   -d ${SCAN_DIRECTORIES}

exit 0
