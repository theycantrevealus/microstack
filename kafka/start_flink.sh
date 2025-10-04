#!/usr/bin/env bash
set -euo pipefail
WORKDIR=${PWD}
export BROKER_PASS=brokerpass
export CLIENT_PASS=clientpass
export CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:latest kafka-storage random-uuid)
export DIR_PROPERTIES=$WORKDIR/properties
export DIR_JAR=$WORKDIR/jar
export DIR_CONFIG=$WORKDIR/config
export DIR_CERTIFICATES=$WORKDIR/certificates
export DIR_SHELL=$WORKDIR/shell
export DIR_PLUGINS=$WORKDIR/plugins
SSL_DIR=${DIR_CERTIFICATES}
CA_DIR=${SSL_DIR}/ca
CLIENT_DIR=${SSL_DIR}/client

# cd java/tere-cdc-process
# mvn clean package -U && cp "$WORKDIR/java/tere-cdc-process/target/tere-cdc-process-1.0-SNAPSHOT.jar" "$WORKDIR/jar" && cd "$WORKDIR"


docker compose -p "flink" -f flink.yml up -d