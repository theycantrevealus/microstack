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

docker run -it --rm \
    --network kafka-cluster-network \
    -v ${DIR_CERTIFICATES}:/etc/kafka/certificates \
    -v ${DIR_CONFIG}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml \
    -v ${DIR_CONFIG}/sql-client-defaults.yaml:/opt/flink/conf/sql-client-defaults.yaml \
    -v ${DIR_CONFIG}/init.sql:/opt/flink/conf/init.sql \
    -v ${DIR_PLUGINS}/flink-sql-connector-kafka-3.2.0-1.19.jar:/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    flink:1.19.1-scala_2.12 \
    bin/sql-client.sh -d /opt/flink/conf/sql-client-defaults.yaml \
    # -f /opt/flink/conf/init.sql