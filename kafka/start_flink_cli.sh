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
export DIR_CATALOG=$WORKDIR/catalog
export JVM_ARGS="--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
SSL_DIR=${DIR_CERTIFICATES}
CA_DIR=${SSL_DIR}/ca
CLIENT_DIR=${SSL_DIR}/client

# docker exec flink-jobmanager \
#   bash -c 'for jid in $(/opt/flink/bin/flink list | grep RUNNING | awk "{print \$4}"); do /opt/flink/bin/flink cancel $jid; done'
docker exec flink-jobmanager \
  bash -c 'for jid in $(/opt/flink/bin/flink list | awk "{print \$4}"); do /opt/flink/bin/flink cancel $jid; done'

docker run -it --rm \
    --network kafka-cluster-network \
    -v ${DIR_CERTIFICATES}:/etc/kafka/certificates \
    -v ${DIR_CONFIG}/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml \
    -v ${DIR_CONFIG}/sql-client-defaults.yaml:/opt/flink/conf/sql-client-defaults.yaml \
    -v ${DIR_CONFIG}/initiate.sql:/opt/flink/conf/initiate.sql \
    -v ${DIR_CONFIG}/job.sql:/opt/flink/conf/job.sql \
    -v ${DIR_PLUGINS}/flink-sql-connector-kafka-3.2.0-1.19.jar:/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
    -v ${DIR_PLUGINS}/flink-connector-filesystem-0.10.2.jar:/opt/flink/lib/flink-connector-filesystem-0.10.2.jar \
    -v ${DIR_CATALOG}:/opt/flink/catalog \
    flink:1.19.1-scala_2.12 \
    bin/sql-client.sh -i /opt/flink/conf/initiate.sql -f /opt/flink/conf/job.sql
    # -d /opt/flink/conf/sql-client-defaults.yaml 
    