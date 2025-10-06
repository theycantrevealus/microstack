#!/usr/bin/env bash
set -euo pipefail

WORKDIR=${PWD}
export BROKER_PASS=brokerpass
export CLIENT_PASS=clientpass
export DIR_PROPERTIES=$WORKDIR/properties
export DIR_JAR=$WORKDIR/jar
export DIR_CONFIG=$WORKDIR/config
export DIR_CERTIFICATES=$WORKDIR/certificates
export DIR_SHELL=$WORKDIR/shell
export DIR_PLUGINS=$WORKDIR/plugins
SSL_DIR=${DIR_CERTIFICATES}
CA_DIR=${SSL_DIR}/ca
CLIENT_DIR=${SSL_DIR}/client

prepare_flink_properties() {
  props="${DIR_CONFIG}/flink-conf.yaml"
  rm -f "$props"
  echo "Rewrite config"
  echo "jobmanager.rpc.address: flink-jobmanager" >> "$props"
  echo "jobmanager.rpc.port: 6123" >> "$props"
  echo "rest.address: flink-jobmanager" >> "$props"
  echo "rest.port: 9081" >> "$props"
  echo "rest.bind-address: 0.0.0.0" >> "$props"
  echo "taskmanager.numberOfTaskSlots: 10" >> "$props"
  echo "taskmanager.memory.process.size: 2048m" >> "$props"
  echo "jobmanager.memory.process.size: 2048m" >> "$props"
  echo "blob.server.port: 6124" >> "$props"
  echo "query.server.port: 6125" >> "$props"
  echo "execution.restart-strategy: fixed-delay" >> "$props"
  echo "execution.restart-strategy.fixed-delay.attempts: 3" >> "$props"
  echo "execution.restart-strategy.fixed-delay.delay: 10s" >> "$props"
  echo "rest.flamegraph.enabled: true" >> "$props"
  echo "jobmanager.memory.heap.size: 1024m" >> "$props"
  echo "jobmanager.memory.flink.size: 1024m" >> "$props"
}

prepare_flink_properties
for library in tere-cdc-process tere-transaction-stream; do
      echo "Build library: $library"
      mvn clean package -U -f java/$library/pom.xml
      cp "$WORKDIR/java/$library/target/$library-1.0-SNAPSHOT.jar" "$WORKDIR/jar/$library.jar"
      echo "Build library: $library ... done"
  done    
echo "Stop all flink services"
docker-compose -p "flink" -f flink.yml down -v >/dev/null 2>&1

echo "Start all flink services"
docker compose -p "flink" -f flink.yml up -d >/dev/null 2>&1

echo "Done"