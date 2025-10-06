#!/usr/bin/env bash
set -euo pipefail
TOPIC=$1

echo "Pause connector $TOPIC";
curl -X PUT "http://localhost:8083/connectors/$TOPIC/pause"

echo "Reset topic: $TOPIC"
docker exec -e KAFKA_OPTS="" -i kafka-broker-1  kafka-topics --delete --topic "$TOPIC" --bootstrap-server localhost:29092

echo "Creating topic: $TOPIC"
docker exec -e KAFKA_OPTS="" -i kafka-broker-1  kafka-topics --create --topic "$TOPIC" --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1 --config cleanup.policy=compact

echo "Start connector $TOPIC";
curl -X PUT "http://localhost:8083/connectors/$TOPIC/resume"

echo "Status connector $TOPIC";
curl -X GET "http://localhost:8083/connectors/$TOPIC/status"