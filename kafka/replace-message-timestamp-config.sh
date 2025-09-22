#!/bin/bash

SERVER="$1"
BROKER="$2"

TOPICS=$(docker exec -e KAFKA_OPTS="" "$SERVER" kafka-topics --bootstrap-server "$BROKER" --list | tr -d '\r')

for TOPIC in $TOPICS; do
  # Get current topic configs
  CONFIGS=$(docker exec -e KAFKA_OPTS="" "$SERVER" kafka-configs \
    --bootstrap-server "$BROKER" \
    --entity-type topics \
    --entity-name "$TOPIC" \
    --describe | grep "Configs for topic" -A 10)

  # Skip topics that do NOT have the obsolete config
  if echo "$CONFIGS" | grep -q "message.timestamp.difference.max.ms"; then
    echo "Updating topic: $TOPIC"

    # Remove obsolete config and apply Kafka 8 compatible ones
    docker exec -e KAFKA_OPTS="" "$SERVER" kafka-configs \
      --bootstrap-server "$BROKER" \
      --entity-type topics \
      --entity-name "$TOPIC" \
      --alter \
      --add-config log.message.timestamp.type=CreateTime,retention.ms=604800000
    
    docker exec -e KAFKA_OPTS="" "$SERVER" kafka-topics \
      --bootstrap-server "$BROKER" \
      --delete \
      --topic "$TOPIC"

    echo "✅ Topic $TOPIC updated successfully."
  fi
done

echo "All topics processed."
