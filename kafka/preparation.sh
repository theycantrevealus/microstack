#!/usr/bin/env bash
set -euo pipefail

RESET="${1:-persist}"
case "$RESET" in
  reset)
    echo "Reset Mode"
    ;;
  persist)
    echo "Persist Mode"
    ;;
  *)
    echo "❌ Error: Unknown mode '$RESET'. Allowed values: reset | persist"
    exit 1
    ;;
esac

custom_print() {
  echo "$@"
  LAST_LINE="$*"
}

update_status() {
  local status="$1"
  local width=$(( $(tput cols) / 2 ))
  local dots=$((width - ${#LAST_LINE} - ${#status}))
  (( dots < 1 )) && dots=1
  echo -ne "\033[1A\033[2K"
  echo -ne "\r$LAST_LINE"
  printf ".%.0s" $(seq 1 $dots)
  echo "$status"
}

TOPIC_PREFIX="mongo.SLRevamp2"
# Preparing Compacted Topic
for TOPIC in programv2 keywords systemconfigs donation stocks lovs notificationtemplates; do
    if [[ "$RESET" == "reset" ]]; then
        custom_print "📦 Reset topic [compact] - $TOPIC"
        docker exec \
            -e KAFKA_OPTS="" \
            -e SUPPRESS_WARNINGS=true \
            kafka-broker-1  kafka-topics \
            --delete \
            --bootstrap-server localhost:29092 \
            --topic "$TOPIC_PREFIX.$TOPIC" >/dev/null 2>&1 | grep -v '^WARN' || true
        update_status "✅ OK"
    fi

    custom_print "📦 Create topic [compact] - $TOPIC"
    docker exec \
        -e KAFKA_OPTS="" \
        -e SUPPRESS_WARNINGS=true \
        kafka-broker-1  kafka-topics \
        --create \
        --bootstrap-server localhost:29092 \
        --topic "$TOPIC_PREFIX.$TOPIC" \
        --partitions 1 \
        --replication-factor 1 \
        --config cleanup.policy=compact >/dev/null 2>&1 | grep -v '^WARN' || true
    update_status "✅ OK"

    # custom_print "📦 Create Mongo CDC - $TOPIC"
    # curl -X POST -H "Content-Type: application/json" \
    #     --data '{
    #         "name": "$TOPIC_PREFIX",
    #         "config": {
    #         "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    #         "tasks.max": "1",
    #         "connection.uri": "mongodb://host.docker.internal:27117,host.docker.internal:27118",
    #         "database": "SLRevamp2",
    #         "collection": "$TOPIC",
    #         "topic.prefix": "mongo",
    #         "output.format.key": "json",
    #         "output.format.value": "json",

    #         "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    #         "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    #         "key.converter.schemas.enable": "false",
    #         "value.converter.schemas.enable": "false",

    #         "value.converter.string.encoding": "UTF-8"
    #         }
    #     }' http://localhost:8083/connectors
    # update_status "✅ OK"
done