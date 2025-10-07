#!/usr/bin/env bash
set -euo pipefail

TOPIC_LIST=("systemconfigs" "lovs" "notificationtemplates" "programs" "keywords" "donations")

recreate_postgres_instance() {
  PGHOST="localhost"
  PGPORT="5432"
  PGUSER="tere"
  PGPASSWORD="mypassword"
  PGDATABASE="TERE"

  local TABLE_NAME="$1"
  local CONSTRAINT_NAME="${TABLE_NAME}_pk"

  local SQL_TEMPLATE="(
    _id varchar NOT NULL,
    \"document\" jsonb NULL,
    CONSTRAINT $CONSTRAINT_NAME PRIMARY KEY (_id)
  );"

  echo "Recreating table $TABLE_NAME..."

  docker exec -i postgres psql -U "$PGUSER" -d "$PGDATABASE" -c "DROP TABLE IF EXISTS $TABLE_NAME; CREATE TABLE $TABLE_NAME $SQL_TEMPLATE"
}

for TOPIC in "${TOPIC_LIST[@]}"; do
  # IFS='.' read -r PART1 PART2 PART3 <<< "$TOPIC"
  PART1="mongo"
  PART2="SLNonCore"
  PART3="$TOPIC"
  recreate_postgres_instance "$TOPIC"

  echo "Delete connector $TOPIC if exists";
  curl -X DELETE "http://localhost:8083/connectors/$TOPIC" >/dev/null 2>&1

  curl -X POST -H "Content-Type: application/json" \
    --data "{
        \"name\": \"mongo.SLNonCore.$TOPIC\",
        \"config\": {
          \"connector.class\": \"com.mongodb.kafka.connect.MongoSourceConnector\",
          \"tasks.max\": \"1\",
          \"connection.uri\": \"mongodb://host.docker.internal:27117,host.docker.internal:27118\",
          \"database\": \"SLNonCore\",
          \"collection\": \"$TOPIC\",
          \"topic.prefix\": \"mongo\",
          \"output.format.value\": \"json\",
          \"key.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
          \"value.converter\": \"org.apache.kafka.connect.storage.StringConverter\",
          \"key.converter.schemas.enable\": \"false\",
          \"value.converter.schemas.enable\": \"false\",
          \"value.converter.string.encoding\": \"UTF-8\"
        }
      }" http://localhost:8083/connectors >/dev/null 2>&1


  PARSED_TOPIC="mongo.SLNonCore.$TOPIC"
  GREP_TOPIC="mongo\.SLNonCore\.$TOPIC"
  
  echo "Pause connector $PARSED_TOPIC";
  curl -X PUT "http://localhost:8083/connectors/mongo.SLNonCore.$TOPIC/pause" >/dev/null 2>&1

  echo "Check topic: $GREP_TOPIC"
  if docker exec -e KAFKA_OPTS="" -i kafka-broker-1  kafka-topics --list --bootstrap-server localhost:29092 | grep "$GREP_TOPIC"; then
      echo "Reset topic: $PARSED_TOPIC"
      docker exec -e KAFKA_OPTS="" -i kafka-broker-1  kafka-topics --delete --topic "$PARSED_TOPIC" --bootstrap-server localhost:29092
  fi

  echo "Creating topic: $PARSED_TOPIC"
  docker exec -e KAFKA_OPTS="" -i kafka-broker-1  kafka-topics --create --topic "$PARSED_TOPIC" --bootstrap-server localhost:29092 --partitions 1 --replication-factor 1 --config cleanup.policy=compact

  echo "Start connector $PARSED_TOPIC";
  curl -X PUT "http://localhost:8083/connectors/$PARSED_TOPIC/resume" >/dev/null 2>&1

  echo "Status connector $PARSED_TOPIC";
  curl -X GET "http://localhost:8083/connectors/$PARSED_TOPIC/status" >/dev/null 2>&1
done


# GREP_TOPIC="SLNonCore"
# CONTAINER="kafka-broker-1"
# BROKER="localhost:29092"

# # List + delete all matching topics
# docker exec -i "$CONTAINER" bash -c "
#   kafka-topics --bootstrap-server $BROKER --list | grep '$GREP_TOPIC' | while read -r TOPIC; do
#     if [ -n \"\$TOPIC\" ]; then
#       echo 'Deleting topic:' \$TOPIC
#       kafka-topics --bootstrap-server $BROKER --delete --topic \"\$TOPIC\"
#     fi
#   done
# "