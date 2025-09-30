# Requirements

You need to download prometheus jmx first from

```bash
wget https://github.com/prometheus/jmx_exporter/releases/download/v1.4.0/jmx_prometheus_javaagent-1.4.0.jar
```

and put it into **jar** folder

## Start the base

```bash
./start.sh [mode] [profile]
```

Remark :

1. mode: dev | prod | flink
2. profile : full

---

**Warning :** \
**Start with prod mode will consume a lot of resource.** \
**Run on your own risk**

---

## Preparation

### Flink Config

```yaml
jobmanager.rpc.address: flink-jobmanager
jobmanager.rpc.port: 6123
rest.address: flink-jobmanager
rest.port: 9081
rest.bind-address: 0.0.0.0
taskmanager.numberOfTaskSlots: 10
taskmanager.memory.process.size: 2048m
jobmanager.memory.process.size: 1024m
blob.server.port: 6124
query.server.port: 6125
```

```yaml
jobmanager.rpc.address: flink-jobmanager
jobmanager.rpc.port: 6123
rest.address: flink-jobmanager
rest.port: 9081
rest.bind-address: 0.0.0.0
taskmanager.numberOfTaskSlots: 10
taskmanager.memory.process.size: 2048m
jobmanager.memory.process.size: 1024m
blob.server.port: 6124
query.server.port: 6125
execution.restart-strategy: fixed-delay
execution.restart-strategy.fixed-delay.attempts: 3
execution.restart-strategy.fixed-delay.delay: 10s
```

```yaml
execution:
  planner: blink
  type: streaming
  result-mode: table
  parallelism: 1
catalogs:
  - name: default_catalog
    type: generic_in_memory
deployment:
  gateway:
    type: standalone
    cluster-host: flink-jobmanager
    cluster-port: 9081
tables:
  - name: mongo_keywords
    connector: kafka
    topic: mongo.SLRevamp2.keywords
    properties.bootstrap.servers: kafka-broker-1:29092
    properties.security.protocol: PLAINTEXT
    properties.sasl.mechanism: SCRAM-SHA-512
    properties.sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";
    properties.ssl.truststore.location: /etc/kafka/certificates/client/kafka.client.truststore.jks
    properties.ssl.truststore.password: clientpass
    properties.ssl.endpoint.identification.algorithm:
    format: json
    json.ignore-parse-errors: true
    scan.startup.mode: earliest-offset
```

```yaml
execution:
  planner: blink
  type: streaming
  result-mode: changelog
  parallelism: 1
catalogs:
  - name: default_catalog
    type: generic_in_memory
deployment:
  gateway:
    type: standalone
    cluster-host: flink-jobmanager
    cluster-port: 9081
tables:
  - name: mongo_keywords
    connector: kafka
    topic: mongo.SLRevamp2.keywords
    properties.bootstrap.servers: kafka-broker-1:29092
    properties.security.protocol: PLAINTEXT
    properties.sasl.mechanism: SCRAM-SHA-512
    properties.sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";
    format: json
    json.ignore-parse-errors: true
    scan.startup.mode: earliest-offset
    schema: |
      _id ROW<`_data` STRING>
      operationType STRING
      fullDocument ROW<`_id` ROW<`$oid` STRING>, `keywords` STRING, `points` INT>
      ns ROW<`db` STRING, `coll` STRING>
      documentKey ROW<`_id` ROW<`$oid` STRING>>
      clusterTime ROW<`$timestamp` ROW<`t` BIGINT, `i` INT>>
      wallTime ROW<`$date` BIGINT>
```

### 1. Add Mongo Connector

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "mongo-keywords-source",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
      "connection.uri": "mongodb://host.docker.internal:27117,host.docker.internal:27118",
      "database": "SLRevamp2",
      "collection": "keywords",
      "publish.full.document.only": "true",
      "output.format.value": "json",
      "output.format.key": "json",
      "topic.prefix": "mongo"
    }
  }' \
  http://localhost:8083/connectors
```

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "mongo-source-connector",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
      "tasks.max": "1",
      "connection.uri": "mongodb://host.docker.internal:27117,host.docker.internal:27118",
      "database": "SLRevamp2",
      "collection": "keywords",
      "topic.prefix": "mongo",
      "output.format.value": "json",
      "output.format.key": "json",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false"
    }
  }' http://localhost:8083/connectors
```

### 2. Check connector config on control-center

### 3. Connect to Flink SQL Client

Run as service:

```bash
docker exec -it flink-sql-client bin/sql-client.sh embedded
```

Run as CLI:

```bash
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
  -v ${DIR_CERTIFICATES}:/etc/kafka/certificates \
  -v ${DIR_CONFIG}/sql-client-defaults.yaml:/opt/flink/conf/sql-client-defaults.yaml \
  -v ${DIR_CONFIG}/init.sql:/opt/flink/conf/init.sql \
  -v ${DIR_PLUGINS}/flink-sql-connector-kafka-3.2.0-1.19.jar:/opt/flink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar \
  flink:1.19.1-scala_2.12 \
  bin/sql-client.sh -d /opt/flink/conf/sql-client-defaults.yaml -f /opt/flink/conf/init.sql
```

### 4. Add connector

```sql
SET execution.parallelism = 10;
SET execution.checkpointing.interval = '10s';
CREATE TABLE TERE_keywords (
  _id STRING,
  keyword STRING,
  points INT
) WITH (
  'connector' = 'kafka',
  'topic' = 'mongo.SLRevamp2.keywords',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092',
  'properties.security.protocol' = 'PLAINTEXT',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";',
  'properties.ssl.truststore.location' = '/etc/kafka/certificates/client/kafka.client.truststore.jks',
  'properties.ssl.truststore.password' = 'clientpass',
  'properties.ssl.endpoint.identification.algorithm' = '',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'scan.startup.mode' = 'earliest-offset'
);
SELECT * FROM TERE_keywords;
CREATE TABLE sink_table (
  _id STRING,
  keyword STRING,
  points INT
) WITH (
  'connector' = 'print'
);
INSERT INTO sink_table SELECT * FROM TERE_keywords;
```

## Run Flink Partially

```bash
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
docker compose -p "flink" -f flink.yml up -d
```
