# Requirements

You need to download prometheus jmx first from

```bash
wget https://github.com/prometheus/jmx_exporter/releases/download/v1.4.0/jmx_prometheus_javaagent-1.4.0.jar
```

and put it into **jar** folder

```bash
wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/4.0.0-2.0/flink-connector-jdbc-postgres-4.0.0-2.0.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

and put it into **plugins** folder

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
blob.server.port: 6124
taskmanager.memory.process.size: 2048m
execution.restart-strategy.fixed-delay.attempts: 3
rest.flamegraph.enabled: true
classloader.resolve-order: parent-first
jobmanager.rpc.address: flink-jobmanager
jobmanager.memory.process.size: 1024m
table.catalog.tere_catalog: filesystem
jobmanager.rpc.port: 6123
query.server.port: 6125
rest.bind-address: 0.0.0.0
rest.port: 9081
execution.restart-strategy.fixed-delay.delay: 10s
table.catalog.tere_catalog.path: file:///opt/flink/catalog
taskmanager.numberOfTaskSlots: 10
rest.address: flink-jobmanager
execution.restart-strategy: fixed-delay
table.catalog.tere_catalog.default-database: default
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
rest.flamegraph.enabled: true
jobmanager.memory.heap.size: 1024m
jobmanager.memory.flink.size: 1024m
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

properties/flink.kafka.properties

```properties
properties.bootstrap.servers=kafka-broker-1:29092
properties.group.id=flink-consumer-keyword
properties.security.protocol=PLAINTEXT
properties.sasl.mechanism=SCRAM-SHA-512
properties.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";
properties.ssl.truststore.location=/etc/kafka/certificates/client/kafka.client.truststore.jks
properties.ssl.truststore.password=clientpass
properties.ssl.endpoint.identification.algorithm=
format=json
json.ignore-parse-errors=true
scan.startup.mode=earliest-offset
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
      "key.converter.schemas.enable": "true",
      "value.converter.schemas.enable": "true",
      "value.converter.string.encoding": "UTF-8"
    }
  }' http://localhost:8083/connectors
```

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "mongo.SLRevamp2",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
      "tasks.max": "1",
      "connection.uri": "mongodb://host.docker.internal:27117,host.docker.internal:27118",
      "database": "SLRevamp2",
      "collection": "keywords",
      "topic.prefix": "mongo",
      "output.format.key": "json",
      "output.format.value": "json",

      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",

      "value.converter.string.encoding": "UTF-8"
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

CREATE TABLE TERE_keywords (
  operationType STRING,
  clusterTime ROW<t BIGINT, i BIGINT>,
  wallTime ROW<`$date` BIGINT>,
  fullDocument ROW<
    `_id` ROW<`$oid` STRING>,
    keyword STRING,
    points INT
  >,
  ns ROW<db STRING, coll STRING>,
  documentKey ROW<
    `_id` ROW<`$oid` STRING>
  >
) WITH (
  'connector' = 'kafka',
  'topic' = 'mongo.SLRevamp2.keywords',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092',
  'properties.group.id' = 'flink-consumer-keyword',
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

CREATE TABLE sink_table (
  fullDocument.`_id`.`$oid` AS id,
  fullDocument.keyword AS keyword,
  fullDocument.points AS points
) WITH (
  'connector' = 'print'
);

INSERT INTO sink_table
SELECT
  fullDocument.`_id`.`$oid` AS id,
  fullDocument.keyword AS keyword,
  fullDocument.points AS points
FROM TERE_keywords;
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

### Test Insert Data

```bash
mongosh mongodb://127.0.0.1:27117,127.0.0.1:27118/SLRevamp2 --quiet --eval 'db.keywords.insertOne({"keyword": "ABC", "points": 100})'
```

# Fail-Over Management

1. Test

# Get Started

## 1. Create Compact Topic on Kafka

Compact topic need to create first so Apache-Flink can re-consume the topic to reload data on Flink.

```bash
docker exec -e KAFKA_OPTS="" -it kafka-broker-1  kafka-topics \
  --create \
  --bootstrap-server localhost:29092 \
  --topic mongo.SLNonCore.keyword \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact

docker exec -e KAFKA_OPTS="" -it kafka-broker-1  kafka-topics \
  --create \
  --bootstrap-server localhost:29092 \
  --topic <TOPIC> \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact
```

`<TOPIC>` : `<topic.prefix>.<collection_name>` \
Options:

- `mongo.SLRevamp2.programv2`
- `mongo.SLRevamp2.keywords`
- `mongo.SLRevamp2.systemconfigs`
- `mongo.SLRevamp2.donation`
- `mongo.SLRevamp2.stocks`
- `mongo.SLRevamp2.lovs`
- `mongo.SLRevamp2.notificationtemplates`

## 2. Create Mongo CDC

Create connector from each compact topics target above:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "mongo.SLRevamp2.<collection_name>",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
      "tasks.max": "1",
      "connection.uri": "mongodb://host.docker.internal:27117,host.docker.internal:27118",
      "database": "SLRevamp2",
      "collection": "<collection_name>",
      "topic.prefix": "mongo",
      "output.format.key": "json",
      "output.format.value": "json",

      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",

      "value.converter.string.encoding": "UTF-8"
    }
  }' http://localhost:8083/connectors
```

```bash
curl -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "mongo.SLCoreCustomer.<collection_name>",
    "config": {
      "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
      "tasks.max": "1",
      "connection.uri": "mongodb://host.docker.internal:27117,host.docker.internal:27118",
      "database": "SLCoreCustomer",
      "collection": "<collection_name>",
      "topic.prefix": "mongo",
      "output.format.key": "json",
      "output.format.value": "json",

      "key.converter": "org.apache.kafka.connect.storage.StringConverter",
      "value.converter": "org.apache.kafka.connect.storage.StringConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",

      "value.converter.string.encoding": "UTF-8"
    }
  }' http://localhost:8083/connectors
```

## 3. Apache Flink - Create Program Module

All program process need for data sync from mongo

### A. Table Program

This table will consume data from topic that connect CDC from mongo on program collection

```sql
CREATE TABLE TERE_SLNonCore_program (
  operationType STRING,
  clusterTime ROW<t BIGINT, i BIGINT>,
  wallTime ROW<`$date` BIGINT>,
  fullDocument ROW<
    `_id` ROW<`$oid` STRING>,
    `name` STRING,
    `desc` STRING,
    start_period ROW<`$date` BIGINT>,
    end_period ROW<`$date` BIGINT>,
    point_type ROW<
      `_id` ROW<`$oid` STRING>,
      group_name STRING,
      set_value STRING,
      additional STRING
    >,
    program_mechanism ROW<
      `_id` ROW<`$oid` STRING>,
      group_name STRING,
      set_value STRING
    >,
    program_owner ROW<
      `_id` ROW<`$oid` STRING>,
      group_name STRING,
      set_value STRING
    >,
    program_owner_detail ROW<
      `_id` ROW<`$oid` STRING>,
      `name` STRING,
      `type` ROW<`$oid` STRING>,
      `data_source` STRING
    >,
    keyword_registration STRING,
    point_registration INT,
    whitelist_counter BOOLEAN,
    logic STRING,
    program_time_zone STRING,
    alarm_pic_type STRING,
    alarm_pic ARRAY<ROW<
      `_id` ROW<`$oid` STRING>,
      name STRING,
      msisdn STRING,
      email STRING
    >>,
    threshold_alarm_expired INT,
    threshold_alarm_voucher INT,
    program_notification ARRAY<ROW<
      `_id` ROW<`$oid` STRING>,
      code STRING,
      notif_type ROW<`$oid` STRING>,
      notif_name STRING,
      notif_via ARRAY<ROW<
        `_id` ROW<`$oid` STRING>,
        group_name STRING,
        set_value STRING,
        description STRING
      >>,
      notif_content STRING,
      receiver ARRAY<ROW<
        `_id` ROW<`$oid` STRING>,
        group_name STRING,
        set_value STRING,
        description STRING
      >>,
      channel_id ARRAY<STRING>,
      variable ARRAY<STRING>
    >>,
    program_approval ROW<`$oid` STRING>,
    is_draft BOOLEAN,
    is_stoped BOOLEAN,
    need_review_after_edit BOOLEAN,
    created_by ROW<
      `_id` ROW<`$oid` STRING>,
      user_name STRING,
      core_role STRING,
      email STRING,
      first_name STRING,
      job_level STRING,
      job_title STRING,
      last_name STRING,
      superior_local STRING,
      superior_hq STRING,
      phone STRING,
      role ROW<`$oid` STRING>,
      type STRING,
      updated_at ROW<`$date` BIGINT>,
      user_id STRING,
      manager_id STRING,
      status STRING
    >,
    deleted_at STRING,
    program_edit STRING,
    whitelist_check BOOLEAN,
    is_whitelist BOOLEAN,
    is_blacklist BOOLEAN,
    created_at ROW<`$date` BIGINT>,
    updated_at ROW<`$date` BIGINT>,
    hq_approver ROW<
      `_id` ROW<`$oid` STRING>,
      user_name STRING,
      core_role STRING,
      email STRING,
      first_name STRING,
      job_level STRING,
      job_title STRING,
      last_name STRING,
      superior_local STRING,
      superior_hq STRING,
      phone STRING,
      role ROW<`$oid` STRING>,
      type STRING,
      updated_at ROW<`$date` BIGINT>,
      user_id STRING,
      manager_id STRING,
      status STRING
    >
  >,
  ns ROW<db STRING, coll STRING>,
  documentKey ROW<
    `_id` ROW<`$oid` STRING>
  >
) WITH (
  'connector' = 'kafka',
  'topic' = 'mongo.SLNonCore.program',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092',
  'properties.group.id' = 'flink-consumer-program',
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
```

### B. View Program

```sql
CREATE VIEW program_lookup AS
SELECT
  _id,
  `name`,
  reward_type,
  reward_value,
  eligibility_rule,
  TO_TIMESTAMP_LTZ(effective_ts, 3) AS effective_time
FROM TERE_SLNonCore_program
WHERE ;
```

## 4. Apache Flink - Create Table Keyword

```sql
CREATE TABLE TERE_SLNonCore_keyword (
  operationType STRING,
  clusterTime ROW<t BIGINT, i BIGINT>,
  wallTime ROW<`$date` BIGINT>,
  fullDocument ROW<
    reward_catalog ROW<
      catalog_type ARRAY<ROW<`$oid` STRING>>,
      catalog_display ARRAY<ROW<`$oid` STRING>>,
      link_aja_voucher_title STRING,
      link_aja_voucher_description STRING,
      link_aja_voucher_poin_price STRING,
      link_aja_voucher_banner_image STRING,
      flashsale ROW<
        `status` BOOLEAN,
        `start_date` STRING,
        end_date STRING,
        poin INT,
        stock STRING,
        flashsale_image STRING,
        flashsale_description STRING
      >,
      area ARRAY<STRING>,
      region ARRAY<STRING>,
      location_type ROW<
        _id ROW<`$oid` STRING>,
        group_name STRING,
        set_value STRING
      >,
      location_area_identifier ROW<
        _id ROW<`$oid` STRING>,
        `name` STRING,
        `type` ROW<
          _id ROW<`$oid` STRING>,
          group_name STRING,
          set_value STRING
        >,
        parent STRING,
        area_id STRING,
        region_id STRING,
        city_id STRING,
        __v INT,
        `data_source` STRING,
        adhoc_group ARRAY<STRING>,
        area STRING,
        city STRING,
        lac BIGINT,
        latitude STRING,
        longitude STRING,
        region STRING,
        prov STRING,
        prov_id STRING,
        updated_at ROW<`$date` BIGINT>,
        timezone STRING,
        city_detail ARRAY<STRING>,
        prov_detail ARRAY<STRING>,
        region_detail ARRAY<STRING>
      >,
      location_region_identifier ROW<
        `name` STRING,
        _id ROW<`$oid` STRING>
      >,
      program STRING,
      keyword STRING,
      keyword_type STRING,
      hashtag_1 STRING,
      hashtag_2 STRING,
      hashtag_3 STRING,
      start_period ROW<`$date` BIGINT>,
      end_period ROW<`$date` BIGINT>,
      point_keyword INT,
      poin_markup_display INT,
      applied_on ARRAY<STRING>,
      special_applied_on ARRAY<ROW<`$oid` STRING>>,
      channel ARRAY<STRING>,
      enable_for_corporate_subs STRING,
      effective ROW<`$date` BIGINT>,
      `to` ROW<`$date` BIGINT>,
      microsite_segment ARRAY<STRING>,
      title STRING,
      teaser STRING,
      teaser_en STRING,
      `description` STRING,
      description_en STRING,
      faq STRING,
      faq_en STRING,
      image_promo_loc STRING,
      image_detail_loc STRING,
      image_detail_1_loc STRING,
      image_detail_2_loc STRING,
      image_detail_3_loc STRING,
      image_small STRING,
      image_medium STRING,
      image_large STRING,
      province ARRAY<STRING>,
      city ARRAY<STRING>,
      locations ARRAY<ROW<
        _id ROW<`$oid` STRING>,
        `name` STRING,
        `type` ROW<`$oid` STRING>,
        parent ROW<
          _id ROW<`$oid` STRING>,
          `name` STRING
        >,
        area STRING,
        region STRING,
        city STRING,
        area_id ROW<_id ROW<`$oid` STRING>, name STRING>,
        region_id ROW<_id ROW<`$oid` STRING>, name STRING>,
        city_id STRING,
        deleted_at STRING,
        created_at ROW<`$date` BIGINT>,
        updated_at ROW<`$date` BIGINT>,
        __v INT,
        `data_source` STRING,
        adhoc_group ARRAY<STRING>,
        lac BIGINT,
        latitude STRING,
        longitude STRING,
        prov STRING,
        prov_id ROW<_id ROW<`$oid` STRING>, `name` STRING>,
        timezone STRING
      >>,
      how_to_redeem STRING,
      how_to_redeem_en STRING,
      video STRING,
      google_maps STRING,
      hot_promo BOOLEAN,
      category STRING,
      merchant_id ROW<
        _id ROW<`$oid` STRING>,
        partner_id ROW<
          _id ROW<`$oid` STRING>,
          partner_code STRING,
          partner_name STRING,
          partner_status STRING
        >,
        merchant_name STRING,
        merchant_short_code STRING,
        location_id ROW<`$oid` STRING>,
        location_type ROW<
          _id ROW<`$oid` STRING>,
          group_name STRING,
          set_value STRING
        >
      >,
      created_at ROW<`$date` BIGINT>,
      updated_at ROW<`$date` BIGINT>,
      stock STRING,
      benefit_category STRING,
      price_info STRING,
      url_merchant STRING,
      category_select ROW<
        _id ROW<`$oid` STRING>,
        group_name STRING,
        set_value STRING,
        additional ROW<
          image_small STRING,
          image_medium STRING,
          image_large STRING
        >
      >,
      reward_catalog_locations ROW<
        _id BOOLEAN,
        set_value STRING
      >,
      image_detail1_loc STRING,
      image_detail2_loc STRING,
      image_detail3_loc STRING
    >,
    eligibility ROW<
      `name` STRING,
      start_period ROW<`$date` BIGINT>,
      end_period ROW<`$date` BIGINT>,
      program_experience ARRAY<ROW<
        _id ROW<`$oid` STRING>,
        group_name STRING,
        set_value STRING,
        additional ROW<
          image_small STRING,
          image_medium STRING,
          image_large STRING
        >
      >>,
      keyword_type STRING,
      point_type STRING,
      poin_value STRING,
      poin_redeemed INT,
      channel_validation BOOLEAN,
      channel_validation_list ARRAY<STRING>,
      eligibility_locations BOOLEAN,
      locations ARRAY<ROW<
        _id ROW<`$oid` STRING>,
        `name` STRING
      >>,
      program_title_expose STRING,
      merchant ROW<
        _id ROW<`$oid` STRING>,
        partner_id ROW<
          _id ROW<`$oid` STRING>,
          partner_code STRING,
          partner_name STRING,
          partner_status STRING
        >,
        merchant_name STRING,
        merchant_short_code STRING,
        location_id ROW<
          _id ROW<`$oid` STRING>,
          `name` STRING,
          `type` ROW<`$oid` STRING>,
          `data_source` STRING
        >,
        location_type ROW<
          _id ROW<`$oid` STRING>,
          group_name STRING,
          set_value STRING
        >
      >,
      program_id ROW<
        _id ROW<`$oid` STRING>,
        `name` STRING,
        start_period ROW<`$date` BIGINT>,
        end_period ROW<`$date` BIGINT>
      >,
      merchandise_keyword BOOLEAN,
      keyword_schedule STRING,
      program_bersubsidi BOOLEAN,
      total_budget BIGINT,
      customer_value BIGINT,
      multiwhitelist BOOLEAN,
      multiwhitelist_program ARRAY<STRING>,
      enable_sms_masking BOOLEAN,
      sms_masking STRING,
      timezone STRING,
      for_new_redeemer BOOLEAN,
      max_mode STRING,
      max_redeem_counter BIGINT,
      segmentation_customer_tier ARRAY<STRING>,
      segmentation_customer_los_operator STRING,
      segmentation_customer_los_max BIGINT,
      segmentation_customer_los_min BIGINT,
      segmentation_customer_los BIGINT,
      segmentation_customer_type STRING,
      segmentation_customer_most_redeem ARRAY<STRING>,
      segmentation_customer_brand ARRAY<STRING>,
      segmentation_customer_prepaid_registration BOOLEAN,
      segmentation_customer_kyc_completeness BOOLEAN,
      segmentation_customer_poin_balance_operator STRING,
      segmentation_customer_poin_balance BIGINT,
      segmentation_customer_poin_balance_max BIGINT,
      segmentation_customer_poin_balance_min BIGINT,
      segmentation_customer_preference STRING,
      segmentation_customer_arpu_operator STRING,
      segmentation_customer_arpu BIGINT,
      segmentation_customer_arpu_min BIGINT,
      segmentation_customer_arpu_max BIGINT,
      segmentation_customer_preferences_bcp STRING,
      location_type ROW<_id ROW<`$oid` STRING>, group_name STRING, set_value STRING>,
      location_area_identifier ROW<_id ROW<`$oid` STRING>, name STRING>,
      location_region_identifier ROW<_id ROW<`$oid` STRING>, name STRING>,
      max_redeem_threshold ROW<
        `status` BOOLEAN,
        `type` STRING,
        `date` ARRAY<STRING>,
        `time` ARRAY<STRING>,
        time_identifier ARRAY<ROW<`hours` STRING, `seconds` STRING>>,
        date_identifier ARRAY<ROW<`date` STRING, `time` STRING>>,
        `start_date` STRING,
        end_date STRING
      >,
      flashsale ROW<`status` BOOLEAN, `start_date` STRING, `end_date` STRING, poin INT>,
      keyword_shift ARRAY<STRING>,
      bcp_app_name STRING,
      bcp_app_name_operator STRING,
      bcp_app_category STRING,
      bcp_app_category_operator STRING,
      imei STRING,
      imei_operator STRING,
      segmentation_telkomsel_employee BOOLEAN
    >,
    bonus ARRAY<ROW<
      bonus_type STRING,
      stock_location ARRAY<ROW<
        `name` STRING,
        _id ROW<`$oid` STRING>,
        stock INT,
        adhoc_group ARRAY<STRING>,
        stock_flashsale INT,
        balance INT
      >>,
      stock_type STRING,
      threshold BIGINT,
      `start_date` STRING,
      end_date STRING,
      `hour` ARRAY<STRING>
    >>,
    `notification` ARRAY<ROW<
      via ARRAY<ROW<_id ROW<`$oid` STRING>, group_name STRING, set_value STRING, `description` STRING>>,
      receiver ARRAY<ROW<_id ROW<`$oid` STRING>, group_name STRING, set_value STRING, `description` STRING>>,
      bonus_type_id STRING,
      keyword_name STRING,
      code_identifier ROW<`$oid` STRING>,
      notification_content STRING,
      start_period ROW<`$date` BIGINT>,
      end_period ROW<`$date` BIGINT>,
      notif_type ROW<`$oid` STRING>
    >>,
    keyword_approval ROW<_id ROW<`$oid` STRING>, group_name STRING, set_value STRING>,
    is_draft BOOLEAN,
    is_stoped BOOLEAN,
    need_review_after_edit BOOLEAN,
    keyword_edit ROW<_id ROW<`$oid` STRING>, eligibility ROW<name STRING, start_period ROW<`$date` BIGINT>, end_period ROW<`$date` BIGINT>>>,
    created_by ROW<
      _id ROW<`$oid` STRING>,
      user_name STRING,
      core_role STRING,
      email STRING,
      first_name STRING,
      job_level STRING,
      job_title STRING,
      last_name STRING,
      superior_local STRING,
      superior_hq STRING,
      phone STRING,
      `role` ROW<`$oid` STRING>,
      `type` STRING,
      updated_at ROW<`$date` BIGINT>,
      user_id STRING,
      manager_id STRING,
      `status` STRING,
      account_location ROW<
        __v INT,
        `location` ROW<`$oid` STRING>,
        agent STRING,
        location_detail ROW<name STRING, `type` ROW<`$oid` STRING>, __v INT, `data_source` STRING>
      >
    >,
    deleted_at STRING,
    is_main_keyword BOOLEAN,
    child_keyword ARRAY<STRING>,
    created_at ROW<`$date` BIGINT>,
    updated_at ROW<`$date` BIGINT>,
    __v INT,
    hq_approver ROW<`$oid` STRING>
  >,
  ns ROW<db STRING, coll STRING>,
  documentKey ROW<_id ROW<`$oid` STRING>>
) WITH (
  'connector' = 'kafka',
  'topic' = 'mongo.SLNonCore.keyword',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092',
  'properties.group.id' = 'flink-consumer-keyword',
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

```

## 5. Apache Flink - Create Table Redeem

This table will stream process from topic `TERE_redeem`

```sql
CREATE TABLE TERE_SLNonCore_redeem (
  `data` ROW<
    locale STRING,
    msisdn STRING,
    keyword_id STRING,
    transaction_id STRING,
    channel_id STRING,
    callback_url STRING,
    transaction_source STRING,
    send_notification BOOLEAN,
    additional MAP<STRING, STRING>
  >,
  `account` ROW<
    _id ROW<`$oid` STRING>,
    user_id STRING
  >,
  token STRING,
  transaction_id STRING,
  `path` STRING,
  keyword_priority STRING,
  `timestamp` STRING METADATA FROM 'timestamp' VIRTUAL,
  `partition` STRING METADATA FROM 'partition' VIRTUAL,
  `offset` STRING METADATA FROM 'offset' VIRTUAL
) WITH (
  'connector' = 'kafka',
  'topic' = 'TERE_redeem',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092',
  'properties.group.id' = 'flink-consumer-redeem',
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
```

## 6. Apache Flink - Create View Redeem

This process mimic redeem consumer for data enrichment

```sql
CREATE VIEW TERE_redeem_enrichment AS
SELECT
  r.msisdn,
  r.keyword_id,
  k.name AS keyword_name,
  k.reward_type,
  k.reward_value,
  k.eligibility_rule,
  c.customer_id,
  c.status AS customer_status,
  c.balance,
  r.event_time
FROM TERE_SLNonCore_redeem AS r
LEFT JOIN keywords_lookup FOR SYSTEM_TIME AS OF r.event_time AS k
  ON r.keyword_id = k.keyword_id
LEFT JOIN customers_lookup FOR SYSTEM_TIME AS OF r.event_time AS c
  ON r.msisdn = c.msisdn;
```
