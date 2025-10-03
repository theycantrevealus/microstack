-- CREATE MASTER DATA STREAM
CREATE TABLE TERE_SLNonCore_program (
  operationType STRING,
  clusterTime ROW<t BIGINT, i BIGINT>,
  wallTime ROW<`$date` BIGINT>,
  event_time AS TO_TIMESTAMP_LTZ(wallTime.`$date`, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND,
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
-- ====================================================================================================================================================================================
CREATE TABLE TERE_SLNonCore_keyword (
  operationType STRING,
  clusterTime ROW<t BIGINT, i BIGINT>,
  wallTime ROW<`$date` BIGINT>,
  event_time AS TO_TIMESTAMP_LTZ(wallTime.`$date`, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND,
  fullDocument ROW<
    `_id` ROW<`$oid` STRING>,
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
-- ====================================================================================================================================================================================
CREATE TABLE TERE_SLNonCore_redeem (
  `data` ROW<
    locale STRING,
    msisdn STRING,
    keyword STRING,
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
  `kafka_timestamp` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
--   `timestamp` STRING METADATA FROM 'timestamp' VIRTUAL,
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
-- ====================================================================================================================================================================================
CREATE VIEW Flink_keyword AS
SELECT
    fullDocument._id.`$oid` AS _id,
    fullDocument.eligibility.`name` AS keyword_identifier,
    CASE
        WHEN operationType = 'delete' THEN CAST(NULL AS STRING)
        ELSE fullDocument.eligibility.`name`
    END AS keyword_name,
    CASE
        WHEN operationType = 'delete' THEN CAST(NULL AS BIGINT)
        ELSE fullDocument.eligibility.start_period.`$date`
    END AS start_period,
    CASE
        WHEN operationType = 'delete' THEN CAST(NULL AS BIGINT)
        ELSE fullDocument.eligibility.end_period.`$date`
    END AS end_period,
    operationType
FROM TERE_SLNonCore_keyword
WHERE operationType IN ('insert', 'update', 'delete');
-- ====================================================================================================================================================================================
CREATE VIEW Flink_redeem AS
SELECT
  r.data.msisdn AS msisdn,
  r.data.keyword AS keyword,
  r.data.transaction_id AS transaction_id,
  r.data.channel_id AS channel_id,
  r.data.callback_url AS callback_url,
  r.data.transaction_source AS transaction_source,
  r.data.send_notification AS send_notification,
  r.data.additional AS additional,
  r.kafka_timestamp AS kafka_timestamp,
  k.start_period AS start_period,
  k.end_period AS end_period
FROM TERE_SLNonCore_redeem AS r
LEFT JOIN Flink_keyword AS k
ON
    r.data.keyword = k.keyword_identifier
WHERE
    k.start_period IS NOT NULL AND
    k.end_period IS NOT NULL AND
    r.data.transaction_id IS NOT NULL;
-- ====================================================================================================================================================================================
CREATE VIEW Flink_eligibility AS
SELECT
  transaction_id,
  keyword,
  start_period,
  end_period,
  kafka_timestamp,
  CURRENT_TIMESTAMP AS processed_time,
  UNIX_MILLIS(CURRENT_TIMESTAMP) -  COALESCE(UNIX_MILLIS(kafka_timestamp), UNIX_MILLIS(CURRENT_TIMESTAMP)) AS latency_ms,
  CASE
    WHEN start_period IS NULL OR end_period IS NULL THEN 'Missing keyword period'
    WHEN CURRENT_TIMESTAMP < TO_TIMESTAMP_LTZ(start_period, 3) THEN 'Keyword period not yet started'
    WHEN CURRENT_TIMESTAMP > TO_TIMESTAMP_LTZ(end_period, 3) THEN 'Keyword period expired'
    ELSE 'ELIGIBLE'
  END AS reason
FROM Flink_redeem;


-- ====================================================================================================================================================================================
CREATE TABLE Kafka_eligibility_sink (
    transaction_id STRING,
    keyword STRING,
    start_period BIGINT,
    end_period BIGINT,
    kafka_timestamp TIMESTAMP_LTZ(3),
    processed_time TIMESTAMP_LTZ(3),
    latency_ms BIGINT,
    reason STRING,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'TERE_eligibility',
  'properties.bootstrap.servers' = 'kafka-broker-1:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'value.json.ignore-parse-errors' = 'true',
  'properties.security.protocol' = 'PLAINTEXT',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";'
);