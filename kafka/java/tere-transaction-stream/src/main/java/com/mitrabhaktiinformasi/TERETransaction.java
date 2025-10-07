package com.mitrabhaktiinformasi;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TERETransaction {

        public static class ParseJsonMap implements MapFunction<String, DTORedeem> {
                private static final ObjectMapper mapper = new ObjectMapper();

                @Override
                public DTORedeem map(String value) throws Exception {
                        DTORedeem data = mapper.readValue(value, DTORedeem.class);
                        data.resolveEventTime();
                        return data;
                }
        }

        public static void main(String[] args) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
                properties.setProperty("group.id", "TERETransaction");
                properties.setProperty("security.protocol", System.getenv("KAFKA_SECURITY_PROTOCOL"));
                properties.setProperty("client.id", "redeem-consumer");
                properties.setProperty("sasl.mechanism", System.getenv("KAFKA_SASL_MECHANISM"));
                properties.setProperty("sasl.jaas.config", System.getenv("KAFKA_SASL_JAAS_CONFIG"));
                properties.setProperty("ssl.truststore.location", System.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION"));
                properties.setProperty("ssl.truststore.password", System.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD"));
                properties.setProperty("ssl.endpoint.identification.algorithm", "");

                KafkaSource<String> source = KafkaSource.<String>builder()
                                .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                                .setTopics("TERE_redeem")
                                .setGroupId("TERETransaction")
                                .setProperties(properties)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(
                                                new SimpleStringSchema())
                                .build();

                DataStream<String> rawEvent = env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Transaction Source");

                DataStream<DTORedeem> stream = rawEvent.map(new ParseJsonMap())
                                .assignTimestampsAndWatermarks(WatermarkStrategy
                                                .<DTORedeem>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((event, ts) -> event.eventTime));

                DataStream<DTORedeemEnriched> enriched = stream
                                .map(new RedeemEnrichment(
                                                System.getenv("JDBC_URL"),
                                                System.getenv("JDBC_USER"),
                                                System.getenv("JDBC_PASSWORD")))
                                .name("Enrichment Step").disableChaining();

                /*
                 * Step : Here lay the eligibility logic
                 * - Check payload completeness
                 * - Validate msisdn format
                 * - Check [NO_RULE]
                 * - Check if is [KEYWORD_REGISTRATION]
                 * - Check if program and keyword is approved
                 */
                // DataStream<DTORedeemEnriched> eligible = enriched
                // .filter(dto -> dto.keyword != null).name("Eligibility
                // Filter").disableChaining();
                DataStream<DTORedeemEnriched> eligible = enriched
                                .filter((dto) -> {
                                        Boolean isEligible = false;

                                        Boolean isNoRule = false;

                                        // Check payload completeness
                                        if (dto.keyword == null || dto.program == null) {
                                                isEligible = false;
                                                dto.setEligibility(false, "INCOMPLETE_PAYLOAD");
                                        }

                                        // Validate msisdn format
                                        else if (dto.customer == null || !dto.customer.has("msisdn")
                                                        || dto.customer.get("msisdn").asText().length() < 10) {
                                                isEligible = false;
                                                dto.setEligibility(false, "INVALID_MSISDN");
                                        }

                                        return isEligible;

                                }).name("Eligibility Filter").disableChaining();

                KafkaSink<String> sink = KafkaSink.<String>builder()
                                .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                                .setProperty("security.protocol", System.getenv("KAFKA_SECURITY_PROTOCOL"))
                                .setProperty("sasl.mechanism", System.getenv("KAFKA_SASL_MECHANISM"))
                                .setProperty("sasl.jaas.config", System.getenv("KAFKA_SASL_JAAS_CONFIG"))
                                .setProperty("client.id", "deduct-producer")
                                .setProperty("group.id", "TERETransaction")
                                .setProperty("ssl.truststore.location", System.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION"))
                                .setProperty("ssl.truststore.password", System.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD"))
                                .setProperty("ssl.endpoint.identification.algorithm", "")
                                .setRecordSerializer(
                                                KafkaRecordSerializationSchema.builder()
                                                                .setTopic("TERE_deduct")
                                                                .setValueSerializationSchema(new SimpleStringSchema())
                                                                .build())
                                .build();

                DataStream<String> jsonStream = eligible.map(new MapFunction<DTORedeemEnriched, String>() {
                        @Override
                        public String map(DTORedeemEnriched dto) throws Exception {
                                Map<String, Object> payload = new LinkedHashMap<>();
                                payload.put("keyword", dto.keyword);
                                payload.put("program", dto.program);
                                payload.put("isEligible", dto.isEligible);
                                payload.put("reason", dto.reason);
                                payload.put("timestamp", System.currentTimeMillis());

                                ObjectMapper mapper = new ObjectMapper();
                                return mapper.writeValueAsString(payload);
                        }
                });

                jsonStream.sinkTo(sink);

                env.execute("TERE Transaction Stream");
        }
}
