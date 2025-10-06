package com.mitrabhaktiinformasi;

import java.lang.System;
import java.time.Duration;
import java.util.Properties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class CDCPostgres {

        public static class ParseJsonMap implements MapFunction<String, CDCData> {
                private static final ObjectMapper mapper = new ObjectMapper();

                @Override
                public CDCData map(String value) throws Exception {
                        CDCData data = mapper.readValue(value, CDCData.class);
                        data.resolveEventTime();
                        return data;
                }
        }

        /**
         * @param args
         * @throws Exception
         */
        public static void main(String[] args) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
                properties.setProperty("group.id", System.getenv("KAFKA_GROUP_ID"));
                properties.setProperty("security.protocol", System.getenv("KAFKA_SECURITY_PROTOCOL"));
                properties.setProperty("sasl.mechanism", System.getenv("KAFKA_SASL_MECHANISM"));
                properties.setProperty("sasl.jaas.config", System.getenv("KAFKA_SASL_JAAS_CONFIG"));
                properties.setProperty("ssl.truststore.location", System.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION"));
                properties.setProperty("ssl.truststore.password", System.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD"));
                properties.setProperty("ssl.endpoint.identification.algorithm", "");
                /*
                 * ======================================================================
                 * Sink Program
                 * ======================================================================
                 */
                KafkaSource<String> sourceProgram = KafkaSource.<String>builder()
                                .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                                .setTopics("mongo.SLNonCore.program")
                                .setGroupId(System.getenv("KAFKA_GROUP_ID"))
                                .setProperties(properties)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(
                                                new SimpleStringSchema())
                                .build();

                DataStream<String> rawEventsProgram = env.fromSource(
                                sourceProgram,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Source");

                DataStream<CDCData> eventsProgram = rawEventsProgram.map(new ParseJsonMap())
                                .assignTimestampsAndWatermarks(WatermarkStrategy
                                                .<CDCData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((event, ts) -> event.eventTime));

                eventsProgram.addSink(new PostgresCDCSink(
                                "program",
                                System.getenv("JDBC_URL"),
                                System.getenv("JDBC_USER"),
                                System.getenv("JDBC_PASSWORD")));

                /*
                 * ======================================================================
                 * Sink Keyword
                 * ======================================================================
                 */
                KafkaSource<String> sourceKeyword = KafkaSource.<String>builder()
                                .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
                                .setTopics("mongo.SLNonCore.keyword")
                                .setGroupId(System.getenv("KAFKA_GROUP_ID"))
                                .setProperties(properties)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(
                                                new SimpleStringSchema())
                                .build();

                DataStream<String> rawEventsKeyword = env.fromSource(
                                sourceKeyword,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Source");

                DataStream<CDCData> eventsKeyword = rawEventsKeyword.map(new ParseJsonMap())
                                .assignTimestampsAndWatermarks(WatermarkStrategy
                                                .<CDCData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((event, ts) -> event.eventTime));

                eventsKeyword.addSink(new PostgresCDCSink(
                                "keyword",
                                System.getenv("JDBC_URL"),
                                System.getenv("JDBC_USER"),
                                System.getenv("JDBC_PASSWORD")));

                env.execute("Flink Unified CDC to Postgres");
        }

}
