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
                properties.setProperty("client.id", "cdc-consumer");
                properties.setProperty("security.protocol", System.getenv("KAFKA_SECURITY_PROTOCOL"));
                properties.setProperty("sasl.mechanism", System.getenv("KAFKA_SASL_MECHANISM"));
                properties.setProperty("sasl.jaas.config", System.getenv("KAFKA_SASL_JAAS_CONFIG"));
                properties.setProperty("ssl.truststore.location", System.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION"));
                properties.setProperty("ssl.truststore.password", System.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD"));
                properties.setProperty("ssl.endpoint.identification.algorithm", "");

                buildStream(env, "mongo.SLNonCore.systemconfigs", "CDC-SystemConfig", "systemconfigs", properties);
                buildStream(env, "mongo.SLNonCore.lovs", "CDC-LOV", "lovs", properties);
                buildStream(env, "mongo.SLNonCore.notificationtemplates", "CDC-NotificationTemplate",
                                "notificationtemplates", properties);
                buildStream(env, "mongo.SLNonCore.programs", "CDC-Program", "programs", properties);
                buildStream(env, "mongo.SLNonCore.keywords", "CDC-Keyword", "keywords", properties);
                buildStream(env, "mongo.SLNonCore.donations", "CDC-Donation", "donations", properties);

                env.execute("CDC - MongoDB to Postgres - Master Data");
        }

        private static void buildStream(StreamExecutionEnvironment env, String topic, String groupId, String sinkTable,
                        Properties properties) {

                KafkaSource<String> source = KafkaSource.<String>builder()
                                .setBootstrapServers(properties.getProperty("bootstrap.servers")).setTopics(topic)
                                .setGroupId(groupId).setProperties(properties)
                                .setStartingOffsets(OffsetsInitializer.earliest())
                                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

                DataStream<String> rawEvents = env
                                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka " + topic + " Source")
                                .name(sinkTable + " Source");

                DataStream<CDCData> events = rawEvents.map(new ParseJsonMap())
                                .assignTimestampsAndWatermarks(WatermarkStrategy
                                                .<CDCData>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                                .withTimestampAssigner((event, ts) -> event.eventTime))
                                .name(groupId + " Map");

                events.addSink(new PostgresCDCSink(sinkTable, System.getenv("JDBC_URL"), System.getenv("JDBC_USER"),
                                System.getenv("JDBC_PASSWORD"))).name(groupId + " Map");
                ;
        }

}
