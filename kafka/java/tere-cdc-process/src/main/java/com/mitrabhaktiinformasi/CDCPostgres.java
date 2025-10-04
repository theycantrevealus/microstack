package com.mitrabhaktiinformasi.flinkcdc;
import java.lang.System;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.sql.PreparedStatement;
import java.util.Properties;

public class CDCPostgres {

    public static class KeywordEvent {
        public String id;
        public String document;
        public String operationType;
        public KeywordEvent() {}
    }

    public static class ParseJsonMap implements MapFunction<String, KeywordEvent> {
        private static final ObjectMapper mapper = new ObjectMapper();
        @Override
        public KeywordEvent map(String value) throws Exception {
            JsonNode node = mapper.readTree(value);
            KeywordEvent event = new KeywordEvent();
            event.id = node.path("documentKey").path("_id").path("$oid").asText();
            JsonNode fullDoc = node.path("fullDocument");
            event.document = fullDoc.isMissingNode() ? null : fullDoc.toString();
            event.operationType = node.path("operationType").asText();
            return event;
        }
    }

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

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "mongo.SLNonCore.keyword",
                new SimpleStringSchema(),
                properties
        );
        kafkaConsumer.setStartFromEarliest();

        DataStream<KeywordEvent> events = env.addSource(kafkaConsumer).map(new ParseJsonMap());

        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://host.docker.internal:5432/TERE")
                .withDriverName("org.postgresql.Driver")
                .withUsername("tere")
                .withPassword("mypassword")
                .build();

        // events.addSink(JdbcSink.sink(
        //         "INSERT INTO keyword (_id, document) VALUES (?, ?) ON CONFLICT (_id) DO UPDATE SET document = EXCLUDED.document",
        //         (PreparedStatement ps, KeywordEvent event) -> {
        //             System.out.println("DataSet=" + event);

        //             if ("delete".equals(event.operationType)) {
        //                 ps.clearParameters();
        //                 ps.setString(1, event.id);
        //                 ps.executeUpdate();
        //             } else {
        //                 ps.setString(1, event.id);
        //                 ps.setString(2, event.document);
        //                 ps.executeUpdate();
        //             }
        //         },
        //         new JdbcExecutionOptions.Builder()
        //                 .withBatchSize(500)
        //                 .withBatchIntervalMs(200)
        //                 .build(),
        //         jdbcOptions
        // ));

        // Delete sink
        events
        .filter(e -> "delete".equals(e.operationType))
        .addSink(JdbcSink.sink(
            "DELETE FROM keyword WHERE _id = ?",
            (ps, e) -> ps.setString(1, e.id),
            new JdbcExecutionOptions.Builder()
                .withBatchSize(500)
                .withBatchIntervalMs(200)
                .build(),
            jdbcOptions
        ));

        // Insert sink
        events
        .filter(e -> "insert".equals(e.operationType))
        .addSink(JdbcSink.sink(
            "INSERT INTO keyword (_id, document) VALUES (?, ?) ON CONFLICT (_id) DO UPDATE SET document = EXCLUDED.document",
            (ps, e) -> {
                ps.setString(1, e.id);
                ps.setString(2, e.document);
            },
            new JdbcExecutionOptions.Builder()
                .withBatchSize(500)
                .withBatchIntervalMs(200)
                .build(),
            jdbcOptions
        ));

        // Update sink
        events
        .filter(e -> "update".equals(e.operationType))
        .addSink(JdbcSink.sink(
            "UPDATE keyword SET document = ? WHERE _id = ?",
            (ps, e) -> {
                ps.setString(1, e.document);
                ps.setString(2, e.id);
            },
            new JdbcExecutionOptions.Builder()
                .withBatchSize(500)
                .withBatchIntervalMs(200)
                .build(),
            jdbcOptions
        ));

        // Replace sink
        events
        .filter(e -> "replace".equals(e.operationType))
        .addSink(JdbcSink.sink(
            "UPDATE keyword SET document = ? WHERE _id = ?",
            (ps, e) -> {
                ps.setString(1, e.document);
                ps.setString(2, e.id);
            },
            new JdbcExecutionOptions.Builder()
                .withBatchSize(500)
                .withBatchIntervalMs(200)
                .build(),
            jdbcOptions
        ));




        env.execute("Flink Unified CDC to Postgres");
    }
}
