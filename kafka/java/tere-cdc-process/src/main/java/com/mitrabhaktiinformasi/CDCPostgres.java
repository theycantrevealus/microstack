package com.mitrabhaktiinformasi.flinkcdc;

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
        properties.setProperty("bootstrap.servers", "kafka-broker-1:29092");
        properties.setProperty("group.id", "flink-consumer-keyword");
        properties.setProperty("security.protocol", "PLAINTEXT");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        properties.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"kafkabroker\" password=\"confluent\";");
        properties.setProperty("ssl.truststore.location", "/etc/kafka/certificates/client/kafka.client.truststore.jks");
        properties.setProperty("ssl.truststore.password", "clientpass");
        properties.setProperty("ssl.endpoint.identification.algorithm", "");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "mongo.SLNonCore.keyword",
                new SimpleStringSchema(),
                properties
        );
        kafkaConsumer.setStartFromEarliest();
        // kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        DataStream<KeywordEvent> events = env.addSource(kafkaConsumer).map(new ParseJsonMap());

        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:postgresql://host.docker.internal:5432/TERE")
                .withDriverName("org.postgresql.Driver")
                .withUsername("tere")
                .withPassword("mypassword")
                .build();

        events.addSink(JdbcSink.sink(
                "INSERT INTO keyword (_id, document) VALUES (?, ?) " +
                        "ON CONFLICT (_id) DO UPDATE SET document = EXCLUDED.document",
                (PreparedStatement ps, KeywordEvent event) -> {
                    if ("delete".equals(event.operationType)) {
                        ps.clearParameters();
                        ps.setString(1, event.id);
                        ps.executeUpdate(); // Execute DELETE separately
                    } else {
                        ps.setString(1, event.id);
                        ps.setString(2, event.document);
                        ps.executeUpdate();
                    }
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
