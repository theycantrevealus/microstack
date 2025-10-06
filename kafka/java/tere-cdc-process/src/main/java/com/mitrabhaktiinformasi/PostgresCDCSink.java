package com.mitrabhaktiinformasi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresCDCSink extends RichSinkFunction<CDCData> {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresCDCSink.class);

    private Connection connection;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    private final ObjectMapper mapper = new ObjectMapper();

    public PostgresCDCSink(String jdbcUrl, String jdbcUser, String jdbcPassword) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Class.forName("org.postgresql.Driver");
            LOG.info("PostgreSQL Driver loaded successfully via Class.forName"); // Optional log
        } catch (ClassNotFoundException ex) {
            LOG.error("PostgreSQL Driver not found on classpath: {}", ex.getMessage());
            throw new RuntimeException("Add org.postgresql:postgresql JAR to your dependencies", ex);
        }

        LOG.info("Opening PostgreSQL connection: {}", jdbcUrl);
        try {
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            LOG.info("Connected successfully! DB Catalog: {}", connection.getCatalog());
        } catch (SQLException ex) {
            LOG.error("Connection or query failed: {}", ex.getMessage(), ex);
            throw new RuntimeException("Database connection error", ex);
        }

        LOG.info("PostgreSQL connection established successfully.");
    }

    @Override
    public void invoke(CDCData e, Context ctx) throws Exception {
        if (e == null || e.documentKey == null || e.documentKey.id == null) {
            LOG.warn("Skipping event due to missing documentKey or id: {}", e);
            return;
        }

        LOG.info("Processing CDC event: operation={}, id={}", e.operationType, e.documentKey.id);

        boolean autoCommitWas = connection.getAutoCommit();
        LOG.info("Connection auto-commit before tx: {}", autoCommitWas);

        try {
            if (!autoCommitWas) {
                connection.setAutoCommit(false);
                LOG.info("Switched to manual tx mode");
            }
            switch (e.operationType) {
                case "delete":
                    try (PreparedStatement ps = connection.prepareStatement(
                            "DELETE FROM keyword WHERE _id = ?")) {
                        ps.setString(1, e.documentKey.id.toString());
                        int rows = ps.executeUpdate();
                        LOG.info("DELETE executed, rows affected={}", rows);

                        if (!autoCommitWas) {
                            connection.commit();
                            LOG.info("Transaction committed for ID: {}", e.documentKey.id);
                        } else {
                            LOG.info("Auto-commit ON—no explicit commit needed for ID: {}", e.documentKey.id);
                        }
                    }
                    break;

                case "insert":
                    try (PreparedStatement ps = connection.prepareStatement(
                            "INSERT INTO keyword (_id, document) VALUES (?, ?::jsonb) ON CONFLICT (_id) DO UPDATE SET document = EXCLUDED.document")) {

                        String docAsString = e.fullDocument != null
                                ? (e.fullDocument instanceof JsonNode
                                        ? e.fullDocument.toString()
                                        : mapper.writeValueAsString(e.fullDocument))
                                : "{}";

                        LOG.debug("Inserting docAsString (excerpt): {}",
                                docAsString.length() > 100 ? docAsString.substring(0, 100) + "..." : docAsString);

                        ps.setString(1, e.documentKey.id.toString());
                        ps.setString(2, docAsString);
                        int rows = ps.executeUpdate();
                        LOG.info("INSERT executed, rows affected={}", rows);

                        try (PreparedStatement selectPs = connection.prepareStatement(
                                "SELECT _id, document FROM keyword WHERE _id = ?")) {
                            selectPs.setString(1, e.documentKey.id.toString());
                            try (ResultSet rs = selectPs.executeQuery()) {
                                if (rs.next()) {
                                    String docExcerpt = rs.getString("document");
                                    LOG.info("Verified in-tx: ID={}, doc excerpt={}",
                                            rs.getString("_id"),
                                            docExcerpt.length() > 50 ? docExcerpt.substring(0, 50) + "..."
                                                    : docExcerpt);
                                } else {
                                    LOG.warn("No data visible in-tx after insert for ID={}", e.documentKey.id);
                                }
                            }
                        }

                        if (!autoCommitWas) {
                            connection.commit();
                            LOG.info("Transaction committed for ID: {}", e.documentKey.id);
                        } else {
                            LOG.info("Auto-commit ON—no explicit commit needed for ID: {}", e.documentKey.id);
                        }
                    } catch (SQLException ex) {
                        if (!autoCommitWas) {
                            connection.rollback();
                            LOG.error("Rollback due to error for ID: {}", e.documentKey.id, ex);
                        }
                        throw ex;
                    }
                    break;

                case "replace":
                    try (PreparedStatement ps = connection.prepareStatement(
                            "UPDATE keyword SET document = COALESCE(document, '{}'::jsonb) || ?::jsonb WHERE _id = ?")) {

                        String docAsString = e.fullDocument != null
                                ? (e.fullDocument instanceof JsonNode
                                        ? e.fullDocument.toString()
                                        : mapper.writeValueAsString(e.fullDocument))
                                : "{}";

                        ps.setString(1, docAsString);
                        ps.setString(2, e.documentKey.id.toString());

                        int rows = ps.executeUpdate();
                        LOG.info("UPDATE executed, rows affected={}", rows);

                        if (!autoCommitWas) {
                            connection.commit();
                            LOG.info("Transaction committed for ID: {}", e.documentKey.id);
                        } else {
                            LOG.info("Auto-commit ON—no explicit commit needed for ID: {}", e.documentKey.id);
                        }
                    } catch (SQLException ex) {
                        if (!autoCommitWas) {
                            connection.rollback();
                            LOG.error("Rollback due to error for ID: {}", e.documentKey.id, ex);
                        }
                        throw ex;
                    }
                    break;

                case "update":

                    String updatedFieldsJson = (e.updateDescription != null &&
                            e.updateDescription.updatedFields != null)
                                    ? mapper.writeValueAsString(e.updateDescription.updatedFields)
                                    : "{}";

                    String[] removed = (e.updateDescription != null &&
                            e.updateDescription.removedFields != null)
                                    ? e.updateDescription.removedFields
                                    : new String[0];

                    try (
                            PreparedStatement ps = connection.prepareStatement(
                                    "UPDATE keyword SET document = COALESCE(document, '{}'::jsonb) || ?::jsonb - ?::text[] WHERE _id = ?")) {

                        ps.setString(1, updatedFieldsJson);
                        ps.setArray(2, connection.createArrayOf("text", removed));
                        ps.setString(3, e.documentKey.id.toString());
                        int rows = ps.executeUpdate();
                        LOG.info("UPDATE executed, rows affected={}", rows);

                        if (!autoCommitWas) {
                            connection.commit();
                            LOG.info("Transaction committed for ID: {}", e.documentKey.id);
                        } else {
                            LOG.info("Auto-commit ON—no explicit commit needed for ID: {}", e.documentKey.id);
                        }
                    }
                    break;

                default:
                    LOG.warn("Unknown operation type: {}", e.operationType);
            }
        } catch (Exception ex) {
            LOG.error("Unexpected error for ID={}: {}", e.documentKey.id, ex.getMessage(), ex);
            if (!autoCommitWas) {
                try {
                    connection.rollback();
                    LOG.error("Rolled back on unexpected error for ID: {}", e.documentKey.id);
                } catch (SQLException rollbackEx) {
                    LOG.error("Rollback failed: {}", rollbackEx.getMessage(), rollbackEx);
                }
            }
            throw ex;
        } finally {
            if (!autoCommitWas) {
                connection.setAutoCommit(autoCommitWas); // Restore original mode
            }
        }

    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null && !connection.isClosed()) {
            connection.close();
            LOG.info("PostgreSQL connection closed.");
        }
    }
}
