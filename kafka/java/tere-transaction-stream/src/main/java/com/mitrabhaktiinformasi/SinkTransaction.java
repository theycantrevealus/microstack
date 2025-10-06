package com.mitrabhaktiinformasi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkTransaction extends RichSinkFunction<DTORedeem> {
    private static final Logger LOG = LoggerFactory.getLogger(TERETransaction.class);

    private Connection connection;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    public SinkTransaction(String jdbcUrl, String jdbcUser, String jdbcPassword) {
        this.jdbcUrl = jdbcUrl;
        this.jdbcUser = jdbcUser;
        this.jdbcPassword = jdbcPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Class.forName("org.postgresql.Driver");
            LOG.info("PostgreSQL Driver loaded successfully via Class.forName");
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
    public void invoke(DTORedeem e, Context ctx) throws Exception {
        /*
         * Step to process:
         * 1. Get all needed data
         * 2. Check eligibility of customer
         * 3. Point Deduction
         * 4. Bonus Fulfillment
         * 5. Refund if any
         * 6. Update transaction status
         * 7. Notify customer
         * 8. Process reporting
         */

        try (PreparedStatement selectPs = connection.prepareStatement(
                "SELECT _id, document FROM keyword WHERE document->'eligibility'->>'name' = ?")) {

            selectPs.setString(1, e.data.keyword.toString());

            try (ResultSet rs = selectPs.executeQuery()) {
                if (rs.next()) {
                    String docExcerpt = rs.getString("document");
                    LOG.warn("Data Found={}", docExcerpt);
                } else {
                    LOG.warn("No data visible in-tx after insert for Keyword={}", e.data.keyword.toString());
                }
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
