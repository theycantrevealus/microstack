package com.mitrabhaktiinformasi;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RedeemEnrichment extends RichMapFunction<DTORedeem, DTORedeemEnriched> {
    private static final Logger LOG = LoggerFactory.getLogger(TERETransaction.class);
    private transient Connection connection;
    private final String jdbcUrl, jdbcUser, jdbcPassword;
    private final ObjectMapper mapper = new ObjectMapper();

    public RedeemEnrichment(String jdbcUrl, String jdbcUser, String jdbcPassword) {
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
    public DTORedeemEnriched map(DTORedeem value) throws Exception {
        PreparedStatement ps = connection.prepareStatement(
                "SELECT k._id, k.document AS keyword_doc, p.document AS program_doc FROM keyword k JOIN program p ON (k.document->'eligibility'->'program_id'->'_id'->>'$oid') = p._id WHERE k.document->'eligibility'->>'name' = ?");

        ps.setString(1, value.data.keyword.toString());
        ps.setString(1, value.data.keyword.toString());

        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                JsonNode keywordNode = mapper.readTree(rs.getString("keyword_doc"));
                JsonNode programNode = mapper.readTree(rs.getString("program_doc"));

                return new DTORedeemEnriched(
                        keywordNode,
                        programNode,
                        null); // TODO : <<== Phung mau g y? Wkwkwkwkwk
            } else {
                LOG.warn("No data found for Keyword={}", value.data.keyword.toString());
                return new DTORedeemEnriched(null, null, null);
            }
        } catch (SQLException ex) {
            LOG.error("Select query failed: {}", ex.getMessage(), ex);
            throw new RuntimeException("Select query failed", ex);
        } finally {
            ps.close();
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
