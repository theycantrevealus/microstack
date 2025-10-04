SET sql-client.execution.result-mode=TABLEAU;
-- INSERT INTO Flink_keyword_pg
-- SELECT
--     documentKey._id.`$oid` AS _id,
--     CAST(fullDocument AS STRING) AS document,
--     operationType
-- FROM TERE_SLNonCore_keyword;

INSERT INTO TERE_keyword
SELECT
    documentKey._id.`$oid` AS _id,
    CAST(fullDocument AS STRING) AS document,
    operationType
FROM TERE_SLNonCore_keyword
WHERE operationType IN ('insert', 'update');

DELETE FROM TERE_keyword AS tk
WHERE tk._id IN (
    SELECT kk.fullDocument._id.`$oid`
    FROM TERE_SLNonCore_keyword AS kk
    WHERE kk.operationType = 'delete'
);


-- SET 'pipeline.name' = 'Master-CDC-Keyword';
-- INSERT INTO Flink_keyword
-- SELECT
--     fullDocument._id.`$oid` AS _id,
--     CASE
--         WHEN operationType = 'delete' THEN NULL
--         ELSE fullDocument
--     END AS fullDocument
-- FROM TERE_SLNonCore_keyword
-- WHERE operationType IN ('insert', 'update', 'delete');
-- INSERT INTO Flink_keyword_pg
-- SELECT
--     documentKey._id.`$oid` AS _id,
--     CAST(fullDocument AS STRING) AS document,
--     operationType
-- FROM TERE_SLNonCore_keyword;

-- ====================================================================================================================================================================================
-- SET 'pipeline.name' = 'Transaction-Eligibility-Processor';
-- INSERT INTO Kafka_eligibility_sink
-- SELECT
--   *,
--   COALESCE(
--     TIMESTAMPDIFF(FRAC_SECOND, kafka_timestamp, CURRENT_TIMESTAMP), 
--     0
--   ) AS latency_ms
-- FROM Flink_eligibility;