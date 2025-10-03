SET sql-client.execution.result-mode=TABLEAU;

SET 'pipeline.name' = 'Transaction-Eligibility-Processor';
INSERT INTO Kafka_eligibility_sink
SELECT * FROM Flink_eligibility;

-- INSERT INTO DebugSink
-- SELECT *
-- FROM Flink_eligibility;



-- SET 'pipeline.name' = 'CDC-Update-Keyword';
-- INSERT INTO Flink_keyword
-- SELECT
--     fullDocument._id AS `_id`,
--     fullDocument.eligibility.name AS `keyword_name`,
--     fullDocument.eligibility.start_period AS `start_period`,
--     fullDocument.eligibility.end_period AS `end_period`
-- FROM TERE_SLNonCore_keyword
-- WHERE operationType IN ('insert', 'update');

-- SET 'pipeline.name' = 'CDC-Deletion-Keyword';
-- INSERT INTO Flink_keyword
-- SELECT
--     documentKey._id AS _id,
--     CAST(NULL AS STRING) AS keyword_name,
--     CAST(NULL AS ROW<`$date` BIGINT>) AS start_period,
--     CAST(NULL AS ROW<`$date` BIGINT>) AS end_period
-- FROM TERE_SLNonCore_keyword
-- WHERE operationType = 'delete';

-- ====================================================================================================================================================================================

-- SET 'pipeline.name' = 'CDC-Keyword-Processor';

-- INSERT INTO Flink_keyword
-- SELECT
--     fullDocument._id,
--     fullDocument.eligibility.name,
--     fullDocument.eligibility.start_period,
--     fullDocument.eligibility.end_period
-- FROM TERE_SLNonCore_keyword
-- WHERE operationType IN ('insert', 'update')

-- UNION ALL

-- SELECT
--     documentKey._id,
--     CAST(NULL AS STRING),
--     CAST(NULL AS ROW<`$date` BIGINT>),
--     CAST(NULL AS ROW<`$date` BIGINT>)
-- FROM TERE_SLNonCore_keyword
-- WHERE operationType = 'delete';