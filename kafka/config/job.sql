SET sql-client.execution.result-mode=TABLEAU;

SET 'pipeline.name' = 'Transaction-Eligibility-Processor';
INSERT INTO Kafka_eligibility_sink
SELECT
  *,
  COALESCE(
    TIMESTAMPDIFF(FRAC_SECOND, kafka_timestamp, CURRENT_TIMESTAMP), 
    0
  ) AS latency_ms
FROM Flink_eligibility;