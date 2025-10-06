#!/usr/bin/env bash
set -euo pipefail
docker exec -it flink-jobmanager flink run -d -c com.mitrabhaktiinformasi.CDCPostgres /opt/flink/jobs/tere-cdc-process.jar -ynm "CDC-Master"
docker exec -it flink-jobmanager flink run -d -c com.mitrabhaktiinformasi.TERETransaction /opt/flink/jobs/tere-transaction-stream.jar -ynm "Stream-Transaction"