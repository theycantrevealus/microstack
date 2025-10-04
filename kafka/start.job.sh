#!/usr/bin/env bash
set -euo pipefail
docker exec -it flink-jobmanager flink run -d -c com.mitrabhaktiinformasi.flinkcdc.CDCPostgres /opt/flink/jobs/tere-cdc-process.jar -ynm "CDC-Keyword"