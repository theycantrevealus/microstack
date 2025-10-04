#!/usr/bin/env bash
set -euo pipefail

docker rm -f postgres 2>/dev/null || true
docker run --rm --name postgres \
  --network kafka-cluster-network \
  -e POSTGRES_USER=tere \
  -e POSTGRES_PASSWORD=mypassword \
  -e POSTGRES_DB=TERE \
  -p 5432:5432 \
  -d postgres:latest