#!/usr/bin/env bash
set -e

MODE=$1
PROFILE="${2:-full}"

if [[ -z "$MODE" || -z "$PROFILE" ]]; then
  echo "Usage: $0 <mode: dev|prod> <profile>"
  exit 1
fi

case "$MODE" in
  dev)
    echo "Starting in DEV mode with profile: $PROFILE"
    ./start_dev.sh "$PROFILE"
    ;;
  prod)
    echo "Starting in PROD mode with profile: $PROFILE"
    ./start_prod.sh "$PROFILE"
    ;;
  flink)
    echo "Starting Flink"
    ./start_flink.sh
    ;;
  flink-cli)
    echo "Starting Flink CLI"
    ./start_flink_cli.sh
    ;;
  *)
    echo "❌ Error: Unknown mode '$MODE'. Allowed values: dev | prod"
    exit 1
    ;;
esac
