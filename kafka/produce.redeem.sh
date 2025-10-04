#!/usr/bin/env bash
set -euo pipefail

docker exec -i kafka-broker-1 kafka-console-producer \
  --bootstrap-server kafka-broker-1:9092 \
  --producer.config /etc/kafka/properties/client.properties \
  --topic TERE_redeem \
  --property "parse.key=true" \
  --property "key.separator=|" <<EOF
{"transaction_id":"TRX_618251002152144573481"}|{"data":{"locale":"id-ID","msisdn":"6282257479618","keyword":"SAMPLE_KEYWORD","transaction_id":"TRX_618251002152144573481","channel_id":"R1","callback_url":"https://api.digitalcore.telkomsel.co.id/secure/Points/Service/RedeemCallback/kftgjb9v4rw3w2tptmbwgcpg?callback_url=httpspost:sl:https://10.54.58.49:7443/v1/transactions/prepaid/callback","transaction_source":"api_v1","send_notification":true},"account":{"_id":"654a1b417c5a693f294d0c1d","user_id":"user-654a1b412fae6ce2adbf21cf"},"token":"Bearer AhyoBiV5jZ1U4NvgzAOLgTdQ95JgRUKg5cPx5bMvnE7Uuh8ROdqNGo5.iKObQAIAsJVzM2mQl58gqJPE3yAyHJIl0d+msiC0+5k","transaction_id":"TRX_618251002152145460748","path":"/v1/redeem","keyword_priority":"DEFAULT"}
EOF