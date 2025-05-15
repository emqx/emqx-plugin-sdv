#!/bin/bash

set -euo pipefail

# Smoke test: send a batch, mocking SDV platform

REQUEST_ID="$(date +%s)"
DATA_ID="data-id-$((RANDOM % 9 + 1))"
DATA="Testdata $DATA_ID"

mqttx pub -h 127.0.0.1 -p 1883 -t "\$SDV-FANOUT/data/$DATA_ID" -m "$DATA"

echo "Sent $DATA_ID: $DATA"

TRIGGER="{\"ids\": [\"1\", \"2\", \"3\"], \"request_id\": \"$REQUEST_ID\", \"data_id\": \"$DATA_ID\"}"

mqttx pub -h 127.0.0.1 -p 1883 -t '$SDV-FANOUT/trigger' -m "$TRIGGER"

echo "Sent $TRIGGER"
