#!/bin/bash

# Send a message to the SDV platform

REQUEST_ID=$(date +%s)
PAYLOAD="{\"ids\": [\"1\", \"2\"], \"request_id\": \"$REQUEST_ID\", \"data\": \"testdata $(date)\"}"

echo "Sending $PAYLOAD"

mqttx pub -h 127.0.0.1 -p 1883 -t '$SDV-FANOUT' -m "$PAYLOAD"

echo "Sent $PAYLOAD"
