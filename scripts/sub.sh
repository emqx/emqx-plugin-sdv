#!/bin/bash

# Smoke test: subscribe to fanout topic, mock a vehicle.

set -euo pipefail
VIN="${1:-}"

if [ -z "$VIN" ]; then
    echo "Usage: $0 VIN"
    echo "  VIN can be 1, 2, or 3, see the VIN list in send.sh"
    exit 1
fi

mqttx sub -h 127.0.0.1 -p 1883 -i "$VIN" -t "agent/$VIN/proxy/request/+" -q 1
