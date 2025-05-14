#!/bin/bash

VIN="$1"

if [ -z "$VIN" ]; then
    echo "Usage: $0 <vin>"
    exit 1
fi

mqttx sub -h 127.0.0.1 -p 1883 -i "$VIN" -t "agent/$VIN/proxy/request/+" -q 1
