# EMQX SDV Fanout

This EMQX plugin is to implement message fanout for EMQ SDV platform.
The messages are published from SDV platform with a batch of VINs for each request (identified by a unique RequestID).

This plugin will receive the messages and store them in mnesia (rocksdb) tables.
Then it will fanout the messages to the corresponding VINs in a reactive manner.

<img src="overview.png" alt="Overview" width="600">

## Data Format

The messages published from SDV platform are in JSON format with the following fields:

`ids`: Array of VINs
`request_id`: `task_{tasktype}_{per-type-number}`, per-type-number is finite. e.g. (1-10)
`data`: Transparent binary blob (maybe compressed) to be fanouted to the corresponding subscribers as MQTT message payload.

## Data Storage

The data is stored in two mnesia (rocksdb) tables.

`sdv_fanout_data`: Key is the `sha1` of the data, value is the timestamp and the binary data itself.
`sdv_fanout_ids`: Key is a composite key of `vin` and `request_id`, value is the timestamp and the `sha1` of the data.

### Compaction

If data has an existing `sha1` checksum in data table, no need to write again.

The 200 IDs in the original messages might be just a fraction of a larger batch, so the hashing will significantly reduce the amount of data to be written.

### Garbage collection

This plugin runs periodic garbage collection to delete `sdv_fanout_data` record if there is no reference left from `sdv_fanout_ids` table.

The `sdv_fanout_data` and `sdv_fanout_ids` table will be deleted if the timestamp is older than the configured retention period.

## Data Flow

### Realtime Forwarding

If the client (identified by VIN) is currently online, try to deliver the message immediately. But the data might still need to be stored for async ACK handling (or if any client is not online).

### Resume After Reconnection

Subscribes (either new subscribe, or resume session after session takeover) to `agent/${VIN}/proxy/request/+`, plugin will read from `sdv_fanout_ids` table, if found, read from `sdv_fanout_data` table, and publish data as payload to this subscription with QoS=1 topic = `agent/${VIN}/proxy/request/${request_id}`, and keep packet ID for correlation in process dictionary.

Subscribes sends `PUBACK`, plugin will find the `request_id` and delete from `sdv_fanout_ids` table.
