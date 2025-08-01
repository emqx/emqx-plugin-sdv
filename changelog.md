# 0.4.6

- Fix handling of a race condition after vehicle client reconnects.
- Add a safety guard for dispatcher pool restart caused inflight table leak.

# 0.4.5

- Fix a UI style hint: for pool size config, it should be integer input.

# 0.4.4

- Release the plugin based on OTP 26

# 0.4.3

- Fix wait_for_table and wait_for_shard order.

# 0.4.2

- Non-blocking start of plugin app. Async wait for tables. Report unhealthy before tables are ready.

# 0.4.1

- Move wait for tables from spplication start callback to dispatcher process `handle_continue` so to prevent plugin app start timeout.

# 0.4.0

- Added CLI `emqx ctl sdv status` to inspect fanout status.

# 0.3.0

- For each trigger notification (batch of VINs), group subscribers by EMQX node name,
  so only one message is sent in RPC for connected vehicles.
  For each node, group subscribers by dispatcher, so only one message is sent for
  each dispatched process.

# 0.2.1

- Check subscription exists before sending the message to subscriber session.
  This should avoid indefinite wait for PUBACK if message dispatched before subscription is inserted.

# 0.2.0

- Add retention based garbage collection for pending fanouts.
- Make fanout message topic prefix pattern configurable.

# 0.1.0

- Basic functionality
