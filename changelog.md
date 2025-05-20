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
