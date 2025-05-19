# 0.2.1

- Check subscription exists before sending the message to subscriber session.
  This should avoid indefinite wait for PUBACK if message dispatched before subscription is inserted.

# 0.2.0

- Add retention based garbage collection for pending fanouts.
- Make fanout message topic prefix pattern configurable.

# 0.1.0

- aBasic functionality
