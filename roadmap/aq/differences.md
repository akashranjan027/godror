Major differences between godror AQ and python-oracledb Thick AQ

Scope: Advanced Queuing (Classic AQ) and Transactional Event Queues (TEQ).

Findings

1) JSON AQ payloads
- python-oracledb: Supports JSON payload queues (Classic and TEQ) in Thick mode.
- godror: Not supported. Only RAW or named Oracle object payloads are handled.
  - Queue creation uses `dpiConn_newQueue` (object/RAW) only.
  - Payload setters/getters use `dpiMsgProps_setPayloadBytes / _setPayloadObject` and `dpiMsgProps_getPayload` (object/bytes). JSON APIs (`_setPayloadJson`, `_getPayloadJson`, `dpiConn_newJsonQueue`) are unused.

2) Recipient lists (Classic AQ)
- python-oracledb: Supports recipient lists (message-level targeting), via message properties.
- godror: No recipient support in `Message` and no call to `dpiMsgProps_setRecipients`.

3) AQ notifications / subscriptions (SUBSCR_NAMESPACE_AQ)
- python-oracledb: Supports AQ notifications namespace.
- godror: Subscription helper (`subscr.go`) hardcodes DB change notifications; no API to choose AQ namespace.

4) Transactional Event Queues (TEQ) nuances
- python-oracledb: TEQ supported in Thick mode with constraints (e.g., transformations not supported, recipient lists not supported, JSON single-message only).
- godror: Core enq/deq likely works but:
  - `PurgeExpired` uses `dbms_aqadm.purge_queue_table(...)` which applies to Classic queue tables, not TEQ.
  - No explicit guards for TEQ option constraints.

5) JMS payloads
- python-oracledb: Supports JMS payloads in Thick mode.
- godror: No explicit helpers. JMS are Oracle object types so basic support may work via object payloads, but no samples/tests.

6) Bulk enq/deq and delivery modes
- Both support enqMany/deqMany and delivery modes mapping.
- Known Oracle bug 29928074 documented in both.
- python-oracledb notes MSG_BUFFERED not supported for bulk in Thick mode; godror has no explicit guard (server will enforce).

Summary of gaps to close first
- JSON AQ support (queue open + payload set/get). ------------------ Important to verify
- Recipient lists on messages.                    ------------------ Is not backported for teq
- AQ namespace subscriptions.                     ------------------ Not on priority
- TEQ-safe operations (avoid Classic-only procedures; document/guard unsupported combos).


