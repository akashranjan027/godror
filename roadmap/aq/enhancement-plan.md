Enhancement Plan for godror AQ parity

Milestone 1 — JSON AQ support
- Goal: Enable JSON payload queues and JSON message payloads.
- Design:
  - Queue creation: Add detection of JSON queue (e.g., `payloadObjectTypeName == "JSON"` or explicit option), then call `dpiConn_newJsonQueue` instead of `dpiConn_newQueue`.
  - Message payload: Extend `Message` with `JSON JSON` (reusing existing `json.go` abstractions) or a light wrapper to set/get JSON via ODPI-C. Implement:
    - Enqueue: if `Message.JSON` is set, call `dpiMsgProps_setPayloadJson` with `*dpiJson` created via `dpiConn_newJson` and filled from Go value or `JSONString`.
    - Dequeue: if JSON payload received (object is nil and ODPI-C indicates JSON via `getPayloadJson`), populate `Message.JSON` and optionally expose helpers to convert to string/value.
  - API ergonomics: Provide helpers to bind native Go values to JSON (leveraging `JSONValue` and friends).
- Touchpoints:
  - `queue.go`: `NewQueue`, `Message.toOra`, `Message.fromOra`.
  - `odpi/include/dpi.h` JSON queue/payload functions already available through cgo.
- Risks/Notes:
  - Coordinate with existing `json.go` types; avoid duplicate ownership of `dpiJson` handles.
  - Ensure memory management and finalizers are correct.

Milestone 2 — Recipient lists for Classic AQ
- Goal: Allow specifying per-message recipients.
- Design:
  - Extend `Message` with `Recipients []string`.
  - In `Message.toOra`, when non-empty, build `[]dpiMsgRecipient` and call `dpiMsgProps_setRecipients`.
- Touchpoints:
  - `queue.go`: `Message` struct, `toOra` implementation.
  - `odpi` functions already present.

Milestone 3 — AQ namespace subscriptions
- Goal: Expose AQ notifications via subscriptions.
- Design:
  - Add a `SubscrNamespace` option to `NewSubscription` to choose between DB change and AQ (use `DPI_SUBSCR_NAMESPACE_AQ`).
  - Define an AQ-specific callback payload wrapper if needed (queue name, consumer name, msgid available from ODPI-C).
- Touchpoints:
  - `subscr.go`: add option, wire `params.subscrNamespace` accordingly; surface AQ event data.

Milestone 4 — TEQ compatibility and guardrails
- Goal: Make helpers TEQ-safe and document constraints.
- Design:
  - Modify `PurgeExpired`: detect queue type; for TEQ, either no-op with warning or provide TEQ purge guidance.
  - Document unsupported combinations: transformations with TEQ, recipient lists with TEQ, JSON array bulk with TEQ.
- Touchpoints:
  - `queue.go`: `PurgeExpired` add detection (query `user_queues` and/or `dbms_aqadm` metadata), or split into Classic-only helper.
  - Docs: README and Go docstrings.

Milestone 5 — Samples and docs
- Add examples for RAW, JSON, object, JMS-style object payloads.
- Show recipient lists usage and AQ subscription sample.

Milestone 6 — Performance & stability
- Validate parallel enqMany behavior and add retries/workarounds; document client version requirements.

Out of scope for now
- Async API parity.
- TEQ creation helpers (DBA operations).


