Test Plan for AQ Enhancements

Conventions
- Use existing test structure in `queue_test.go` as a template.
- Each test creates and cleans up its own queues/types. Guard for missing AQ privileges with Skip.
- Use transactions with explicit commit where needed.

1) JSON AQ (Classic)
- Setup:
  - Create Classic JSON queue table and queue:
    ```sql
    begin
      dbms_aqadm.create_queue_table(user||'.'||:tbl, 'JSON');
      dbms_aqadm.create_queue(user||'.'||:q, user||'.'||:tbl);
      dbms_aqadm.start_queue(user||'.'||:q);
    end;
    ```
- Enqueue/dequeue single message:
  - Open queue via `NewQueue(ctx, tx, qName, "JSON")`.
  - Enqueue map/array/bool/number/string/bytes/timestamp variants using `Message.JSON` (once implemented).
  - Dequeue and assert roundtrip equivalence using `JSON.GetValue(JSONOptDefault)`.
- Negative:
  - Bulk enq/deq with JSON should fail or be skipped per TEQ constraints; for Classic JSON, bulk may be unsupported (document and assert errors accordingly).

2) JSON AQ (TEQ) — optional if DB supports TEQ
- Setup:
  - `dbms_aqadm.create_sharded_queue(:q, queue_payload_type=>'JSON')` and start.
- Enqueue/dequeue single message only; assert bulk not supported.
- Ensure transformations are not set; verify attempt yields error.

3) Recipient lists (Classic)
- Setup:
  - Create RAW Classic queue.
- Enqueue a message with `Recipients = ["sub2", "sub3"]`.
- Dequeue with `DeqOptions.Consumer = "sub3"` and assert message is available; with another consumer not listed, assert not available.
- Ensure all recipients must dequeue before removal (enqueue two recipients and test behavior across two consumes).

4) AQ subscriptions (AQ namespace)
- Setup:
  - Create RAW Classic queue; enable events (`enableEvents=1`).
- Subscribe with namespace AQ; register callback; enqueue a message; assert callback receives event with queue name, consumer name, and msgid fields populated.
- Cleanup subscription.

5) TEQ-safe PurgeExpired behavior
- For Classic queue, exercise `PurgeExpired` and assert it executes.
- For TEQ, ensure `PurgeExpired` is a no-op or returns a well-defined error; assert behavior.

6) JMS-style object payload (smoke)
- Setup:
  - Create Classic queue table for JMS object type (e.g., `SYS.AQ$_JMS_TEXT_MESSAGE`), if available.
- Enqueue/dequeue object payload; assert attributes can be read (subject/text).
- Skip if JMS types not present.

7) Delivery modes and options
- Verify `DeliveryMode` mapping (persistent, buffered, both) on deq/enq options.
- Verify `Transformation`, `Visibility`, `Navigation`, `Wait`, `Condition`, `Correlation`, `MsgID` behaviors.

Test infra notes
- Reuse `testQueue` helper pattern; add new helpers for JSON and recipient flow.
- Include guard for Oracle bug 29928074 in bulk tests; skip or serialize accordingly.


