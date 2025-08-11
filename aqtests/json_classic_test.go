package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestJSONClassicQueue documents current behavior: JSON AQ is not supported yet.
// It creates a Classic JSON queue and attempts to use godror.NewQueue with payload "JSON".
// Expected: either queue open fails, or enqueue/dequeue fails due to unsupported JSON path.
func TestJSONClassicQueue(t *testing.T) {
    db := openTestDB(t, false)
    user := currentUser(t, db)

    const qName = "DEMO_JSON_Q"
    const qTblName = qName + "_TBL"

    setup := func(ctx context.Context, db execer, user string) error {
        qry := fmt.Sprintf(`DECLARE
  tbl CONSTANT VARCHAR2(61) := '%s.%s';
  q   CONSTANT VARCHAR2(61) := '%s.%s';
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

  SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'JSON');
  SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
  SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '%s');
  SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '%s');
  SYS.DBMS_AQADM.start_queue(q);
END;`, user, qTblName, user, qName, user, user)
        _, err := db.ExecContext(ctx, qry)
        return err
    }
    teardown := func(ctx context.Context, db execer, user string) error {
        _, _ = db.ExecContext(ctx, `DECLARE
  tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
  q   CONSTANT VARCHAR2(61) := USER||'.'||:2;
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
END;`, qTblName, qName)
        return nil
    }

    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        if err := setup(ctx, tx, user); err != nil {
            skipIfNoAQPriv(t, err)
            t.Fatalf("setup: %+v", err)
        }
    })
    t.Cleanup(func() { _ = teardown(context.Background(), db, user) })

    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        // Attempt to open JSON queue using payload type "JSON"
        q, err := godror.NewQueue(ctx, tx, qName, "JSON")
        if err != nil {
            t.Logf("NewQueue JSON failed as expected: %v", err)
            return
        }
        defer q.Close()

        // Try to enqueue a JSON payload. Current code path does not support JSON; expect failure.
        jsonObj := map[string]any{"name": "John", "age": 30, "active": true}
        buf := &bytes.Buffer{}
        _ = json.NewEncoder(buf).Encode(jsonObj)
        // Without native JSON path, Raw will be used, which is not valid for a JSON queue.
        msgs := []godror.Message{{Raw: buf.Bytes(), Expiration: 30 * time.Second}}
        err = q.Enqueue(msgs)
        if err == nil {
            t.Fatalf("enqueue unexpectedly succeeded on JSON queue without JSON binding support")
        }
        t.Logf("enqueue failed as expected: %v", err)
    })
}


