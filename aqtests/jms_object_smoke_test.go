package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

// TestJMSObjectSmoke attempts to enqueue/dequeue a JMS-like object payload.
// If JMS types are unavailable, the test is skipped. This is a smoke test to document object payload behavior.
func TestJMSObjectSmoke(t *testing.T) {
    db := openTestDB(t, false)
    user := currentUser(t, db)

    const qName = "JMS_Q"
    const qTblName = qName + "_TBL"
    // Common JMS text type in Oracle installations
    const jmsTextType = "SYS.AQ$_JMS_TEXT_MESSAGE"

    // Pre-check type existence
    var cnt int
    if err := db.QueryRow("SELECT COUNT(*) FROM ALL_TYPES WHERE UPPER(OWNER||'.'||TYPE_NAME)=:1", strings.ToUpper(jmsTextType)).Scan(&cnt); err != nil || cnt == 0 {
        t.Skipf("JMS type %s not found; skipping", jmsTextType)
    }

    setup := func(ctx context.Context, db execer, user string) error {
        qry := fmt.Sprintf(`DECLARE
  tbl CONSTANT VARCHAR2(61) := '%s.%s';
  q   CONSTANT VARCHAR2(61) := '%s.%s';
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

  SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, '%s');
  SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
  SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '%s');
  SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '%s');
  SYS.DBMS_AQADM.start_queue(q);
END;`, user, qTblName, user, qName, jmsTextType, user, user)
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
        q, err := godror.NewQueue(ctx, tx, qName, jmsTextType)
        if err != nil {
            t.Fatalf("NewQueue: %+v", err)
        }
        defer q.Close()

        // Build JMS object
        obj, err := q.PayloadObjectType.NewObject()
        if err != nil {
            t.Fatalf("NewObject: %+v", err)
        }
        defer obj.Close()

        // JMS text attribute is "TEXT_VC" or "TEXT" depending on DB version; try both
        attrNames := []string{"TEXT_VC", "TEXT"}
        var setErr error
        for _, a := range attrNames {
            if e := obj.Set(a, "hello-jms"); e == nil {
                setErr = nil
                break
            } else {
                setErr = e
            }
        }
        if setErr != nil {
            t.Skipf("cannot set JMS text attribute: %v", setErr)
        }

        msgs := []godror.Message{{Object: obj, Expiration: 30 * time.Second}}
        if err := q.Enqueue(msgs); err != nil {
            t.Fatalf("enqueue: %+v", err)
        }
    })

    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        q, err := godror.NewQueue(ctx, tx, qName, jmsTextType)
        if err != nil {
            t.Fatalf("NewQueue: %+v", err)
        }
        defer q.Close()

        var msgs [1]godror.Message
        n, err := q.Dequeue(msgs[:])
        if err != nil {
            t.Fatalf("dequeue: %+v", err)
        }
        if n != 1 {
            t.Fatalf("expected 1 message, got %d", n)
        }
        if msgs[0].Object == nil {
            t.Fatalf("expected object payload, got nil")
        }
        defer msgs[0].Object.Close()
    })
}


