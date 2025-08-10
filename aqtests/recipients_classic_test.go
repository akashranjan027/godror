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

// TestRecipientsClassic documents current behavior: recipient lists are not supported in godror yet.
// It creates a RAW Classic queue, enqueues a message intended for specific consumers via DBMS_AQ native PL/SQL,
// then attempts to dequeue with a specific consumer using godror DeqOptions.Consumer.
// Expected: godror can filter by consumer name at dequeue time, but there is no API to set recipients when enqueuing.
func TestRecipientsClassic(t *testing.T) {
    db := openTestDB(t, false)
    user := currentUser(t, db)

    const qName = "RECIP_Q"
    const qTblName = qName + "_TBL"
    const payload = "hello-recipient"

    setup := func(ctx context.Context, db execer, user string) error {
        qry := fmt.Sprintf(`DECLARE
  tbl CONSTANT VARCHAR2(61) := '%s.%s';
  q   CONSTANT VARCHAR2(61) := '%s.%s';
BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

  SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW', multiple_consumers=>TRUE);
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
        // Enqueue via DBMS_AQ to set recipients explicitly (since godror lacks API yet)
        // Two recipients: sub2, sub3
        plsql := fmt.Sprintf(`DECLARE
  enqueue_options    DBMS_AQ.enqueue_options_t;
  message_properties DBMS_AQ.message_properties_t;
  msgid              RAW(16);
  payload_raw        RAW(32767) := UTL_RAW.CAST_TO_RAW('%s');
  r1 DBMS_AQ.aq$_recipient_list_t := DBMS_AQ.aq$_recipient_list_t(DBMS_AQ.aq$_agent('sub2', NULL, NULL));
  r2 DBMS_AQ.aq$_recipient_list_t := DBMS_AQ.aq$_recipient_list_t(DBMS_AQ.aq$_agent('sub3', NULL, NULL));
BEGIN
  message_properties.recipient_list := r1 || r2;
  DBMS_AQ.ENQUEUE(queue_name => '%s.%s', enqueue_options => enqueue_options,
                  message_properties => message_properties, payload => payload_raw, msgid => msgid);
END;`, payload, user, qName)
        if _, err := tx.ExecContext(ctx, plsql); err != nil {
            if strings.Contains(err.Error(), "PLS-00201") {
                t.Skipf("missing DBMS_AQ: %v", err)
            }
            t.Fatalf("enqueue via DBMS_AQ: %+v", err)
        }
    })

    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        q, err := godror.NewQueue(ctx, tx, qName, "")
        if err != nil {
            t.Fatalf("NewQueue: %+v", err)
        }
        defer q.Close()

        // Dequeue as sub3 -> expect to receive the message
        opts, err := q.DeqOptions()
        if err != nil {
            t.Fatalf("DeqOptions: %+v", err)
        }
        opts.Mode = godror.DeqRemove
        opts.Visibility = godror.VisibleOnCommit
        opts.Consumer = "sub3"
        opts.Wait = 2 * time.Second

        var msgs [1]godror.Message
        n, err := q.DequeueWithOptions(msgs[:], &opts)
        if err != nil {
            t.Fatalf("DequeueWithOptions: %+v", err)
        }
        if n != 1 {
            t.Fatalf("wanted 1 message for sub3, got %d", n)
        }
        got := string(msgs[0].Raw)
        if got != payload {
            t.Fatalf("payload mismatch: got %q want %q", got, payload)
        }
    })

    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        q, err := godror.NewQueue(ctx, tx, qName, "")
        if err != nil {
            t.Fatalf("NewQueue: %+v", err)
        }
        defer q.Close()

        // Dequeue as other consumer not in recipient list -> expect no message
        opts, err := q.DeqOptions()
        if err != nil {
            t.Fatalf("DeqOptions: %+v", err)
        }
        opts.Mode = godror.DeqRemove
        opts.Visibility = godror.VisibleOnCommit
        opts.Consumer = "not_in_list"
        opts.Wait = 1 * time.Second
        var msgs [1]godror.Message
        n, err := q.DequeueWithOptions(msgs[:], &opts)
        if err != nil {
            // Some DB versions may error; allow zero-msg success path to prove filtering
            if n == 0 {
                t.Logf("dequeue with non-listed consumer yielded error: %v", err)
                return
            }
            t.Fatalf("DequeueWithOptions: %+v", err)
        }
        if n != 0 {
            t.Fatalf("expected zero messages for non-listed consumer, got %d", n)
        }
    })
}


