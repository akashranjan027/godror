package godror_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	godror "github.com/godror/godror"
)

// TestPurgeExpiredBehavior documents current behavior for Classic vs TEQ.
// For Classic queues, PurgeExpired should execute. For TEQ, current helper is Classic-specific and may fail or no-op.
func TestPurgeExpiredBehavior(t *testing.T) {
    db := openTestDB(t, false)
    user := currentUser(t, db)

    const classicQName = "PURGE_CLASSIC_Q"
    const classicTbl = classicQName + "_TBL"
    const teqName = "PURGE_TEQ"

    // Setup Classic
    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        qry := fmt.Sprintf(`BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue('%s.%s'); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue('%s.%s'); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table('%s.%s', TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
  SYS.DBMS_AQADM.CREATE_QUEUE_TABLE('%s.%s', 'RAW');
  SYS.DBMS_AQADM.CREATE_QUEUE('%s.%s', '%s.%s');
  SYS.DBMS_AQADM.start_queue('%s.%s');
END;`, user, classicQName, user, classicQName, user, classicTbl, user, classicTbl, user, classicQName, user, classicTbl, user, classicQName)
        if _, err := tx.ExecContext(ctx, qry); err != nil {
            skipIfNoAQPriv(t, err)
            t.Fatalf("setup classic: %+v", err)
        }
    })
    t.Cleanup(func() {
        _, _ = db.Exec(`BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue(USER||'.` + classicQName + `'); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue(USER||'.` + classicQName + `'); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_queue_table(USER||'.` + classicTbl + `', TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
END;`)
    })

    // Classic purge works
    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        q, err := godror.NewQueue(ctx, tx, classicQName, "")
        if err != nil {
            t.Fatalf("NewQueue classic: %+v", err)
        }
        defer q.Close()
        if err := q.PurgeExpired(ctx); err != nil {
            t.Fatalf("PurgeExpired classic: %+v", err)
        }
    })

    // Setup TEQ (skip if not supported)
    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        plsql := fmt.Sprintf(`BEGIN
  BEGIN SYS.DBMS_AQADM.stop_queue('%s'); EXCEPTION WHEN OTHERS THEN NULL; END;
  BEGIN SYS.DBMS_AQADM.drop_sharded_queue('%s'); EXCEPTION WHEN OTHERS THEN NULL; END;
  SYS.DBMS_AQADM.create_sharded_queue(queue_name => '%s', queue_payload_type=>'RAW');
  SYS.DBMS_AQADM.start_queue('%s');
END;`, teqName, teqName, teqName, teqName)
        if _, err := tx.ExecContext(ctx, plsql); err != nil {
            if strings.Contains(err.Error(), "PLS-00201") || strings.Contains(err.Error(), "ORA-") {
                t.Skipf("TEQ not available or insufficient privileges: %v", err)
            }
        }
    })
    t.Cleanup(func() { _, _ = db.Exec(`BEGIN BEGIN SYS.DBMS_AQADM.drop_sharded_queue('` + teqName + `'); EXCEPTION WHEN OTHERS THEN NULL; END; END;`) })

    // TEQ purge with Classic helper: expect error or no-op
    withTx(t, db, func(ctx context.Context, tx *sql.Tx) {
        q, err := godror.NewQueue(ctx, tx, teqName, "")
        if err != nil {
            t.Logf("NewQueue TEQ returned error (expected possible): %v", err)
            return
        }
        defer q.Close()
        err = q.PurgeExpired(ctx)
        if err == nil {
            t.Logf("PurgeExpired on TEQ did not error; current implementation may be a no-op for TEQ")
        } else {
            t.Logf("PurgeExpired on TEQ errored as expected: %v", err)
        }
    })
}


