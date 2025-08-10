package godror_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

type execer interface {
    ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func testContext(name string) context.Context {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    // Attach cancel to context value for cleanup via t.Cleanup if needed in tests
    _ = cancel // tests should defer cancel() explicitly
    return ctx
}

func openTestDB(t *testing.T, enableEvents bool) *sql.DB {
    t.Helper()
    dsn := os.Getenv("GODROR_TEST_DSN")
    if dsn == "" {
        t.Skip("GODROR_TEST_DSN not set; skipping AQ tests")
    }
    P, err := godror.ParseConnString(dsn)
    if err != nil {
        t.Fatalf("ParseConnString: %+v", err)
    }
    if enableEvents {
        P.EnableEvents = true
    }
    db := sql.OpenDB(godror.NewConnector(P))
    t.Cleanup(func() { db.Close() })
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := db.PingContext(ctx); err != nil {
        t.Fatalf("Ping: %+v", err)
    }
    return db
}

func currentUser(t *testing.T, db *sql.DB) string {
    t.Helper()
    var user string
    if err := db.QueryRow("SELECT USER FROM DUAL").Scan(&user); err != nil {
        t.Fatalf("get USER: %+v", err)
    }
    return user
}

func skipIfNoAQPriv(t *testing.T, err error) {
    if err == nil {
        return
    }
    msg := err.Error()
    // PLS-00201 for missing DBMS_AQADM or ORA-01031 insufficient privileges
    if strings.Contains(msg, "PLS-00201") || strings.Contains(msg, "ORA-01031") || strings.Contains(msg, "ORA-06512") {
        t.Skipf("Skipping: missing AQ privileges: %v", err)
    }
}

func withTx(t *testing.T, db *sql.DB, fn func(context.Context, *sql.Tx)) {
    t.Helper()
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    tx, err := db.BeginTx(ctx, nil)
    if err != nil {
        t.Fatalf("BeginTx: %+v", err)
    }
    defer tx.Rollback()
    fn(ctx, tx)
    if err := tx.Commit(); err != nil {
        t.Fatalf("Commit: %+v", err)
    }
}

func isOraErr(err error, code string) bool {
    if err == nil {
        return false
    }
    var e interface{ Error() string }
    if errors.As(err, &e) {
        return strings.Contains(e.Error(), code)
    }
    return strings.Contains(err.Error(), code)
}


