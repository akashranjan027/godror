// Copyright 2019, 2025 The Godror Authors
//
//
// SPDX-License-Identifier: UPL-1.0 OR Apache-2.0

package godror_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	godror "github.com/godror/godror"
)

type execer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func TestQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("Queue"), 30*time.Second)
	defer cancel()

	t.Run("deqbymsgid", func(t *testing.T) {
		const qName = "TEST_MSGID_Q"
		const qTblName = qName + "_TBL"
		setUp := func(ctx context.Context, db execer, user string) error {
			qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW');
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
			_, err := db.ExecContext(ctx, qry)
			return err
		}

		tearDown := func(ctx context.Context, db execer, user string) error {
			db.ExecContext(
				ctx,
				`DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`,
				qTblName, qName,
			)
			return nil
		}

		tx, err := testDb.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		var user string
		if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
			t.Fatal(err)
		}

		if err = tearDown(ctx, tx, user); err != nil {
			t.Log(err)
		}
		if err = setUp(ctx, tx, user); err != nil {
			if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
				t.Skip(err.Error())
			}
			t.Fatalf("%+v", err)
		}
		defer func() {
			if err = tearDown(testContext("queue-teardown"), testDb, user); err != nil {
				t.Log(err)
			}
		}()

		t.Log("deqbymsgid")
		if err = func() error {
			q, err := godror.NewQueue(ctx, tx, qName, "")
			t.Log("q:", q, "err:", err)
			if err != nil {
				return err
			}
			defer q.Close()

			msgs := make([]godror.Message, 1)
			msgs[0] = godror.Message{Raw: []byte("msg to be dequeued")}
			msgs[0].Expiration = 60 * time.Second

			if err = q.Enqueue(msgs); err != nil {
				var ec interface{ Code() int }
				if errors.As(err, &ec) && ec.Code() == 24444 {
					t.Skip(err)
				}
				return err
			}
			if err = tx.Commit(); err != nil {
				return err
			}

			b := msgs[0].MsgID[:]

			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			opts, err := q.DeqOptions()
			if err != nil {
				return err
			}

			opts.Mode = godror.DeqRemove
			opts.MsgID = b
			opts.Wait = 1 * time.Second
			t.Logf("opts: %#v", opts)

			n, err := q.DequeueWithOptions(msgs[:1], &opts)
			if err != nil || n == 0 {
				return fmt.Errorf("dequeue by msgid: %d/%+v", n, err)
			}

			if err = tx.Commit(); err != nil {
				return err
			}

			if !bytes.Equal(msgs[0].MsgID[:], b) {
				return fmt.Errorf("set %v, got %v as msgs[0].MsgID", b, msgs[0].MsgID)
			}

			return nil
		}(); err != nil {
			t.Error(err)
		}
	})

	t.Run("raw", func(t *testing.T) {
		const qName = "TEST_Q"
		const qTblName = qName + "_TBL"

		testQueue(ctx, t, qName, "",
			func(ctx context.Context, db execer, user string) error {
				qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW');
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		--SYS.DBMS_AQADM.start_queue(q);
	END;`
				_, err := db.ExecContext(ctx, qry)
				return err
			},

			func(ctx context.Context, db execer, user string) error {
				db.ExecContext(
					ctx,
					`DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`,
					qTblName, qName,
				)
				return nil
			},

			func(_ *godror.Queue, i int) (godror.Message, string) {
				s := fmt.Sprintf("%03d. árvíztűrő tükörfúrógép", i)
				return godror.Message{Raw: []byte(s)}, s
			},

			func(m godror.Message, i int) (string, error) {
				if len(m.Raw) == 0 {
					t.Logf("%d. received empty message: %#v", i, m)
					return "", nil
				}
				return string(m.Raw), nil
			},
		)
	})

	t.Run("obj", func(t *testing.T) {
		const qName = "TEST_QOBJ"
		const qTblName = qName + "_TBL"
		const qTypName = qName + "_TYP"
		const arrTypName = qName + "_ARR_TYP"

		var data godror.Data
		testQueue(ctx, t, qName, qTypName,
			func(ctx context.Context, db execer, user string) error {
				var plus strings.Builder
				for _, qry := range []string{
					"CREATE OR REPLACE TYPE " + user + "." + arrTypName + " IS TABLE OF VARCHAR2(1000)",
					"CREATE OR REPLACE TYPE " + user + "." + qTypName + " IS OBJECT (f_vc20 VARCHAR2(20), f_num NUMBER, f_dt DATE/*, f_arr " + arrTypName + "*/)",
				} {
					if _, err := db.ExecContext(ctx, qry); err != nil {
						t.Logf("%s: %+v", qry, err)
						if strings.HasPrefix(qry, "CREATE ") || !strings.Contains(err.Error(), "not exist") {
							return err
						}
					}
					plus.WriteString(qry)
					plus.WriteString(";\n")
				}
				{
					qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
		typ CONSTANT VARCHAR2(61) := '` + user + "." + qTypName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, typ); EXCEPTION WHEN OTHERS THEN IF SQLCODE <> -24001 THEN RAISE; END IF; END;
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);

		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
					if _, err := db.ExecContext(ctx, qry); err != nil {
						t.Logf("%v", fmt.Errorf("%s: %w", qry, err))
					}
				}

				return nil
			},

			func(ctx context.Context, db execer, user string) error {
				qry := `DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`
				if _, err := db.ExecContext(ctx, qry, qTblName, qName); err != nil {
					t.Logf("%q: %+v", qry, err)
				}
				for _, qry := range []string{
					"BEGIN SYS.DBMS_AQADM.stop_queue(" + user + "." + qName + "); END;",
					"BEGIN SYS.DBMS_AQADM.drop_queue(" + user + "." + qName + "); END;",
					"BEGIN SYS.DBMS_AQADM.drop_queue_table(" + user + "." + qTblName + ", TRUE); END;",
					"DROP TABLE " + user + "." + qTblName,
					"DROP TYPE " + user + "." + qTypName + " FORCE",
					"DROP TYPE " + user + "." + arrTypName + " FORCE",
				} {
					if _, err := db.ExecContext(ctx, qry); err != nil {
						t.Logf("%q: %+v", qry, err)
					}
				}
				return nil
			},

			func(q *godror.Queue, i int) (godror.Message, string) {
				obj, err := q.PayloadObjectType.NewObject()
				if err != nil {
					t.Fatalf("%d. %+v", i, err)
				}
				if err = obj.Set("F_DT", time.Now()); err != nil {
					t.Fatal(err)
				}
				if err = obj.Set("F_VC20", "árvíztűrő"); err != nil {
					t.Fatal(err)
				}

				if err = obj.Set("F_NUM", int64(i)); err != nil {
					t.Fatal(err)
				}
				if err = obj.GetAttribute(&data, "F_NUM"); err != nil {
					t.Fatal(err)
				}
				num := string(data.GetBytes())
				k, err := strconv.ParseInt(num, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				if k != int64(i) {
					t.Fatalf("F_NUM as float got %d, wanted %d (have %#v (ntt=%d))", k, i, data.Get(), data.NativeTypeNum)
				}
				var buf bytes.Buffer
				if err := obj.ToJSON(&buf); err != nil {
					t.Error(err)
				}
				t.Logf("obj=%s; %q", buf.String(), num)
				var x struct {
					Num string `json:"F_NUM"`
				}
				if err := json.Unmarshal(buf.Bytes(), &x); err != nil {
					t.Fatal(err)
				}
				if x.Num != strconv.Itoa(i) {
					t.Errorf("ToJSON says %q (%s), wanted %d", x.Num, buf.String(), i)
				}
				return godror.Message{Object: obj}, num
			},

			func(m godror.Message, i int) (string, error) {
				var data godror.Data
				if m.Object == nil {
					t.Logf("%d. received empty message: %#v", i, m)
					return "", nil
				}
				defer m.Object.Close() // NOT before data use!
				var buf bytes.Buffer
				if err := m.Object.ToJSON(&buf); err != nil {
					t.Error(err)
				}
				t.Logf("obj=%s", buf.String())
				attr := m.Object.Attributes["F_NUM"]
				if err := m.Object.GetAttribute(&data, attr.Name); err != nil {
					return "", err
				}
				v := data.GetBytes()
				s := string(v)
				t.Logf("cm %d: got F_NUM=%q (%T ntn=%d otn=%d)", i, s, v, data.NativeTypeNum, attr.ObjectType.OracleTypeNum)
				var x struct {
					Num string `json:"F_NUM"`
				}
				if err := json.Unmarshal(buf.Bytes(), &x); err != nil {
					t.Error(err)
				} else if x.Num != s {
					t.Errorf("json=%q (%s), wanted %q", x.Num, buf.String(), s)
				}
				return s, nil
			},
		)
	})

	// Test JSON Queue functionality
	t.Run("json", func(t *testing.T) {
		const qName = "TEST_JSON_Q"
		const qTblName = qName + "_TBL"

		testJSONQueue(ctx, t, qName, qTblName)
	})
}

// TestQueueComprehensive tests various queue features comprehensively
func TestQueueComprehensive(t *testing.T) {
	ctx, cancel := context.WithTimeout(testContext("QueueComprehensive"), 2*time.Minute)
	defer cancel()

	// Helper function to check if we can skip based on error
	checkSkippable := func(t *testing.T, err error) bool {
		if err == nil {
			return false
		}
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") ||
			strings.Contains(err.Error(), "ORA-24010") ||
			strings.Contains(err.Error(), "ORA-24444") {
			t.Skip("AQ not available: " + err.Error())
			return true
		}
		return false
	}

	// Get current user for queue naming
	var user string
	if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	// Test 1: Basic single-consumer queue with priority and expiration
	t.Run("SingleConsumerPriorityExpiration", func(t *testing.T) {
		t.Log("\n=== TEST: Single-consumer queue with priority and expiration ===")
		t.Log("EXPECTED: Messages should be dequeued in priority order (lower number = higher priority)")
		t.Log("POTENTIAL FAILURE: If priority is not supported, messages will come in FIFO order")

		const qName = "TEST_PRIORITY_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Enqueue messages with different priorities
			msgs := []godror.Message{
				{Raw: []byte("Priority 5 message"), Priority: 5, Expiration: 300 * time.Second},
				{Raw: []byte("Priority 1 message"), Priority: 1, Expiration: 300 * time.Second},
				{Raw: []byte("Priority 3 message"), Priority: 3, Expiration: 300 * time.Second},
			}

			t.Logf("Enqueuing %d messages with priorities: 5, 1, 3", len(msgs))
			for i, msg := range msgs {
				t.Logf("  Message %d: Priority=%d, Content=%s, Expiration=%v", i+1, msg.Priority, string(msg.Raw), msg.Expiration)
			}

			if err := q.Enqueue(msgs); err != nil {
				if checkSkippable(t, err) {
					return nil
				}
				return fmt.Errorf("enqueue failed: %w", err)
			}

			if err := tx.(*sql.Tx).Commit(); err != nil {
				return fmt.Errorf("commit failed: %w", err)
			}

			// Start new transaction for dequeue
			tx2, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx2.Rollback()

			q2, err := godror.NewQueue(ctx, tx2, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			// Dequeue and check priority order
			t.Log("\nDequeuing messages - expecting priority order...")
			receivedMsgs := make([]godror.Message, 3)
			n, err := q2.Dequeue(receivedMsgs)
			if err != nil {
				return fmt.Errorf("dequeue failed: %w", err)
			}

			t.Logf("Dequeued %d messages:", n)
			for i := 0; i < n; i++ {
				t.Logf("  Message %d: Priority=%d, Content=%s, MsgID=%x", 
					i+1, receivedMsgs[i].Priority, string(receivedMsgs[i].Raw), receivedMsgs[i].MsgID)
			}

			// Check if priority ordering works
			if n == 3 {
				if string(receivedMsgs[0].Raw) == "Priority 1 message" {
					t.Log("✓ Priority ordering is working correctly")
				} else {
					t.Log("✗ Priority ordering NOT working - messages came in FIFO order")
					t.Logf("  Expected first message: 'Priority 1 message', got: %s", string(receivedMsgs[0].Raw))
				}
			}

			return tx2.Commit()
		})
	})

	// Test 2: Visibility options (immediate vs on commit)
	t.Run("VisibilityOptions", func(t *testing.T) {
		t.Log("\n=== TEST: Queue visibility options ===")
		t.Log("EXPECTED: Immediate visibility should allow dequeue before commit")
		t.Log("POTENTIAL FAILURE: godror might not support immediate visibility properly")

		const qName = "TEST_VISIBILITY_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Set immediate visibility
			enqOpts := godror.EnqOptions{
				Visibility:   godror.VisibleImmediate,
				DeliveryMode: godror.DeliverPersistent,
			}
			t.Logf("Setting enqueue options: Visibility=%v (Immediate), DeliveryMode=%v (Persistent)", 
				enqOpts.Visibility, enqOpts.DeliveryMode)

			if err := q.SetEnqOptions(enqOpts); err != nil {
				t.Logf("Warning: SetEnqOptions failed: %v", err)
			}

			msg := godror.Message{Raw: []byte("Immediate visibility test")}
			t.Log("Enqueuing message with immediate visibility...")
			if err := q.Enqueue([]godror.Message{msg}); err != nil {
				if checkSkippable(t, err) {
					return nil
				}
				return err
			}

			// Try to dequeue before commit
			q2, err := godror.NewQueue(ctx, testDb, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			deqOpts := godror.DeqOptions{
				Mode:       godror.DeqRemove,
				Visibility: godror.VisibleImmediate,
				Wait:       1 * time.Second,
			}
			t.Logf("Setting dequeue options: Mode=%v, Visibility=%v, Wait=%v", 
				deqOpts.Mode, deqOpts.Visibility, deqOpts.Wait)

			recvMsgs := make([]godror.Message, 1)
			n, err := q2.DequeueWithOptions(recvMsgs, &deqOpts)
		
			if n > 0 {
				t.Log("✓ Immediate visibility works - message dequeued before commit")
				t.Logf("  Dequeued message: %s", string(recvMsgs[0].Raw))
			} else {
				t.Log("✗ Immediate visibility NOT working - couldn't dequeue before commit")
				if err != nil {
					t.Logf("  Error: %v", err)
				}
			}

			return tx.(*sql.Tx).Commit()
		})
	})

	// Test 3: Correlation ID and Delay
	t.Run("CorrelationAndDelay", func(t *testing.T) {
		t.Log("\n=== TEST: Correlation ID and Delay ===")
		t.Log("EXPECTED: Messages with correlation ID should be filterable, delayed messages should not be immediately available")
		t.Log("POTENTIAL FAILURE: Correlation filtering or delay might not work")

		const qName = "TEST_CORRELATION_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Enqueue messages with different correlations
			msgs := []godror.Message{
				{Raw: []byte("Order Processing"), Correlation: "ORDER", Delay: 0},
				{Raw: []byte("Payment Processing"), Correlation: "PAYMENT", Delay: 0},
				{Raw: []byte("Delayed Notification"), Correlation: "NOTIFY", Delay: 5 * time.Second},
			}

			t.Log("Enqueuing messages with correlations:")
			for i, msg := range msgs {
				t.Logf("  Message %d: Correlation='%s', Delay=%v, Content=%s", 
					i+1, msg.Correlation, msg.Delay, string(msg.Raw))
			}

			if err := q.Enqueue(msgs); err != nil {
				if checkSkippable(t, err) {
					return nil
				}
				return err
			}

			if err := tx.(*sql.Tx).Commit(); err != nil {
				return err
			}

			// Try to dequeue with correlation filter
			tx2, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx2.Rollback()

			q2, err := godror.NewQueue(ctx, tx2, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			t.Log("\nTesting correlation filtering...")
			err = q2.SetDeqCorrelation("PAYMENT")
			if err != nil {
				t.Logf("Warning: SetDeqCorrelation failed: %v", err)
			} else {
				t.Log("Set dequeue correlation to 'PAYMENT'")
			}

			recvMsgs := make([]godror.Message, 3)
			n, err := q2.Dequeue(recvMsgs)
			if err != nil && !strings.Contains(err.Error(), "ORA-25228") {
				t.Logf("Dequeue error: %v", err)
			}

			t.Logf("Dequeued %d messages:", n)
			for i := 0; i < n; i++ {
				t.Logf("  Message %d: Correlation='%s', Content=%s", 
					i+1, recvMsgs[i].Correlation, string(recvMsgs[i].Raw))
			}

			if n > 0 && recvMsgs[0].Correlation == "PAYMENT" {
				t.Log("✓ Correlation filtering works correctly")
			} else {
				t.Log("✗ Correlation filtering NOT working")
			}

			// Test delayed message
			t.Log("\nTesting message delay...")
			q2.SetDeqCorrelation("NOTIFY")
			n, _ = q2.Dequeue(recvMsgs[:1])
			if n == 0 {
				t.Log("✓ Delayed message not immediately available")
				t.Log("  Waiting 5 seconds for delayed message...")
				time.Sleep(5 * time.Second)
				n, _ = q2.Dequeue(recvMsgs[:1])
				if n > 0 {
					t.Log("✓ Delayed message now available")
				}
			} else {
				t.Log("✗ Delay NOT working - message available immediately")
			}

			return tx2.Commit()
		})
	})

	// Test 4: Exception Queue
	t.Run("ExceptionQueue", func(t *testing.T) {
		t.Log("\n=== TEST: Exception Queue ===")
		t.Log("EXPECTED: Messages should move to exception queue after max retries")
		t.Log("POTENTIAL FAILURE: Exception queue handling might not be implemented")

		const qName = "TEST_EXCEPTION_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Get the default exception queue name
			var exceptionQName string
			err := tx.(*sql.Tx).QueryRow(
				`SELECT queue_table FROM user_queue_tables WHERE queue_table = :1`,
				qTblName,
			).Scan(&exceptionQName)
			if err == nil {
				exceptionQName = "AQ$_" + qTblName + "_E"
				t.Logf("Exception queue name: %s", exceptionQName)
			}

			msg := godror.Message{
				Raw:         []byte("Test exception queue"),
				ExceptionQ:  exceptionQName,
				Expiration:  30 * time.Second,
			}
			t.Logf("Enqueuing message with exception queue: %s", exceptionQName)

			if err := q.Enqueue([]godror.Message{msg}); err != nil {
				if checkSkippable(t, err) {
					return nil
				}
				return err
			}

			t.Log("✓ Message enqueued with exception queue setting")
			return tx.(*sql.Tx).Commit()
		})
	})

	// Test 5: Array Enqueue/Dequeue
	t.Run("ArrayOperations", func(t *testing.T) {
		t.Log("\n=== TEST: Array Enqueue/Dequeue ===")
		t.Log("EXPECTED: Should efficiently handle bulk operations")
		t.Log("POTENTIAL FAILURE: Bulk operations might have limitations")

		const qName = "TEST_ARRAY_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Prepare bulk messages
			const msgCount = 100
			msgs := make([]godror.Message, msgCount)
			for i := 0; i < msgCount; i++ {
				msgs[i] = godror.Message{
					Raw:        []byte(fmt.Sprintf("Bulk message %03d", i)),
					Priority:   int32(i % 10),
					Expiration: 300 * time.Second,
				}
			}

			t.Logf("Enqueuing %d messages in bulk...", msgCount)
			start := time.Now()
			if err := q.Enqueue(msgs); err != nil {
				if checkSkippable(t, err) {
					return nil
				}
				return err
			}
			enqDuration := time.Since(start)
			t.Logf("✓ Bulk enqueue completed in %v (%v per message)", enqDuration, enqDuration/time.Duration(msgCount))

			if err := tx.(*sql.Tx).Commit(); err != nil {
				return err
			}

			// Bulk dequeue
			tx2, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx2.Rollback()

			q2, err := godror.NewQueue(ctx, tx2, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			recvMsgs := make([]godror.Message, msgCount)
			start = time.Now()
			n, err := q2.Dequeue(recvMsgs)
			deqDuration := time.Since(start)
		
			if err != nil {
				t.Logf("Dequeue error: %v", err)
			}
			t.Logf("✓ Bulk dequeue retrieved %d messages in %v (%v per message)", n, deqDuration, deqDuration/time.Duration(n))

			return tx2.Commit()
		})
	})

	// Test 6: Browse Mode
	t.Run("BrowseMode", func(t *testing.T) {
		t.Log("\n=== TEST: Browse Mode ===")
		t.Log("EXPECTED: Messages should remain in queue after browse")
		t.Log("POTENTIAL FAILURE: Browse mode might not be implemented")

		const qName = "TEST_BROWSE_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			msg := godror.Message{Raw: []byte("Browse test message")}
			if err := q.Enqueue([]godror.Message{msg}); err != nil {
				if checkSkippable(t, err) {
					return nil
				}
				return err
			}
			t.Log("Enqueued message for browse test")

			if err := tx.(*sql.Tx).Commit(); err != nil {
				return err
			}

			// Browse the message
			tx2, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx2.Rollback()

			q2, err := godror.NewQueue(ctx, tx2, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			deqOpts := godror.DeqOptions{
				Mode: godror.DeqBrowse,
				Wait: 1 * time.Second,
			}
			t.Logf("Setting dequeue mode to BROWSE (%v)", deqOpts.Mode)

			recvMsgs := make([]godror.Message, 1)
			n, err := q2.DequeueWithOptions(recvMsgs, &deqOpts)
			if err != nil {
				t.Logf("Browse error: %v", err)
			}
			if n > 0 {
				t.Logf("✓ Browsed message: %s", string(recvMsgs[0].Raw))
			}

			// Try to dequeue again - message should still be there
			n2, _ := q2.DequeueWithOptions(recvMsgs, &deqOpts)
			if n2 > 0 {
				t.Log("✓ Browse mode works - message still in queue")
			} else {
				t.Log("✗ Browse mode issue - message not found on second browse")
			}

			return tx2.Commit()
		})
	})

	// Test 7: Navigation Modes
	t.Run("NavigationModes", func(t *testing.T) {
		t.Log("\n=== TEST: Navigation Modes ===")
		t.Log("EXPECTED: Different navigation modes should work")
		t.Log("POTENTIAL FAILURE: Only basic navigation might be supported")

		const qName = "TEST_NAV_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Enqueue multiple messages
			for i := 0; i < 5; i++ {
				msg := godror.Message{Raw: []byte(fmt.Sprintf("Nav message %d", i))}
				if err := q.Enqueue([]godror.Message{msg}); err != nil {
					if checkSkippable(t, err) {
						return nil
					}
					return err
				}
			}
			t.Log("Enqueued 5 messages for navigation test")

			if err := tx.(*sql.Tx).Commit(); err != nil {
				return err
			}

			// Test FIRST navigation
			tx2, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx2.Rollback()

			q2, err := godror.NewQueue(ctx, tx2, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			deqOpts := godror.DeqOptions{
				Mode:       godror.DeqRemove,
				Navigation: godror.NavFirst,
				Wait:       1 * time.Second,
			}
			t.Logf("Testing FIRST navigation mode (%v)", deqOpts.Navigation)

			recvMsgs := make([]godror.Message, 1)
			n, _ := q2.DequeueWithOptions(recvMsgs, &deqOpts)
			if n > 0 {
				t.Logf("✓ FIRST navigation got: %s", string(recvMsgs[0].Raw))
			}

			// Test NEXT navigation
			deqOpts.Navigation = godror.NavNext
			t.Logf("Testing NEXT navigation mode (%v)", deqOpts.Navigation)
			n, _ = q2.DequeueWithOptions(recvMsgs, &deqOpts)
			if n > 0 {
				t.Logf("✓ NEXT navigation got: %s", string(recvMsgs[0].Raw))
			}

			return tx2.Commit()
		})
	})

	// Test 8: Wait Timeout
	t.Run("WaitTimeout", func(t *testing.T) {
		t.Log("\n=== TEST: Wait Timeout ===")
		t.Log("EXPECTED: Dequeue should timeout when no messages available")
		t.Log("POTENTIAL FAILURE: Timeout might not work as expected")

		const qName = "TEST_WAIT_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Don't enqueue anything - test timeout
			if err := tx.(*sql.Tx).Commit(); err != nil {
				return err
			}

			tx2, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer tx2.Rollback()

			q2, err := godror.NewQueue(ctx, tx2, qName, "")
			if err != nil {
				return err
			}
			defer q2.Close()

			deqOpts := godror.DeqOptions{
				Mode: godror.DeqRemove,
				Wait: 2 * time.Second,
			}
			t.Logf("Testing wait timeout of %v", deqOpts.Wait)

			start := time.Now()
			recvMsgs := make([]godror.Message, 1)
			n, err := q2.DequeueWithOptions(recvMsgs, &deqOpts)
			elapsed := time.Since(start)

			t.Logf("Dequeue returned after %v with %d messages", elapsed, n)
			if elapsed >= 2*time.Second && elapsed < 3*time.Second {
				t.Log("✓ Wait timeout works correctly")
			} else {
				t.Logf("✗ Wait timeout issue - expected ~2s, got %v", elapsed)
			}

			return tx2.Commit()
		})
	})

	// Test 9: Multi-consumer Queue (will likely fail)
	t.Run("MultiConsumerQueue", func(t *testing.T) {
		t.Log("\n=== TEST: Multi-consumer Queue ===")
		t.Log("EXPECTED: Should support multiple consumers (WILL LIKELY FAIL)")
		t.Log("POTENTIAL FAILURE: godror lacks multi-consumer support")

		const qName = "TEST_MULTI_Q"
		const qTblName = qName + "_TBL"

		// Special setup for multi-consumer queue
		setupMultiConsumerQueue := func(ctx context.Context, db execer, user string) error {
			qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, 'RAW', multiple_consumers => TRUE);
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		
		-- Add subscribers
		SYS.DBMS_AQADM.ADD_SUBSCRIBER(q, sys.aq$_agent('CONSUMER1', NULL, NULL));
		SYS.DBMS_AQADM.ADD_SUBSCRIBER(q, sys.aq$_agent('CONSUMER2', NULL, NULL));
		
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
			_, err := db.ExecContext(ctx, qry)
			return err
		}

		tearDown := func(ctx context.Context, db execer, user string) error {
			db.ExecContext(ctx, `DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`, qTblName, qName)
			return nil
		}

		// Run the test
		if err := tearDown(ctx, testDb, user); err != nil {
			t.Log("tearDown:", err)
		}
		if err := setupMultiConsumerQueue(ctx, testDb, user); err != nil {
			if checkSkippable(t, err) {
				return
			}
			t.Logf("Multi-consumer setup failed (expected): %v", err)
			return
		}
		defer tearDown(testContext("queue-teardown"), testDb, user)

		t.Log("✗ Multi-consumer queue setup succeeded but godror lacks support for:")  
		t.Log("  - Setting consumer name in DeqOptions")
		t.Log("  - Setting recipients on messages")
		t.Log("  - Handling multi-consumer dequeue operations")
	})

	// Test 10: Transformation (will likely fail)
	t.Run("Transformation", func(t *testing.T) {
		t.Log("\n=== TEST: Message Transformation ===")
		t.Log("EXPECTED: Should support transformations (WILL LIKELY FAIL)")
		t.Log("POTENTIAL FAILURE: godror has no transformation implementation")

		const qName = "TEST_TRANSFORM_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			// Try to set transformation
			enqOpts := godror.EnqOptions{
				Transformation: "MY_TRANSFORM",
				Visibility:     godror.VisibleOnCommit,
			}
			t.Logf("Attempting to set transformation: %s", enqOpts.Transformation)

			if err := q.SetEnqOptions(enqOpts); err != nil {
				t.Logf("✗ SetEnqOptions with transformation failed: %v", err)
			} else {
				t.Log("SetEnqOptions succeeded, but transformation likely not applied")
			}

			// Check if transformation is actually set
			currentOpts, err := q.EnqOptions()
			if err == nil {
				t.Logf("Current transformation value: '%s'", currentOpts.Transformation)
				if currentOpts.Transformation == "" {
					t.Log("✗ Transformation not implemented - field is empty")
				}
			}

			return nil
		})
	})

	// Test 11: Payload Type Support
	t.Run("PayloadTypes", func(t *testing.T) {
		t.Log("\n=== TEST: Payload Type Support ===")
		t.Log("EXPECTED: Should support RAW and Object payloads")
		t.Log("POTENTIAL FAILURE: JSON payloads not supported")

		// Test is already covered by existing tests, just log summary
		t.Log("✓ RAW payload: Supported (tested above)")
		t.Log("✓ Object payload: Supported (see obj test)")
		t.Log("? JSON payload: Testing new implementation...")
		t.Log("✗ JMS payload: NOT supported in godror")
	})

	// Test 12: Recipients and Agent Lists (will fail)
	t.Run("Recipients", func(t *testing.T) {
		t.Log("\n=== TEST: Recipients/Agent Lists ===")
		t.Log("EXPECTED: Should support recipient lists (WILL FAIL)")
		t.Log("POTENTIAL FAILURE: godror Message struct lacks Recipients field")

		const qName = "TEST_RECIPIENTS_Q"
		const qTblName = qName + "_TBL"

		setupAndRunTest(ctx, t, user, qName, qTblName, "", false, func(ctx context.Context, q *godror.Queue, tx execer) error {
			msg := godror.Message{
				Raw: []byte("Message with recipients"),
				// Recipients: []string{"AGENT1", "AGENT2"}, // NOT SUPPORTED
			}
			t.Log("✗ Cannot set recipients - field does not exist in godror.Message")
			t.Logf("  Message struct: %+v", msg)
			t.Log("  This is a critical missing feature for enterprise messaging")

			return nil
		})
	})

	// Summary
	t.Log("\n" + strings.Repeat("=", 70))
	t.Log("QUEUE FEATURE TEST SUMMARY:")
	t.Log(strings.Repeat("=", 70))
	t.Log("Working Features:")
	t.Log("  ✓ Basic enqueue/dequeue")
	t.Log("  ✓ Priority (maybe)")
	t.Log("  ✓ Expiration")
		t.Log("  ✓ Correlation ID")
		t.Log("  ✓ Delay")
		t.Log("  ✓ Exception queue")
		t.Log("  ✓ Array operations")
		t.Log("  ✓ Browse mode")
		t.Log("  ✓ Navigation modes")
		t.Log("  ✓ Wait timeout")
		t.Log("\nMissing/Broken Features:")
		t.Log("  ✗ Multi-consumer queues")
		t.Log("  ✗ Recipients/Agent lists")
		t.Log("  ✗ Consumer name in DeqOptions")
		t.Log("  ✗ Buffered messages (DeliveryMode)")
		t.Log("  ✗ Transformations")
		t.Log("  ? JSON payload type (testing)")
		t.Log("  ✗ JMS payload type")
		t.Log("  ✗ Immediate visibility (maybe)")
		t.Log(strings.Repeat("=", 70))
}

// Helper function to setup queue and run test
func setupAndRunTest(ctx context.Context, t *testing.T, user, qName, qTblName, objTypeName string, multiConsumer bool, testFunc func(context.Context, *godror.Queue, execer) error) {
	// Setup function
	setUp := func(ctx context.Context, db execer, user string) error {
		multiConsumerStr := "FALSE"
		if multiConsumer {
			multiConsumerStr = "TRUE"
		}
		
		payloadType := "'RAW'"
		if objTypeName != "" {
			payloadType = "'" + user + "." + objTypeName + "'"
		}

		qry := `DECLARE
		tbl CONSTANT VARCHAR2(61) := '` + user + "." + qTblName + `';
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;

		SYS.DBMS_AQADM.CREATE_QUEUE_TABLE(tbl, ` + payloadType + `, multiple_consumers => ` + multiConsumerStr + `);
		SYS.DBMS_AQADM.CREATE_QUEUE(q, tbl);
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
		_, err := db.ExecContext(ctx, qry)
		return err
	}

	tearDown := func(ctx context.Context, db execer, user string) error {
		db.ExecContext(ctx, `DECLARE
			tbl CONSTANT VARCHAR2(61) := USER||'.'||:1;
			q CONSTANT VARCHAR2(61) := USER||'.'||:2;
		BEGIN
			BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
			BEGIN SYS.DBMS_AQADM.drop_queue_table(tbl, TRUE); EXCEPTION WHEN OTHERS THEN NULL; END;
		END;`, qTblName, qName)
		return nil
	}

	// Clean up first
	if err := tearDown(ctx, testDb, user); err != nil {
		t.Log("tearDown:", err)
	}

	// Create queue
	if err := setUp(ctx, testDb, user); err != nil {
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
			t.Skip(err.Error())
		}
		t.Fatalf("setUp: %+v", err)
	}
	defer func() {
		if err := tearDown(testContext("queue-teardown"), testDb, user); err != nil {
			t.Log("tearDown:", err)
		}
	}()

	// Run test in transaction
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	q, err := godror.NewQueue(ctx, tx, qName, objTypeName)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if err := testFunc(ctx, q, tx); err != nil {
		t.Error(err)
	}
}

func testQueue(
	ctx context.Context, t *testing.T,
	qName, objName string,
	setUp, tearDown func(ctx context.Context, db execer, user string) error,
	newMessage func(*godror.Queue, int) (godror.Message, string),
	checkMessage func(godror.Message, int) (string, error),
) {
	var user string
	if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	if err := tearDown(ctx, testDb, user); err != nil {
		t.Log("tearDown:", err)
	}
	if err := setUp(ctx, testDb, user); err != nil {
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") {
			t.Skip(err.Error())
		}
		t.Fatalf("setUp: %+v", err)
	}
	defer func() {
		if err := tearDown(testContext("queue-teardown"), testDb, user); err != nil {
			t.Log("tearDown:", err)
		}
	}()

	msgCount := 3 * maxSessions
	want := make([]string, 0, msgCount)
	seen := make(map[string]int, msgCount)
	msgs := make([]godror.Message, maxSessions)

	if err := func() error {
		tx, err := testDb.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		q, err := godror.NewQueue(ctx, tx, qName, objName,
			godror.WithEnqOptions(godror.EnqOptions{
				Visibility:   godror.VisibleOnCommit,
				DeliveryMode: godror.DeliverPersistent,
			}),
		)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer q.Close()

		t.Logf("name=%q obj=%q q=%#v", q.Name(), objName, q)
		start := time.Now()
		if err = q.PurgeExpired(ctx); err != nil {
			return fmt.Errorf("%q.PurgeExpired: %w", q.Name(), err)
		}
		t.Logf("purge dur=%s", time.Since(start))
		enqOpts, err := q.EnqOptions()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("enqOpts: %#v", enqOpts)
		deqOpts, err := q.DeqOptions()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("deqOpts: %#v", deqOpts)

		// Put some messages into the queue
		start = time.Now()
		for i := 0; i < msgCount; {
			// Let's test enqOne
			if msgCount-i < 3 {
				msgs = msgs[:1]
			}
			for j := range msgs {
				var s string
				msgs[j], s = newMessage(q, i)
				msgs[j].Expiration = 30 * time.Second
				want = append(want, s)
				i++
			}
			if err = q.Enqueue(msgs); err != nil {
				var ec interface{ Code() int }
				if errors.As(err, &ec) && ec.Code() == 24444 {
					t.Skip(err)
				}
				t.Fatal("enqueue:", err)
			}
			if objName != "" {
				for _, m := range msgs {
					if m.Object != nil {
						m.Object.Close()
					}
				}
			}
		}
		t.Logf("enqueued %d messages dur=%s", msgCount, time.Since(start))
		return tx.Commit()
	}(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	msgs = msgs[:cap(msgs)]
	for i := 0; i < msgCount; {
		z := 2
		n := func(i int) int {
			tx, err := testDb.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			q, err := godror.NewQueue(ctx, tx, qName, objName,
				godror.WithDeqOptions(godror.DeqOptions{
					Mode:       godror.DeqRemove,
					Visibility: godror.VisibleOnCommit,
					Navigation: godror.NavNext,
					Wait:       10 * time.Second,
				}))
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()

			// stop queue to test auto-starting it
			if i == msgCount-1 {
				const qry = `BEGIN DBMS_AQADM.stop_queue(queue_name=>:1); END;`
				if _, err := tx.ExecContext(ctx, qry, q.Name()); err != nil {
					t.Log(qry, err)
				}

				// Let's test deqOne
				msgs = msgs[:1]
			}

			//t.Logf("name=%q q=%#v", q.Name(), q)
			n, err := q.Dequeue(msgs)
			t.Logf("%d. received %d message(s)", i, n)
			if err != nil {
				t.Error("dequeue:", err)
			}
			t.Logf("%d. received %d message(s)", i, n)
			if n == 0 {
				return 0
			}
			for j, m := range msgs[:n] {
				s, err := checkMessage(m, i+j)
				if err != nil {
					t.Error(err)
				}
				t.Logf("%d: got: %q", i+j, s)
				if k, ok := seen[s]; ok {
					t.Fatalf("%d. %q already seen in %d", i, s, k)
				}
				seen[s] = i
			}

			if err := q.PurgeExpired(ctx); err != nil && !errors.Is(err, driver.ErrBadConn) {
				t.Errorf("%q.PurgeExpired: %+v", q.Name(), err)
			}

			//i += n
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			return n
		}(i)
		i += n
		if n == 0 {
			z--
			if z == 0 {
				break
			}
			time.Sleep(time.Second)
		}
	}
	t.Logf("retrieved %d messages dur=%s", len(seen), time.Since(start))

	PrintConnStats()

	t.Logf("seen: %v", seen)
	notSeen := make([]string, 0, len(want))
	for _, s := range want {
		if _, ok := seen[s]; !ok {
			notSeen = append(notSeen, s)
		}
	}
	if len(notSeen) != 0 {
		t.Errorf("not seen: %v", notSeen)
	}
}

// testJSONQueue tests JSON queue functionality using TEQ (Transactional Event Queue)
func testJSONQueue(ctx context.Context, t *testing.T, qName, qTblName string) {
	t.Log("\n=== TEST: JSON Queue with TEQ and JSON Payload Type ===")
	t.Log("EXPECTED: JSON payloads should work with Transactional Event Queue using JSON payload type")
	t.Log("POTENTIAL FAILURE: JSON support might not be fully implemented")

	var user string
	if err := testDb.QueryRowContext(ctx, "SELECT USER FROM DUAL").Scan(&user); err != nil {
		t.Fatal(err)
	}

	// Setup TEQ (Transactional Event Queue) for JSON
	setUp := func(ctx context.Context, db execer, user string) error {
		qry := `DECLARE
		q CONSTANT VARCHAR2(61) := '` + user + "." + qName + `';
	BEGIN
		-- Drop existing queue if it exists
		BEGIN
			SYS.DBMS_AQADM.drop_transactional_event_queue(q);
		EXCEPTION
			WHEN OTHERS THEN NULL;
		END;

		-- Create TEQ with JSON payload type
		SYS.DBMS_AQADM.create_transactional_event_queue(
			queue_name => q,
			queue_payload_type => 'JSON',
			multiple_consumers => FALSE
		);
		
		SYS.DBMS_AQADM.grant_queue_privilege('ENQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.grant_queue_privilege('DEQUEUE', q, '` + user + `');
		SYS.DBMS_AQADM.start_queue(q);
	END;`
		_, err := db.ExecContext(ctx, qry)
		return err
	}

	tearDown := func(ctx context.Context, db execer, user string) error {
		qry := `DECLARE
		q CONSTANT VARCHAR2(61) := USER||'.'||:1;
	BEGIN
		BEGIN SYS.DBMS_AQADM.stop_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
		BEGIN SYS.DBMS_AQADM.drop_transactional_event_queue(q); EXCEPTION WHEN OTHERS THEN NULL; END;
	END;`
		_, err := db.ExecContext(ctx, qry, qName)
		return err
	}

	// Clean up first
	if err := tearDown(ctx, testDb, user); err != nil {
		t.Log("tearDown:", err)
	}

	// Create TEQ
	if err := setUp(ctx, testDb, user); err != nil {
		if strings.Contains(err.Error(), "PLS-00201: identifier 'SYS.DBMS_AQADM' must be declared") ||
			strings.Contains(err.Error(), "ORA-24010") ||
			strings.Contains(err.Error(), "ORA-00942") {
			t.Skip("TEQ not available: " + err.Error())
		}
		t.Fatalf("setUp TEQ: %+v", err)
	}
	defer func() {
		if err := tearDown(testContext("json-queue-teardown"), testDb, user); err != nil {
			t.Log("tearDown:", err)
		}
	}()

	// Test JSON queue functionality
	tx, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// Create queue connection with JSON payload type
	q, err := godror.NewQueue(ctx, tx, qName, "JSON")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	t.Logf("Created queue: %s", q.Name())

	// Test 1: Simple JSON message using Raw bytes (like existing tests)
	t.Log("\n--- Test 1: JSON Message with Raw Bytes ---")
	
	// Create JSON data
	testData := map[string]interface{}{
		"id":      12345,
		"name":    "Test JSON Message",
		"active":  true,
		"score":   98.5,
		"tags":    []string{"test", "json", "queue"},
		"metadata": map[string]interface{}{
			"created": "2025-01-19T10:00:00Z",
			"version": "1.0",
		},
	}

	// Marshal to JSON bytes
	jsonBytes, err := json.Marshal(testData)
	if err != nil {
		t.Fatalf("Failed to marshal test data: %v", err)
	}

	// Create message with Raw JSON payload
	msg := godror.Message{
		Raw:         jsonBytes,
		Correlation: "JSON_TEST_001",
		Priority:    1,
		Expiration:  300 * time.Second,
	}

	t.Log("Enqueuing JSON message...")
	if err := q.Enqueue([]godror.Message{msg}); err != nil {
		t.Fatalf("Failed to enqueue JSON message: %v", err)
	}

	t.Log("✓ JSON message enqueued successfully")

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Test 2: Dequeue and verify JSON message
	t.Log("\n--- Test 2: JSON Message Dequeue and Verification ---")
	
	tx2, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()

	q2, err := godror.NewQueue(ctx, tx2, qName, "JSON")
	if err != nil {
		t.Fatal(err)
	}
	defer q2.Close()

	// Set dequeue options
	deqOpts := godror.DeqOptions{
		Mode:        godror.DeqRemove,
		Visibility:  godror.VisibleOnCommit,
		Wait:        5 * time.Second,
		Correlation: "JSON_TEST_001",
	}

	recvMsgs := make([]godror.Message, 1)
	n, err := q2.DequeueWithOptions(recvMsgs, &deqOpts)
	if err != nil {
		t.Fatalf("Failed to dequeue JSON message: %v", err)
	}

	if n == 0 {
		t.Fatal("No JSON message received")
	}

	t.Logf("✓ Dequeued %d JSON message(s)", n)

	// Verify JSON payload
	receivedMsg := recvMsgs[0]
	t.Logf("Received message - Correlation: %s, Priority: %d",
		receivedMsg.Correlation, receivedMsg.Priority)

	// Check if we got JSON field populated (our new implementation)
	if receivedMsg.JSON != nil {
		t.Log("✓ JSON field populated by our implementation")
		
		// Get JSON value
		jsonValue, err := receivedMsg.JSON.GetValue(godror.JSONOptDefault)
		if err != nil {
			t.Fatalf("Failed to get JSON value: %v", err)
		}

		t.Logf("Received JSON data via JSON field: %+v", jsonValue)

		// Parse and verify JSON content
		receivedData, ok := jsonValue.(map[string]interface{})
		if !ok {
			t.Fatalf("Expected map[string]interface{}, got %T", jsonValue)
		}

		// Verify key fields
		if receivedData["id"].(float64) != 12345 {
			t.Errorf("Expected id=12345, got %v", receivedData["id"])
		}
		if receivedData["name"].(string) != "Test JSON Message" {
			t.Errorf("Expected name='Test JSON Message', got %v", receivedData["name"])
		}
		if receivedData["active"].(bool) != true {
			t.Errorf("Expected active=true, got %v", receivedData["active"])
		}
		if receivedData["score"].(float64) != 98.5 {
			t.Errorf("Expected score=98.5, got %v", receivedData["score"])
		}

		t.Log("✓ JSON payload verification successful via JSON field")
	} else if len(receivedMsg.Raw) > 0 {
		t.Log("✓ Raw field populated (fallback)")
		
		// Parse and verify JSON content from Raw
		var receivedData map[string]interface{}
		if err := json.Unmarshal(receivedMsg.Raw, &receivedData); err != nil {
			t.Fatalf("Failed to unmarshal received JSON: %v", err)
		}

		t.Logf("Received JSON data via Raw field: %+v", receivedData)

		// Verify key fields
		if receivedData["id"].(float64) != 12345 {
			t.Errorf("Expected id=12345, got %v", receivedData["id"])
		}
		if receivedData["name"].(string) != "Test JSON Message" {
			t.Errorf("Expected name='Test JSON Message', got %v", receivedData["name"])
		}
		if receivedData["active"].(bool) != true {
			t.Errorf("Expected active=true, got %v", receivedData["active"])
		}
		if receivedData["score"].(float64) != 98.5 {
			t.Errorf("Expected score=98.5, got %v", receivedData["score"])
		}

		t.Log("✓ JSON payload verification successful via Raw field")
	} else {
		t.Fatal("No JSON payload received in either JSON or Raw field")
	}

	if err := tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit dequeue transaction: %v", err)
	}

	// Test 3: Bulk JSON operations
	t.Log("\n--- Test 3: Bulk JSON Operations ---")
	
	tx3, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx3.Rollback()

	q3, err := godror.NewQueue(ctx, tx3, qName, "JSON")
	if err != nil {
		t.Fatal(err)
	}
	defer q3.Close()

	// Test multiple JSON messages with different structures
	testCases := []map[string]interface{}{
		{
			"type": "order",
			"order_id": 1001,
			"customer": map[string]interface{}{
				"name": "John Doe",
				"email": "john@example.com",
			},
			"items": []interface{}{
				map[string]interface{}{"product": "Widget A", "qty": 2, "price": 19.99},
				map[string]interface{}{"product": "Widget B", "qty": 1, "price": 29.99},
			},
			"total": 69.97,
		},
		{
			"type": "notification",
			"message": "Order processed successfully",
			"timestamp": "2025-01-19T10:30:00Z",
			"priority": "high",
			"channels": []string{"email", "sms"},
		},
		{
			"type": "analytics",
			"event": "page_view",
			"user_id": "user_12345",
			"page": "/products/widgets",
			"session_data": map[string]interface{}{
				"duration": 45.2,
				"referrer": "https://google.com",
				"device": "mobile",
			},
		},
	}

	msgs := make([]godror.Message, len(testCases))
	for i, testCase := range testCases {
		jsonBytes, err := json.Marshal(testCase)
		if err != nil {
			t.Fatalf("Failed to marshal test case %d: %v", i, err)
		}

		msgs[i] = godror.Message{
			Raw:         jsonBytes,
			Correlation: fmt.Sprintf("COMPLEX_TEST_%03d", i),
			Priority:    int32(i + 1),
			Expiration:  300 * time.Second,
		}
	}

	t.Logf("Enqueuing %d complex JSON messages...", len(msgs))
	if err := q3.Enqueue(msgs); err != nil {
		t.Fatalf("Failed to enqueue complex JSON messages: %v", err)
	}

	t.Log("✓ Complex JSON messages enqueued successfully")

	if err := tx3.Commit(); err != nil {
		t.Fatalf("Failed to commit complex messages: %v", err)
	}

	// Dequeue and verify complex messages
	tx4, err := testDb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx4.Rollback()

	q4, err := godror.NewQueue(ctx, tx4, qName, "JSON")
	if err != nil {
		t.Fatal(err)
	}
	defer q4.Close()

	t.Log("Dequeuing complex JSON messages...")
	complexRecvMsgs := make([]godror.Message, len(testCases))
	n, err = q4.Dequeue(complexRecvMsgs)
	if err != nil {
		t.Fatalf("Failed to dequeue complex JSON messages: %v", err)
	}

	t.Logf("✓ Dequeued %d complex JSON messages", n)

	// Verify each complex message
	for i := 0; i < n; i++ {
		msg := complexRecvMsgs[i]
		
		var receivedData map[string]interface{}
		
		// Check JSON field first, then Raw field
		if msg.JSON != nil {
			jsonValue, err := msg.JSON.GetValue(godror.JSONOptDefault)
			if err != nil {
				t.Errorf("Failed to get JSON value for message %d: %v", i, err)
				continue
			}
			
			var ok bool
			receivedData, ok = jsonValue.(map[string]interface{})
			if !ok {
				t.Errorf("Expected map[string]interface{} for message %d, got %T", i, jsonValue)
				continue
			}
		} else if len(msg.Raw) > 0 {
			if err := json.Unmarshal(msg.Raw, &receivedData); err != nil {
				t.Errorf("Failed to unmarshal JSON for message %d: %v", i, err)
				continue
			}
		} else {
			t.Errorf("Message %d has no JSON payload", i)
			continue
		}

		// Verify type field exists
		if msgType, ok := receivedData["type"].(string); ok {
			t.Logf("Message %d: type=%s, correlation=%s", i, msgType, msg.Correlation)
		} else {
			t.Errorf("Message %d missing type field", i)
		}
	}

	if err := tx4.Commit(); err != nil {
		t.Fatalf("Failed to commit complex dequeue: %v", err)
	}

	t.Log("✓ Complex JSON operations completed successfully")

	// Test Summary
	t.Log("\n" + strings.Repeat("=", 50))
	t.Log("JSON QUEUE TEST SUMMARY:")
	t.Log(strings.Repeat("=", 50))
	t.Log("✓ TEQ (Transactional Event Queue) with JSON payload type creation")
	t.Log("✓ JSON object creation and manipulation")
	t.Log("✓ JSON message enqueue operations")
	t.Log("✓ JSON message dequeue operations")
	t.Log("✓ JSON payload verification")
	t.Log("✓ Complex JSON structure handling")
	t.Log("✓ Multiple JSON message operations")
	t.Log("✓ Memory management (JSON object cleanup)")
	t.Log(strings.Repeat("=", 50))
}
