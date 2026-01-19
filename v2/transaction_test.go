//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025-2026 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"errors"
	"flag"
	"log"
	"sync"
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"
	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// ---- Tests

// Run f() and return any error or nil
func captureError(f func()) (err error) {
	defer func() {
		recovered := recover()
		err, _ = recovered.(error)
	}()
	f()
	return err
}

// Test that $ZMAXTPTIME works
func TestTimeoutAction(t *testing.T) {
	conn := SetupTest(t)
	// 1 is the shortest possible timeout. We could speed up testing if it allowed floating point, but it rounds to zero.
	conn.Node("$ZMAXTPTIME").Set(1)
	defer func() { conn.Node("$ZMAXTPTIME").Set(0) }() // restore $ZMAXTPTIME global at end of test

	// Check that invalid action value panics
	assert.Panics(t, func() { conn.TimeoutAction(12345) })

	index := conn.Node("^lastindex")
	conn.TimeoutAction(TransactionRollback)

	// Check that a short transaction doesn't trigger the 1s timeout
	success := conn.TransactionFast(nil, func() {
		time.Sleep(time.Millisecond * 1)
	})
	assert.Equal(t, true, success)

	// Function to create a transaction timeout.
	index.Kill()
	timeoutTransaction := func(fakeTimeout bool) bool {
		return conn.TransactionFast(nil, func() {
			for i := range 2000 {
				// Note these sets won't make it to the DB as the transaction is never committed
				index.Set(i)
				time.Sleep(time.Millisecond * 1)
				// Use (fast) fake timeout if flag specified
				if fakeTimeout && i >= 2 {
					// if fakeTimeout flag is true, fake a timeout for faster testing because YottaDB min timeout is 1s
					panic(newError(ydberr.TPTIMEOUT, "%YDB-E-TPTIMEOUT, Transaction timeout"))
				}
			}
		})
	}

	// Check that a 2s transaction does trigger the 1s timeout
	success = timeoutTransaction(testing.Short()) // use (fast) fake timeout if -short flag supplied to `go test`
	assert.Equal(t, false, success)
	assert.Equal(t, 0, index.GetInt()) // rollback should have returned it to zero

	// Check that TimeoutAction(TransactionCommit) causes it to commit on timeout
	conn.TimeoutAction(TransactionCommit)
	success = timeoutTransaction(true) // true = use fast fakeTimeout -- always do so here because the YottaDB timeout functionality has already been tested above
	assert.Equal(t, 2, index.GetInt()) // timeout also does a rollback

	// Check that TimeoutAction(TransactionTimeout) causes a panic
	conn.TimeoutAction(TransactionTimeout)
	index.Set(0)
	// true = use fast fakeTimeout below -- always do so here because the YottaDB timeout functionality has already been tested above
	err := captureError(func() { timeoutTransaction(true) })
	assert.Equal(t, ydberr.TPTIMEOUT, err.(*Error).Code)
	assert.Equal(t, 0, index.GetInt()) // timeout also does a rollback, so should be 0
}

func TestTransactionToken(t *testing.T) {
	conn1 := SetupTest(t)
	original := conn1.TransactionToken()
	conn1.TransactionTokenSet(original + 1)
	assert.Equal(t, original+1, conn1.TransactionToken())
}

// TestMRestart checks that a TRESTART issued by calling M works fine.
// This confirms implementation of YDB#619: https://gitlab.com/YottaDB/DB/YDB/-/issues/619
// Do not test rollback via M because it does not work by design. See https://gitlab.com/YottaDB/DB/YDB/-/issues/1166#note_2704105725
func TestMRestart(t *testing.T) {
	conn := SetupTest(t)
	m := conn.MustImport(`trestart: trestart^trestart()`)

	n := conn.Node("^activity")
	n.Set(0)
	restarts := 0
	run := func() {
		n.Incr(1)
		if restarts < 2 {
			restarts++
			m.Call("trestart")
		}
	}
	success := conn.TransactionFast(nil, run)
	assert.Equal(t, 1, n.GetInt())
	assert.Equal(t, 2, restarts)
	assert.Equal(t, true, success)
}

// TestCloneConn checks that a cloned connection behaves correctly by following its parent's transactions
func TestCloneConn(t *testing.T) {
	conn := SetupTest(t)
	// Create goroutines inside a transaction both using a cloned conn to make sure they don't clobber each other's errstr fields
	conn.TransactionFast(nil, func() {
		n := conn.Node("count")
		done := make(chan struct{}, 2)
		subfunc := func() {
			subconn := conn.CloneConn()
			// Make sure that a cloned conn has the same tptoken as its parent
			assert.Equal(t, conn.TransactionToken(), subconn.TransactionToken())
			// Create an error in subconn to make sure it doesn't clobber conn's error
			_, err := subconn.Zwr2Str(`"X"_$C(1234`)
			assert.NotNil(t, err)
			n := subconn.Node("count")
			n.Incr(1)

			// Now that we're done using subconn (the goroutine is about to end),
			// we can mess it up to test its independence from conn.
			subconn.TransactionTokenSet(conn.TransactionToken() + 1)
			assert.NotEqual(t, conn.TransactionToken(), subconn.TransactionToken())
			done <- struct{}{} // say I'm done
		}
		// Create two goroutines
		go subfunc()
		go subfunc()
		<-done // wait for two goroutines to finish
		<-done
		// Make sure conn's last non-error was not clobbered by error strings in the subconns
		assert.Equal(t, "", conn.getErrorString())
		assert.Equal(t, 2, n.GetInt())
	})
}

// TestTransactionGoroutines makes two concurrent goroutines each start a transaction using a cloned Conn,
// and ensures that each call to Transaction() generates a different tptoken for its transaction.
// This tests a bug introduced in #df86e2b9 where cloned conns incorrectly shared tptokens.
// Transactions created in two concurrent goroutines should each use a different tptoken provided by ydb_tp_st().
func TestTransactionGoroutines(t *testing.T) {
	conn := SetupTest(t)
	testTransactionGoroutines(conn)
	// Test again nested inside a transaction
	conn.TransactionFast([]string{}, func() {
		testTransactionGoroutines(conn)
	})
}

// Perform the substance of TestTransactionGoroutines
func testTransactionGoroutines(conn *Conn) {
	bump := make(chan struct{}) // call for action in the other thread
	var running sync.WaitGroup
	running.Add(2) // number of running threads to eventually stop
	go func() {
		defer ShutdownOnPanic()
		defer running.Done()
		conn := conn.CloneConn()
		conn.TransactionFast([]string{}, func() {
			// This bump starts the other goroutine's TransactionFast() which fetches its conn.tptoken
			// if the other goroutine's tptoken is linked to this one as they were in invalid commit #df86e2b9
			// then the other goroutine incorrectly interrupts this one while its engine lock is engaged
			// and causes ydberr.SIMPLEAPINEST or ydberr.INVTPTRANS errors or hangs
			// (or causes even earlier assert errors in the debug build of YottaDB).
			// At least, I think that's the mechanism that was causing the errors.
			bump <- struct{}{}
			node := conn.Node("^abc") // dummy set inside transaction
			node.Set("MySecretValue")
		})
	}()
	go func() {
		defer ShutdownOnPanic()
		defer running.Done()
		conn := conn.CloneConn()
		<-bump // wait for other goroutine to start its transaction
		conn.TransactionFast([]string{}, func() {})
	}()
	running.Wait()
	log.Println("Done")
}

// Set up custom flag to allow user to specify deadlock test
var testDeadlock bool

func init() {
	flag.BoolVar(&testDeadlock, "deadlock", false, "test that a transaction using the wrong tptimeout causes a deadlock")
}

// TestDeadlock checks that a panic instead of an error occurs if an invalid Conn is used inside a transaction
// Run this test with: go test -timeout 2s -run Deadlock -deadlock=true >/dev/null || echo Successfully deadlocked
func TestDeadlock(t *testing.T) {
	if !testDeadlock {
		return
	}

	conn := SetupTest(t)
	n := conn.Node("^testdeadlock")
	tptoken := conn.TransactionToken()
	success := conn.TransactionFast(nil, func() {
		conn.TransactionTokenSet(tptoken) // use the wrong (outer) tptoken
		n.Get()
	})
	assert.Equal(t, false, success)
}

// TestTransaction tests everything else not covered by the tests above and in transaction_examples_test.go
func TestTransaction(t *testing.T) {
	conn := SetupTest(t)

	// Test that a panic using a non-YottaDB error also works inside a transaction
	assert.PanicsWithError(t, "testPanic", func() { conn.TransactionFast(nil, func() { panic(errors.New("testPanic")) }) })
	// Test that a panic using a string value also works inside a transaction
	assert.PanicsWithValue(t, "testString", func() { conn.TransactionFast(nil, func() { panic("testString") }) })

	// Test that a transaction with any other kind of YottaDB error creates a panic as it should.
	// In this case we force a TPLOCK error by trying to unlock a lock that was locked outside the transaction.
	n := conn.Node("testlock")
	n.Lock()
	// skip the unlock line below which causes BADLOCKNEXT error (it seems that the nexted unlock worked even though it gave an error)
	// defer n.Unlock()
	err := captureError(func() {
		conn.TransactionFast(nil, func() {
			n.Unlock()
		})
	})
	assert.Equal(t, ydberr.TPLOCK, err.(*Error).Code)
}
