//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.
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
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"
	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// ---- Tests

// Test that $ZMAXTPTIME works
func TestTimeoutAction(t *testing.T) {
	conn := NewConn()
	// 1 is the shortest possible timeout. We could speed up testing if it allowed floating point, but it rounds to zero.
	conn.Node("$ZMAXTPTIME").Set(1)
	defer func() { conn.Node("$ZMAXTPTIME").Set(0) }() // restore $ZMAXTPTIME global at end of test

	// Check that invalid action value panics
	assert.Panics(t, func() { conn.TimeoutAction(12345) })

	index := conn.Node("^lastindex")
	conn.TimeoutAction(TransactionRollback)

	// Check that a short transaction doesn't trigger the 1s timeout
	success := conn.TransactionFast([]string{}, func() {
		time.Sleep(time.Millisecond * 1)
	})
	assert.Equal(t, true, success)

	// Function to create a transaction timeout.
	timeoutTransaction := func(fakeTimeout bool) bool {
		return conn.TransactionFast([]string{}, func() {
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
	assert.PanicsWithError(t, "%YDB-E-TPTIMEOUT, Transaction timeout", func() { timeoutTransaction(true) })
	assert.Equal(t, 0, index.GetInt()) // timeout also does a rollback, so should be 0

	// Test that a panic using a non-YottaDB error also works inside a transaction
	assert.PanicsWithError(t, "testPanic", func() { conn.TransactionFast([]string{}, func() { panic(errors.New("testPanic")) }) })
	// Test that a panic using a string value also works inside a transaction
	assert.PanicsWithValue(t, "testString", func() { conn.TransactionFast([]string{}, func() { panic("testString") }) })

	// Test that a transaction with any other kind of YottaDB error creates a panic as it should.
	// In this case we force a TPLOCK error by trying to unlock a lock that was locked outside the transaction.
	n := conn.Node("testlock")
	n.Lock()
	// skip the unlock line below which causes BADLOCKNEXT error (it seems that the nexted unlock worked even though it gave an error)
	// defer n.Unlock()
	assert.PanicsWithError(t, "%YDB-E-TPLOCK, Cannot release LOCK(s) held prior to current TSTART", func() {
		conn.TransactionFast([]string{}, func() {
			n.Unlock()
		})
	})
}

func TestTransactionToken(t *testing.T) {
	// Make sure that a cloned conn points to the same tptoken as its parent
	conn1 := NewConn()
	original := conn1.TransactionToken()
	conn1.TransactionTokenSet(original + 1)
	assert.Equal(t, original+1, conn1.tptoken.Load())
}

// TestMRestart checks that a TRESTART issued by calling M works fine.
// This confirms implementation of YDB#619: https://gitlab.com/YottaDB/DB/YDB/-/issues/619
// Do not test rollback via M because it does not work by design. See https://gitlab.com/YottaDB/DB/YDB/-/issues/1166#note_2704105725
func TestMRestart(t *testing.T) {
	conn := NewConn()
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
	success := conn.TransactionFast([]string{}, run)
	assert.Equal(t, 1, n.GetInt())
	assert.Equal(t, 2, restarts)
	assert.Equal(t, true, success)
}

// TestCloneConn checks that a cloned connection behaves correctly by following its parent's transactions
func TestCloneConn(t *testing.T) {
	// Make sure that a cloned conn points to the same tptoken as its parent
	conn1 := NewConn()
	original := conn1.TransactionToken()
	conn2 := conn1.CloneConn()
	conn2.TransactionTokenSet(original + 1)
	assert.Equal(t, original+1, conn1.TransactionToken())

	// Now create actual goroutines inside a transaction both using a cloned conn to make sure that works
	conn := NewConn()
	conn.TransactionFast([]string{}, func() {
		n := conn.Node("count")
		done := make(chan struct{})
		subfunc := func() {
			subconn := conn.CloneConn()
			// Create an error in subconn to make sure it doesn't clobber conn's error
			_, err := subconn.Zwr2Str(`"X"_$C(1234`)
			assert.NotNil(t, err)
			n := subconn.Node("count")
			n.Incr(1)
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
