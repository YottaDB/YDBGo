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
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

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
