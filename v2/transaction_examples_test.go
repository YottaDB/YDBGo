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

package yottadb_test

import (
	"fmt"

	yottadbV1 "lang.yottadb.com/go/yottadb"    // for TransactionTokenSet() example
	yottadbV2 "lang.yottadb.com/go/yottadb/v2" // for TransactionTokenSet() example
)

// ---- Examples

func ExampleConn_TransactionTokenSet() {
	defer yottadbV2.Shutdown(yottadbV2.MustInit())
	yottadbV1.ForceInit() // Tell v1 that v2 has done the initialization

	conn := yottadbV2.NewConn()
	err := yottadbV1.TpE(yottadbV1.NOTTP, nil, func(tptoken uint64, errstr *yottadbV1.BufferT) int32 {
		err := yottadbV1.SetValE(tptoken, nil, "Fred", "^person", nil)
		if err != nil {
			panic(err)
		}

		// Run a YDBGo v2 function Node.Dump()
		conn.TransactionTokenSet(tptoken) // without this the v2 function will hang
		person := conn.Node("^person")
		fmt.Print(person.Dump())
		return yottadbV2.YDB_OK
	}, "BATCH", nil)
	if err != nil {
		panic(err)
	}
	// Restore transaction token in conn to its initial value
	// without this, subsequent use of conn will hang
	conn.TransactionTokenSet(yottadbV1.NOTTP)

	// Output:
	// ^person="Fred"
}

func ExampleConn_TransactionToken() {
	defer yottadbV2.Shutdown(yottadbV2.MustInit())
	yottadbV1.ForceInit() // Tell v1 that v2 has done the initialization

	conn := yottadbV2.NewConn()
	conn.TransactionFast(nil, func() {
		person := conn.Node("^person")
		person.Set("Sally")

		// Run a YDBGo v1 function
		tptoken := conn.TransactionToken() // without this the v1 function will hang
		val, err := yottadbV1.ValE(tptoken, nil, "^person", nil)
		if err != nil {
			panic(err)
		}
		fmt.Print(val)
	})
	// Output:
	// Sally
}

func ExampleConn_Restart() {
	conn := yottadbV2.NewConn()
	n := conn.Node("^activity")
	n.Set(0)
	// M locals to demonstrate restoration of M 'local' on restart or not
	incr1 := conn.Node("incr1")
	incr2 := conn.Node("incr2")
	incr1.Set(0)
	incr2.Set(0)
	restarts := 0 // Go local
	run := func() {
		n.Incr(1)
		incr1.Incr(1)
		incr2.Incr(1)
		if restarts < 2 {
			restarts++
			conn.Restart()
		}
		if restarts > 9 {
			// Error condition
			conn.Rollback()
		}
	}
	normal := conn.TransactionFast([]string{"incr1"}, run)
	fmt.Printf("Database updated %d times; transaction restarted %d times then normal exit was: %v\n", n.GetInt(), restarts, normal)
	fmt.Printf("M local 'incr1' was restored so became %d; 'incr2' became %d\n", incr1.GetInt(), incr2.GetInt())

	// Run transaction again but give it an error condition so that it rolls back
	restarts = 10
	rollback := !conn.TransactionFast(nil, run)
	fmt.Printf("Transaction rollback %v; restores database value n of 2 to %d\n", rollback, n.GetInt())
	// Output:
	// Database updated 1 times; transaction restarted 2 times then normal exit was: true
	// M local 'incr1' was restored so became 1; 'incr2' became 3
	// Transaction rollback true; restores database value n of 2 to 1
}
