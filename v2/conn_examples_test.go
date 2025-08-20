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

package yottadb_test

import (
	"fmt"

	yottadbV1 "lang.yottadb.com/go/yottadb" // for TransactionTokenSet() example
	yottadb "lang.yottadb.com/go/yottadb/v2"
	yottadbV2 "lang.yottadb.com/go/yottadb/v2" // for TransactionTokenSet() example
)

// ---- Examples

func ExampleConn_TransactionTokenSet() {
	defer yottadbV2.Shutdown(yottadbV2.MustInit())
	yottadbV1.ForceInit() // Tell v1 that v2 has done the initialization

	err := yottadbV1.TpE(yottadbV1.NOTTP, nil, func(tptoken uint64, errstr *yottadbV1.BufferT) int32 {
		err := yottadbV1.SetValE(tptoken, nil, "Fred", "^person", []string{})
		if err != nil {
			panic(err)
		}

		// Run a YDBGo v2 function Node.Dump()
		conn := yottadbV2.NewConn()
		conn.TransactionTokenSet(tptoken) // without this the v2 function will hang
		person := conn.Node("^person")
		fmt.Print(person.Dump())
		return yottadb.YDB_OK
	}, "BATCH", []string{})
	if err != nil {
		panic(err)
	}
	// Output:
	// ^person="Fred"
}

func ExampleConn_TransactionToken() {
	defer yottadbV2.Shutdown(yottadbV2.MustInit())
	yottadbV1.ForceInit() // Tell v1 that v2 has done the initialization

	conn := yottadbV2.NewConn()
	conn.TransactionFast([]string{}, func() int {
		person := conn.Node("^person")
		person.Set("Sally")

		// Run a YDBGo v1 function
		tptoken := conn.TransactionToken() // without this the v1 function will hang
		val, err := yottadbV1.ValE(tptoken, nil, "^person", []string{})
		if err != nil {
			panic(err)
		}
		fmt.Print(val)
		return yottadbV2.YDB_OK
	})
	// Output:
	// Sally
}
