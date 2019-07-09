//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2019 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb_test

import (
	"fmt"
	"lang.yottadb.com/go/yottadb"
)

// Example demonstrating how to do transactions in Go
func Example_transactionProcessing() {
	// Allocate a key to set our value equal too
	var buffertary1 yottadb.BufferTArray
	var errstr yottadb.BufferT
	var tptoken uint64
	var err error

	// The tptoken argument to many functions is either a value passed into the
	//  callback routine for TP, or yottadb.NOTTP if not in a transaction
	tptoken = yottadb.NOTTP

	// Restore all YDB local buffers on a TP-restart
	buffertary1.Alloc(1, 32)
	errstr.Alloc(1024)
	err = buffertary1.SetValStr(tptoken, &errstr, 0, "*")
	if err != nil {
		panic(err)
	}
	err = buffertary1.TpST(tptoken, &errstr, func(tptoken uint64, errstr *yottadb.BufferT) int32 {
		fmt.Printf("Hello from MyGoCallBack!\n")
		return 0
	}, "TEST")
	if err != nil {
		panic(err)
	}

	/* Output: Hello from MyGoCallBack!
	 */
}
