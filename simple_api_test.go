//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.	//
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
	"lang.yottadb.com/go/yottadb"
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"testing"
)

func TestLockST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var timeout uint64
	var errors int

	dbkey.Alloc(VarSiz, AryDim, SubSiz) // Reallocate the key
	err = dbkey.Varnm.SetValStrLit(tptoken, "^Variable1A")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, 0, "Index0")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, 1)
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, timeout, &dbkey) // 10 second timeout
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^Variable1A(\"Index0\")"), &errors, true, t)
	err = yottadb.LockST(tptoken, 0) // Release all locks
	Assertnoerr(err, t)
}