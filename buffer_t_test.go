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
	"fmt"
	"go.yottadb.com/yottadb"
	. "go.yottadb.com/yottadb/internal/test_helpers"
	"testing"
)

func TestStr2ZwrSt(t *testing.T) {
	var ovalue, cvalue yottadb.BufferT
	var outstrp *string
	var tptoken uint64 = yottadb.NOTTP
	var err error

	ovalue.Alloc(64)
	cvalue.Alloc(128)

	origstr := "This\tis\ta\ttest\tstring"
	if DebugFlag {
		fmt.Println("Original string unmodified:  ", origstr)
	}
	err = ovalue.SetValStr(tptoken, &origstr)
	Assertnoerr(err, t)
	err = ovalue.Str2ZwrST(tptoken, &cvalue)
	Assertnoerr(err, t)
	outstrp, err = cvalue.ValStr(tptoken)
	Assertnoerr(err, t)
	if DebugFlag {
		t.Log("Str2ZwrS modified string:    ", *outstrp)
	}
	err = cvalue.Zwr2StrST(tptoken, &ovalue)
	Assertnoerr(err, t)
	outstrp, err = ovalue.ValStr(tptoken)
	Assertnoerr(err, t)
	if DebugFlag {
		t.Log("Zwr2StrS re-modified string: ", *outstrp)
	}
	if *outstrp != origstr {
		t.Log("  Re-modified string should be same as original string but is not")
		t.Log("  Original string:", origstr)
		t.Log("  Modified string:", *outstrp)
		t.Fail()
	}
	//
}
