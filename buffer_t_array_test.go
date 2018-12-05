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

// TestDeleteExclST tests the DeleteExclST() method
func TestDeleteExclST(t *testing.T) {
	var namelst yottadb.BufferTArray
	var tptoken uint64 = yottadb.NOTTP
	var err error

	namelst.Alloc(2, 10) // Need an array of two names not more than 10 bytes
	// We need to create 4 local variables to test this so do that first (thus also testing KeyT.SetValE()
	err = yottadb.SetValE(tptoken, "I have a value", "var1", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, "I wish I was a value", "var2", []string{})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, "I was a value", "var3", []string{"sub1"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, "I AM A VALUE", "var4", []string{})
	Assertnoerr(err, t)
	// Now delete var1 and var3 by exclusively keeping var2 and var 4
	err = namelst.SetValStrLit(tptoken, 0, "var2")
	Assertnoerr(err, t)
	err = namelst.SetValStrLit(tptoken, 1, "var4")
	Assertnoerr(err, t)
	err = namelst.SetElemUsed(tptoken, 2)
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken)
	Assertnoerr(err, t)
	// OK, delete done, see which vars exist
	_, err = yottadb.ValE(tptoken, "var1", []string{"sub1", "sub2"}) // Expect this var to be gone
	if nil == err {
		t.Errorf("var1 found when it should have been deleted (no error occurred when fetched")
	}
	_, err = yottadb.ValE(tptoken, "var2", []string{})
	if nil != err {
		t.Errorf("var2 not found when it should still exist (if ever existed)")
	}
	_, err = yottadb.ValE(tptoken, "var3", []string{"sub1"})
	if nil == err {
		t.Errorf("var3 found when it should have been deleted (no error occurred when fetched")
	}
	_, err = yottadb.ValE(tptoken, "var4", []string{})
	if nil != err {
		t.Errorf("var4 not found when it should still exist (if ever existed)")
	}
}

// TestTpST tests the TpST() method by driving a transaction that sets a couple nodes them verifies they exist after the commit
func TestTpST(t *testing.T) {
	var novars yottadb.BufferTArray
	var namelst yottadb.BufferTArray
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errors int

	namelst.Alloc(2, 10) // Need an array of two names not more than 10 bytes
	// Start with clean slate then drive TP transaction
	Dbdeleteall(tptoken, &errors, t)
	err = novars.TpST(tptoken, TpRtn_cgo(), nil, "BATCH")
	Assertnoerr(err, t)
	// Fetch the two nodes to make sure they are there and have correct values
	val1, err := yottadb.ValE(tptoken, "^Variable1A", []string{"Index0", "Index1", "Index2"})
	Assertnoerr(err, t)
	if "The value of Variable1A" != val1 {
		t.Logf("FAIL - The fetched value of ^Variable1A(\"Index0\",\"Index1\",\"Index2\") was not correct\n")
		t.Logf("       Expected: 'The value of Variable1A', Received: '%s'\n", val1)
		t.Fail()
	}
	val2, err := yottadb.ValE(tptoken, "^Variable2B", []string{"Idx0", "Idx1"})
	Assertnoerr(err, t)
	if "The value of Variable2B" != val2 {
		t.Logf("FAIL - The fetched value of ^Variable2B(\"Idx0\",\"Idx1\") was not correct\n")
		t.Logf("       Expected: 'The value of Variable2B', Received: '%s'\n", val2)
		t.Fail()
	}
}
