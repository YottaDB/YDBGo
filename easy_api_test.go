//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2022 YottaDB LLC and/or its subsidiaries.	//
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
	"strconv"
	"testing"
)

func TestDataE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var dval uint32
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// Create a few nodes so we can check DataE() on them
	err = yottadb.SetValE(tptoken, &errstr, "val1", "^tdaNoSubs", []string{})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val2", "^tdaSubs", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val3", "^tdaSubs", []string{"sub1", "sub2", "sub3"})
	Assertnoerr(err, t)
	// Check against a non-existent node - should return 0
	dval, err = yottadb.DataE(tptoken, &errstr, "^noExistGbl", []string{})
	Assertnoerr(err, t)
	if 0 != int(dval) {
		t.Error("FAIL - The DataE() value for ^noExistGbl expected to be 0 but was", dval)
	}
	// Check node with value but no subscripts - should be 1
	dval, err = yottadb.DataE(tptoken, &errstr, "^tdaNoSubs", []string{})
	Assertnoerr(err, t)
	if yottadb.YDB_DATA_VALUE_NODESC != int(dval) {
		t.Error("The DataE() value for ^tdaNoSubs expected to be 1 but was", dval)
	}
	// Check against a subscripted node with no value but has descendants
	dval, err = yottadb.DataE(tptoken, &errstr, "^tdaSubs", []string{"sub1"})
	Assertnoerr(err, t)
	if yottadb.YDB_DATA_NOVALUE_DESC != int(dval) {
		t.Error("The DataE() value for ^tdaSubs(\"sub1\") expected to be 10 but was", dval)
	}
	// Check against a subscripted node with a value and descendants
	dval, err = yottadb.DataE(tptoken, &errstr, "^tdaSubs", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	if yottadb.YDB_DATA_VALUE_DESC != int(dval) {
		t.Error("The DataE() value for ^tdaSubs(\"sub1\",\"sub2\") expected to be 11 but was", dval)
	}
	// Check if return value set correctly on an error by giving it an invalid var name
	dval, err = yottadb.DataE(tptoken, &errstr, "12345", []string{})
	if yottadb.YDB_DATA_ERROR != int(dval) {
		t.Error("The DataE() return value for an invalid varname expected to be YDB_DATA_NOERROR but was", dval)
	}
}

func TestDataEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	_, err = yottadb.DataE(tptoken, &errstr, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DataE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	_, err = yottadb.DataE(tptoken, &errstr, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The DataE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.DataE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The DataE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.DataE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The DataE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.DataE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The DataE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
}

func TestDeleteE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var dval uint32
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// Create a few nodes so we can check DataE() on them
	err = yottadb.SetValE(tptoken, &errstr, "val1", "^tdaNoSubs", []string{})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val2", "^tdaSubs", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val3", "^tdaSubs", []string{"sub1", "sub2", "sub3"})
	Assertnoerr(err, t)
	err = yottadb.DeleteE(tptoken, &errstr, yottadb.YDB_DEL_TREE, "^tdaSubs", []string{})
	Assertnoerr(err, t)
	dval, err = yottadb.DataE(tptoken, &errstr, "^tdaSubs", []string{})
	Assertnoerr(err, t)
	if 0 != dval {
		t.Error("The ^tdaSubs node still exists after DeleteE() - DataE() returned:", dval)
	}
	err = yottadb.DeleteE(tptoken, &errstr, yottadb.YDB_DEL_NODE, "^tdaNoSubs", []string{})
	Assertnoerr(err, t)
	dval, err = yottadb.DataE(tptoken, &errstr, "^tdaNoSubs", []string{})
	Assertnoerr(err, t)
	if 0 != dval {
		t.Error("The ^tdaNoSubs node still exists after DeleteE() - DataE() returned:", dval)
	}
}

func TestDeleteEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	err = yottadb.DeleteE(tptoken, &errstr, yottadb.YDB_DEL_TREE, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP - from special variable
	err = yottadb.DeleteE(tptoken, &errstr, yottadb.YDB_DEL_TREE, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The DeleteE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	err = yottadb.DeleteE(tptoken, &errstr, yottadb.YDB_DEL_TREE, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The DeleteE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = yottadb.DeleteE(yottadb.NOTTP, &errstr, yottadb.YDB_DEL_TREE, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The DeleteE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	err = yottadb.DeleteE(yottadb.NOTTP, &errstr, yottadb.YDB_DEL_TREE, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The DeleteE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP - from bad deltype parm (not YDB_DEL_NODE/TREE)
	err = yottadb.DeleteE(tptoken, &errstr, 999, "^deletethis", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The DeleteE() errorcode for deltype 999 expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
}

func TestDeleteExclE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errcode int
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// We need to create 4 local variables to test this so do that first (thus also testing KeyT.SetValE()
	err = yottadb.SetValE(tptoken, &errstr, "I have a value", "var1", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "I wish I was a value", "var2", []string{})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "I was a value", "var3", []string{"sub1"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "I AM A VALUE", "var4", []string{})
	Assertnoerr(err, t)
	_, err = yottadb.ValE(tptoken, &errstr, "var1", []string{"sub1", "sub2"})
	// Now delete var1 and var3 by exclusively keeping var2 and var4
	err = yottadb.DeleteExclE(tptoken, &errstr, []string{"var2", "var4"})
	Assertnoerr(err, t)
	// OK, delete done, see which vars exist
	_, err = yottadb.ValE(tptoken, &errstr, "var1", []string{"sub1", "sub2"}) // Expect this var to be gone
	if nil == err {
		t.Error("var1 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode { // YDB_ERR_LVUNDEF
		t.Error("The ValE() errorcode for var1(\"sub1\",\"sub2\") expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	_, err = yottadb.ValE(tptoken, &errstr, "var2", []string{})
	if nil != err {
		t.Error("var2 not found when it should still exist (if ever existed)")
	}
	_, err = yottadb.ValE(tptoken, &errstr, "var3", []string{"sub1"})
	if nil == err {
		t.Error("var3 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode { // YDB_ERR_LVUNDEF
		t.Error("The ValE() errorcode for var3(\"sub1\") expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	_, err = yottadb.ValE(tptoken, &errstr, "var4", []string{})
	if nil != err {
		t.Error("var4 not found when it should still exist (if ever existed)")
	}
	// Pass an empty array check that it deletes var2 and var4
	err = yottadb.DeleteExclE(tptoken, &errstr, []string{})
	Assertnoerr(err, t)
	_, err = yottadb.ValE(tptoken, &errstr, "var2", []string{}) // Expect this var to be gone
	if nil == err {
		t.Error("var2 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode { // YDB_ERR_LVUNDEF
		t.Error("The ValE() errorcode for var2 expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	_, err = yottadb.ValE(tptoken, &errstr, "var4", []string{}) // Expect this var to be gone
	if nil == err {
		t.Error("var4 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode { // YDB_ERR_LVUNDEF
		t.Error("The ValE() errorcode for var4 expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
}

func TestDeleteExclEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME - from passing a bad M name
	err = yottadb.DeleteExclE(tptoken, &errstr, []string{"1"})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteExclE() errorcode for 1 expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_INVVARNAME - from special variable
	err = yottadb.DeleteExclE(tptoken, &errstr, []string{"$ZCHSET"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteExclE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_INVVARNAME - from passing a global
	err = yottadb.DeleteExclE(tptoken, &errstr, []string{"^a"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteExclE() errorcode for ^a expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = yottadb.DeleteExclE(yottadb.NOTTP, &errstr, []string{"a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The DeleteExclE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_NAMECOUNT2HI
	err = yottadb.DeleteExclE(yottadb.NOTTP, &errstr, []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "a10", "a11", "a12", "a13", "a14", "a15", "a16", "a17", "a18", "a19", "a20", "a21", "a22", "a23", "a24", "a25", "a26", "a27", "a28", "a29", "a30", "a31", "a32", "a33", "a34", "a35", "a36"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_NAMECOUNT2HI != errcode {
		t.Error("The DeleteExclE() errorcode for node with 36 subscripts expected to be", yottadb.YDB_ERR_NAMECOUNT2HI, "but was", errcode)
	}
}

func TestValE(t *testing.T) {
	// Already tested in tests for TpST() and DeleteExclE()
}

func TestValEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	_, err = yottadb.ValE(tptoken, &errstr, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The ValE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_GVUNDEF
	_, err = yottadb.ValE(tptoken, &errstr, "^doesnotexist", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_GVUNDEF != errcode {
		t.Error("The ValE() errorcode for ^doesnotexist expected to be", yottadb.YDB_ERR_GVUNDEF, "but was", errcode)
	}
	// YDB_ERR_LVUNDEF
	_, err = yottadb.ValE(tptoken, &errstr, "doesnotexist", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode {
		t.Error("The ValE() errorcode for doesnotexist expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.ValE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The ValE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.ValE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The ValE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.ValE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The ValE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
}

func TestIncrE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var newvalA, newvalB string
	var err error
	var newvalBi int
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// Create a simple subscripted node, then increment it, then fetch it and compare to returned value
	// increment by a value (2)
	err = yottadb.SetValE(tptoken, &errstr, "42", "^ivar", []string{"isub1"})
	Assertnoerr(err, t)
	newvalB, err = yottadb.IncrE(tptoken, &errstr, "2", "^ivar", []string{"isub1"})
	Assertnoerr(err, t)
	newvalBi, err = strconv.Atoi(newvalB)
	Assertnoerr(err, t)
	if 44 != newvalBi {
		t.Error("The expected increment value is 44 but it is", newvalB)
	}
	// Fetch the value and verify same as what we got back from IncrST()
	newvalA, err = yottadb.ValE(tptoken, &errstr, "^ivar", []string{"isub1"})
	Assertnoerr(err, t)
	if newvalA != newvalB {
		t.Error("Returned and post-increment fetch values not same - db :", newvalA,
			"  returned: ", newvalB)
	}
	// increment by nil which should increase global by 1
	newvalB, err = yottadb.IncrE(tptoken, &errstr, "", "^ivar", []string{"isub1"})
	Assertnoerr(err, t)
	newvalBi, err = strconv.Atoi(newvalB)
	Assertnoerr(err, t)
	if 45 != newvalBi {
		t.Error("The expected increment value is 45 but it is", newvalB)
	}
	// Fetch the value and verify same as what we got back from IncrST()
	newvalA, err = yottadb.ValE(tptoken, &errstr, "^ivar", []string{"isub1"})
	Assertnoerr(err, t)
	if newvalA != newvalB {
		t.Error("Returned and post-increment fetch values not same - db :", newvalA,
			"  returned: ", newvalB)
	}
	// Increment a nonexistant node, check that it is set to the increment
	newvalB, err = yottadb.IncrE(tptoken, &errstr, "9001", "^ivar", []string{"isub2"})
	Assertnoerr(err, t)
	newvalBi, err = strconv.Atoi(newvalB)
	Assertnoerr(err, t)
	if 9001 != newvalBi {
		t.Error("The expected increment value is 9001 but it is", newvalB)
	}
	// Fetch the value and verify same as what we got back from IncrST()
	newvalA, err = yottadb.ValE(tptoken, &errstr, "^ivar", []string{"isub2"})
	Assertnoerr(err, t)
	if newvalA != newvalB {
		t.Error("Returned and post-increment fetch values not same - db :", newvalA,
			"  returned: ", newvalB)
	}
	// Increment a non-cannonical number, check that it is foreced to a number
	err = yottadb.SetValE(tptoken, &errstr, "This is not a number", "^ivar", []string{"isub3"})
	Assertnoerr(err, t)
	newvalB, err = yottadb.IncrE(tptoken, &errstr, "1337", "^ivar", []string{"isub3"})
	Assertnoerr(err, t)
	newvalBi, err = strconv.Atoi(newvalB)
	Assertnoerr(err, t)
	if 1337 != newvalBi {
		t.Error("The expected increment value is 1337 but it is", newvalB)
	}
	// Fetch the value and verify same as what we got back from IncrST()
	newvalA, err = yottadb.ValE(tptoken, &errstr, "^ivar", []string{"isub3"})
	Assertnoerr(err, t)
	if newvalA != newvalB {
		t.Error("Returned and post-increment fetch values not same - db :", newvalA,
			"  returned: ", newvalB)
	}
}

func TestIncrEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	_, err = yottadb.IncrE(tptoken, &errstr, "", "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The IncrE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	_, err = yottadb.IncrE(tptoken, &errstr, "", "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The IncrE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.IncrE(tptoken, &errstr, "", "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The IncrE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.IncrE(yottadb.NOTTP, &errstr, "", "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The IncrE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.IncrE(yottadb.NOTTP, &errstr, "", "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The IncrE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
	// YDB_ERR_NUMOFLOW
	_, err = yottadb.IncrE(tptoken, &errstr, "1E12345", "^ivar", []string{"newsub"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_NUMOFLOW != errcode {
		t.Error("The IncrE() errorcode for incrementing by 1E12345 expected to be", yottadb.YDB_ERR_NUMOFLOW, "but was", errcode)
	}
}

func TestLockE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var timeout uint64
	var errors int
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// Take out 3 locks (2 global, 1 local) with one call and verify they are held. Note more than two subscripts breaks verifyLockExists()
	err = yottadb.LockE(tptoken, &errstr, timeout, "^lock1", []string{"sub11", "sub12"}, "lock2", []string{}, "^lock3", []string{"sub21"})
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lock1(\"sub11\",\"sub12\")"), &errors, true, t)
	VerifyLockExists([]byte("lock2"), &errors, true, t)
	VerifyLockExists([]byte("^lock3(\"sub21\")"), &errors, true, t)
}

func TestLockEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	err = yottadb.LockE(tptoken, &errstr, 0, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The LockE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	err = yottadb.LockE(tptoken, &errstr, 0, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The LockE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	err = yottadb.LockE(tptoken, &errstr, 0, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The LockE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = yottadb.LockE(yottadb.NOTTP, &errstr, 0, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The LockE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	err = yottadb.LockE(yottadb.NOTTP, &errstr, 0, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "a33", "a34", "a35"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The LockE() errorcode for node with 35 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
	// YDB_ERR_TIME2LONG
	err = yottadb.LockE(tptoken, &errstr, yottadb.YDB_MAX_TIME_NSEC+1, "a", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_TIME2LONG != errcode {
		t.Error("The LockE() errorcode for timeout of YDB_MAX_TIME_NSEC+1 expected to be", yottadb.YDB_ERR_TIME2LONG, "but was", errcode)
	}
	// YDB_ERR_NAMECOUNT2HI
	err = yottadb.LockE(tptoken, &errstr, 0, "a1", []string{}, "a2", []string{}, "a3", []string{}, "a4", []string{}, "a5", []string{}, "a6", []string{}, "a7", []string{}, "a8", []string{}, "a9", []string{}, "a10", []string{}, "a11", []string{}, "a12", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_NAMECOUNT2HI != errcode {
		t.Error("The LockE() errorcode for 12 lock pairs expected to be", yottadb.YDB_ERR_NAMECOUNT2HI, "but was", errcode)
	}
	// YDB_ERR_INVLKNMPAIRLIST
	err = yottadb.LockE(tptoken, &errstr, 0, "a")
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVLKNMPAIRLIST != errcode {
		t.Error("The LockE() errorcode for an incorrect pair expected to be", yottadb.YDB_ERR_INVLKNMPAIRLIST, "but was", errcode)
	}
}

func TestLockIncrE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var timeout uint64
	var errors int
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// Increment a given lock 3 times then start decrementing it and after each check, the lock
	// should still be there until we've decremented the 3rd time after which the lock should
	// NOT be there.
	//
	// First, create the key for the lock we are incrementally locking/unlocking
	err = yottadb.LockIncrE(tptoken, &errstr, timeout, "^lvar", []string{"Don't", "Panic!"}) // Lock it 3 times
	Assertnoerr(err, t)
	err = yottadb.LockIncrE(tptoken, &errstr, timeout, "^lvar", []string{"Don't", "Panic!"})
	Assertnoerr(err, t)
	err = yottadb.LockIncrE(tptoken, &errstr, timeout, "^lvar", []string{"Don't", "Panic!"})
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, true, t)
	// Start decrementing the lock checking each time it still exists
	err = yottadb.LockDecrE(tptoken, &errstr, "^lvar", []string{"Don't", "Panic!"})
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, true, t)
	err = yottadb.LockDecrE(tptoken, &errstr, "^lvar", []string{"Don't", "Panic!"})
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, true, t)
	err = yottadb.LockDecrE(tptoken, &errstr, "^lvar", []string{"Don't", "Panic!"})
	Assertnoerr(err, t)
	if VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, false, t) {
		t.Error("Lock should be gone but is not")
	}
	err = yottadb.LockST(tptoken, &errstr, 0) // Release all locks
	Assertnoerr(err, t)
}

func TestLockIncrEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	err = yottadb.LockIncrE(tptoken, &errstr, 0, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The LockIncrE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	err = yottadb.LockIncrE(tptoken, &errstr, 0, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The LockIncrE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	err = yottadb.LockIncrE(tptoken, &errstr, 0, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The LockIncrE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = yottadb.LockIncrE(yottadb.NOTTP, &errstr, 0, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The LockIncrE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	err = yottadb.LockIncrE(yottadb.NOTTP, &errstr, 0, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "a33", "a34", "a35"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The LockIncrE() errorcode for node with 35 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
	// YDB_ERR_TIME2LONG
	err = yottadb.LockIncrE(tptoken, &errstr, yottadb.YDB_MAX_TIME_NSEC+1, "a", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_TIME2LONG != errcode {
		t.Error("The LockIncrE() errorcode for timeout of YDB_MAX_TIME_NSEC+1 expected to be", yottadb.YDB_ERR_TIME2LONG, "but was", errcode)
	}

	// YDB_ERR_INVVARNAME
	err = yottadb.LockDecrE(tptoken, &errstr, "^", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The LockDecrE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	err = yottadb.LockDecrE(tptoken, &errstr, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The LockDecrE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	err = yottadb.LockDecrE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The LockDecrE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = yottadb.LockDecrE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The LockDecrE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	err = yottadb.LockDecrE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "a33", "a34", "a35"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The LockDecrE() errorcode for node with 35 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
}

func TestMessageE(t *testing.T) {
	// Already tested in tests for NodeNextST()/NodePrevST()
}

func TestNodeNextE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errors, i int
	var subscrary []string
	var subs [3][]string
	var sublst yottadb.BufferTArray
	var errstr yottadb.BufferT

	errstr.Alloc(128)
	sublst.Alloc(AryDim, SubSiz)

	// Need to start with a clean slate (empty database) so do that first
	Dbdeleteall(tptoken, &errstr, &errors, t)
	subs[0] = []string{"sub0a", "sub0b", "sub0c", "sub0d"}
	subs[1] = []string{"sub1a", "sub1b"}
	subs[2] = []string{"sub2a", "sub2b", "sub2c-but a long sub2c comparatively"}
	err = yottadb.SetValE(tptoken, &errstr, "val0", "^node", subs[0])
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val1", "^node", subs[1])
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val2", "^node", subs[2])
	Assertnoerr(err, t)
	subscrary = []string{""}
	// Loop to test NodeNextST()
	if DebugFlag {
		t.Log("   Starting NodeNextE() loop")
	}
	for i = 0; ; i++ {
		err = sublst.SetElemUsed(tptoken, &errstr, AryDim) // Reset each round to (re)set how many array elems are available
		Assertnoerr(err, t)
		retsubs, err := yottadb.NodeNextE(tptoken, &errstr, "^node", subscrary)
		if nil != err {
			errorcode := yottadb.ErrorCode(err)
			if CheckErrorExpectYDB_ERR_NODEEND(errorcode) {
				// We've reached the end of the list - all done!
				break
			}
			if DebugFlag {
				t.Error("NodeNextST() failed:", err)
			}
			Assertnoerr(err, t)
		}
		// Check if subscript list is as expected
		if DebugFlag {
			t.Logf("   Retsubs: %v [len=%d]\n", retsubs, len(retsubs))
		}
		if !Cmpstrary(&retsubs, &subs[i]) {
			t.Error("Expected subscript array and return array not the same for index", i)
			t.Log("     Expected:", subs[i])
			t.Log("     Returned:", retsubs)
		}
		// Move sublst into dbkey.Subary using the retsubs subscript array as the source
		subscrary = retsubs
	}
	if 3 != i {
		t.Errorf("Unexpected NodeNextST() loop count - expected 3 but got %d\n", i)
	}
	// Next run the loop in reverse to refetch things using NodePrev()
	subscrary = []string{"~~~~~~~~~~"}
	if DebugFlag {
		t.Log("   Starting NodePrevE() loop")
	}
	for i = 2; ; i-- {
		err = sublst.SetElemUsed(tptoken, &errstr, AryDim) // Reset each round to (re)set how many array elems are available
		Assertnoerr(err, t)
		retsubs, err := yottadb.NodePrevE(tptoken, &errstr, "^node", subscrary)
		if nil != err {
			errorcode := yottadb.ErrorCode(err)
			if CheckErrorExpectYDB_ERR_NODEEND(errorcode) {
				// We've reached the end of the list - all done!
				break
			}
			if DebugFlag {
				t.Error("NodePrevST() failed:", err)
			}
			Assertnoerr(err, t)
		}
		// Check if subscript list is as expected
		if DebugFlag {
			t.Logf("   Retsubs: %v [len=%d]\n", retsubs, len(retsubs))
		}
		if !Cmpstrary(&retsubs, &subs[i]) {
			t.Error("   Expected subscript array and return array not the same for index", i)
			t.Log("     Expected:", subs[i])
			t.Log("     Returned:", retsubs)
		}
		// Move sublst into dbkey.Subary using the retsubs subscript array as the source
		subscrary = retsubs
	}
	if -1 != i {
		t.Errorf("Unexpected NodePrevST() loop count - expected -1 but got %d\n", i)
	}
}

func TestNodeNextEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	_, err = yottadb.NodeNextE(tptoken, &errstr, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The NodeNextE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	_, err = yottadb.NodeNextE(tptoken, &errstr, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The NodeNextE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.NodeNextE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The NodeNextE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.NodeNextE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The NodeNextE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.NodeNextE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The NodeNextE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}

	// YDB_ERR_INVVARNAME
	_, err = yottadb.NodePrevE(tptoken, &errstr, "^", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The NodePrevE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	_, err = yottadb.NodePrevE(tptoken, &errstr, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The NodePrevE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.NodePrevE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The NodePrevE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.NodePrevE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The NodePrevE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.NodePrevE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The NodePrevE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
}

func TestSetValE(t *testing.T) {
	// Already tested in *MANY* tests above
}

func TestSetValEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	err = yottadb.SetValE(tptoken, &errstr, "A Value", "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The SetValE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_SVNOSET
	err = yottadb.SetValE(tptoken, &errstr, "A Value", "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_SVNOSET != errcode {
		t.Error("The SetValE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_SVNOSET, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	err = yottadb.SetValE(tptoken, &errstr, "A Value", "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The SetValE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = yottadb.SetValE(yottadb.NOTTP, &errstr, "A Value", "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The SetValE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	err = yottadb.SetValE(yottadb.NOTTP, &errstr, "A Value", "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The SetValE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
}

func TestSubNextE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errors, i int
	var subscrary []string
	var sublst yottadb.BufferTArray
	var errstr yottadb.BufferT

	errstr.Alloc(128)
	sublst.Alloc(AryDim, SubSiz)

	// Start with a clean slate
	Dbdeleteall(tptoken, &errstr, &errors, t)
	// Create a simple 4 element array
	err = yottadb.SetValE(tptoken, &errstr, "val0", "^dbvar", []string{"sub0"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val1", "^dbvar", []string{"sub1"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val2", "^dbvar", []string{"sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, &errstr, "val3", "^dbvar", []string{"sub3"})
	Assertnoerr(err, t)
	// Initialize key with null subscript so find first one
	subscrary = []string{""}
	// Start forward SubNextE() loop
	if DebugFlag {
		t.Log("   Starting SubNextE() loop")
	}
	for i = 0; ; i++ {
		retsub, err := yottadb.SubNextE(tptoken, &errstr, "^dbvar", subscrary)
		if nil != err {
			errorcode := yottadb.ErrorCode(err)
			if CheckErrorExpectYDB_ERR_NODEEND(errorcode) {
				// We've reached the end of the list - all done!
				break
			}
			if DebugFlag {
				t.Error("SubNextE() failed:", err)
			}
			Assertnoerr(err, t) // Unknown error - cause panic
		}
		// Validate subname
		expectsub := "sub" + strconv.Itoa(i)
		if retsub != expectsub {
			t.Errorf("Subscript not what was expected. Expected: %s but got %s\n", expectsub, retsub)
		}
		// The returned subscript becomes the next subscript to use
		subscrary[0] = retsub
	}
	// Verify loop termination conditions
	if 4 != i {
		t.Error("Unexpected SubNextE() loop count - expected 4 but got", i)
	}
	// Now run the loop the other direction using SubPrevE()
	subscrary = []string{"~~~~~~~~~~"}
	if DebugFlag {
		t.Log("   Starting SubPrevE() loop")
	}
	for i = 3; ; i-- {
		retsub, err := yottadb.SubPrevE(tptoken, &errstr, "^dbvar", subscrary)
		if nil != err {
			errorcode := yottadb.ErrorCode(err)
			if CheckErrorExpectYDB_ERR_NODEEND(errorcode) {
				// We've reached the end of the list - all done!
				break
			}
			if DebugFlag {
				t.Error("SubPrev() failed:", err)
			}
			Assertnoerr(err, t) // Unknown error - cause panic
		}
		// Validate subname
		expectsub := "sub" + strconv.Itoa(i)
		if retsub != expectsub {
			t.Errorf("Subscript not what was expected. Expected: %s but got %s\n", expectsub, retsub)
		}
		// The returned subscript becomes the next subscript to use
		subscrary[0] = retsub
	}
	// Verify loop termination conditions
	if -1 != i {
		t.Error("Unexpected SubPrevE() loop count - expected -1 but got", i)
	}
}

func TestSubNextEErrors(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	_, err = yottadb.SubNextE(tptoken, &errstr, "^", []string{})
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The SubNextE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	_, err = yottadb.SubNextE(tptoken, &errstr, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The SubNextE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.SubNextE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The SubNextE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.SubNextE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The SubNextE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.SubNextE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The SubNextE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}

	// YDB_ERR_INVVARNAME
	_, err = yottadb.SubPrevE(tptoken, &errstr, "^", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The SubPrevE() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	_, err = yottadb.SubPrevE(tptoken, &errstr, "$ZCHSET", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The SubPrevE() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	_, err = yottadb.SubPrevE(tptoken, &errstr, "$NOTATHING", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The SubPrevE() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	_, err = yottadb.SubPrevE(yottadb.NOTTP, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0", []string{})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The SubPrevE() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	_, err = yottadb.SubPrevE(yottadb.NOTTP, &errstr, "a", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32"})
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The SubPrevE() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
}

func TestTpE(t *testing.T) {
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errors int
	var val1, val2 string
	var errstr yottadb.BufferT

	errstr.Alloc(128)

	// Start with a clean slate
	Dbdeleteall(tptoken, &errstr, &errors, t)
	// Invoke TP transaction
	err = yottadb.TpE(tptoken, &errstr, func(tptoken uint64, errstr *yottadb.BufferT) int32 {
		return int32(TestTpRtn(tptoken, nil, nil))
	}, "BATCH", []string{"*"})
	Assertnoerr(err, t)
	// Fetch the two nodes to make sure they are there and have correct values
	val1, err = yottadb.ValE(tptoken, &errstr, "^Variable1A", []string{"Index0", "Index1", "Index2"})
	Assertnoerr(err, t)
	if "The value of Variable1A" != val1 {
		t.Errorf("The fetched value of ^Variable1A(\"Index0\",\"Index1\",\"Index2\") was not correct\n")
		t.Logf("       Expected: 'The value of Variable1A', Received: '%s'\n", val1)
	}
	val2, err = yottadb.ValE(tptoken, &errstr, "^Variable2B", []string{"Idx0", "Idx1"})
	Assertnoerr(err, t)
	if "The value of Variable2B" != val2 {
		t.Error("The fetched value of ^Variable2B(\"Idx0\",\"Idx1\") was not correct\n")
		t.Logf("       Expected: 'The value of Variable2B', Received: '%s'\n", val2)
	}

}
