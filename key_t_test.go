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
	"bytes"
	"github.com/stretchr/testify/assert"
	"lang.yottadb.com/go/yottadb"
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"strconv"
	"sync"
	"testing"
)

func TestDataSt(t *testing.T) {
	var dbkey yottadb.KeyT
	var ovalue, cvalue yottadb.BufferT
	var tptoken uint64 = yottadb.NOTTP
	var err error

	ovalue.Alloc(64)
	cvalue.Alloc(128)
	// Create a few nodes so we can check DataST() on them
	err = yottadb.SetValE(tptoken, nil, "val1", "^tdaNoSubs", []string{})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val2", "^tdaSubs", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val3", "^tdaSubs", []string{"sub1", "sub2", "sub3"})
	Assertnoerr(err, t)
	// Build query structs for DataST()
	dbkey.Alloc(VarSiz, AryDim, SubSiz) // Reallocate the key
	// Check against a non-existent node - should return 0
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^noExistGbl")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 0)
	Assertnoerr(err, t)
	dval, err := dbkey.DataST(tptoken, nil)
	Assertnoerr(err, t)
	if 0 != int(dval) {
		t.Error("The DataST() value for ^noExistGbl expected to be 0 but was", dval)
	}
	// Check node with value but no subscripts - should be 1
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^tdaNoSubs")
	Assertnoerr(err, t)
	dval, err = dbkey.DataST(tptoken, nil)
	Assertnoerr(err, t)
	if 1 != int(dval) {
		t.Error("The DataST() value for ^tdaNoSubs expected to be 1 but was", dval)
	}
	// Check against a subscripted node with no value but has descendents
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^tdaSubs")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "sub1")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	Assertnoerr(err, t)
	dval, err = dbkey.DataST(tptoken, nil)
	Assertnoerr(err, t)
	if 10 != int(dval) {
		t.Error("The DataST() value for ^tdaSubs(\"sub1\") expected to be 10 but was", dval)
	}
	// Check against a subscripted node with a value and descendants
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 1, "sub2")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 2)
	Assertnoerr(err, t)
	dval, err = dbkey.DataST(tptoken, nil)
	Assertnoerr(err, t)
	if 11 != int(dval) {
		t.Error("The DataST() value for ^tdaSubs(\"sub1\",\"sub2\") expected to be 11 but was", dval)
	}
}

/*func TestDeleteSTNullKeyT(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error

	// Hijack dbkey from DataST() testing so we can delete tdaSubs and check if it exists
	err = dbkey.Subary.SetUsed(tptoken, nil, 0)   // No subs are included
	Assertnoerr(err, t)
	err = dbkey.DeleteST(tptoken, nil, YdbDelTree())
	Assertnoerr(err, t)
	dval, err := dbkey.DataST(tptoken)
	Assertnoerr(err, t)
	if 0 != dval {
		t.Error("FAIL - The ^tdaSubs node still exists after DeleteST() - DataST() returned:", dval)
	}
}*/

func TestDeleteST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error

	defer dbkey.Free()
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^tdaSubs")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "sub2")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	// TODO: we should check to make sure error messages are correctly filled out (!UL replaced with number)
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 0) // No subs are included
	Assertnoerr(err, t)
	err = dbkey.DeleteST(tptoken, nil, YdbDelTree())
	Assertnoerr(err, t)
	dval, err := dbkey.DataST(tptoken, nil)
	Assertnoerr(err, t)
	if 0 != dval {
		t.Error("FAIL - The ^tdaSubs node still exists after DeleteST() - DataST() returned:", dval)
	}
}

func TestValST(t *testing.T) {
	// Not tested because it is already tested in TpST() via ValE()
}

func TestIncrST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var newval2i int
	var incrval, dbval1, dbval2 yottadb.BufferT
	var newval1, newval2 *string

	defer dbkey.Free()
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	defer dbval1.Free()
	dbval1.Alloc(128)
	defer dbval2.Free()
	dbval2.Alloc(128)
	defer incrval.Free()
	incrval.Alloc(16)

	// Create a simple subscripted node, then increment it, then fetch it and compare to returned value
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^ivar")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "isub1")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	Assertnoerr(err, t)
	err = dbval1.SetValStrLit(tptoken, nil, "42")
	Assertnoerr(err, t)
	err = dbkey.SetValST(tptoken, nil, &dbval1) // Set the initial value into the node
	err = incrval.SetValStrLit(tptoken, nil, "2")
	Assertnoerr(err, t)
	err = dbkey.IncrST(tptoken, nil, &incrval, &dbval2)
	Assertnoerr(err, t)
	newval2, err = dbval2.ValStr(tptoken, nil)
	Assertnoerr(err, t)
	newval2i, err = strconv.Atoi(*newval2)
	Assertnoerr(err, t)
	if newval2i != 44 {
		t.Error("The expected increment value is 44 but it is", *newval2)
	}
	// Fetch the value and verify same as what we got back from IncrST()
	err = dbkey.ValST(tptoken, nil, &dbval1)
	Assertnoerr(err, t)
	newval1, err = dbval1.ValStr(tptoken, nil)
	if *newval1 != *newval2 {
		t.Error("Returned and post-increment fetch values not same - db :", *newval1,
			"  returned: ", *newval2)
	}
}

func TestLockIncrSt(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var incrval, dbval1, dbval2 yottadb.BufferT
	var timeout uint64
	var errors int

	defer dbkey.Free()
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	defer dbval1.Free()
	dbval1.Alloc(128)
	defer dbval2.Free()
	dbval2.Alloc(128)
	defer incrval.Free()
	incrval.Alloc(16)
	// Increment a given lock 3 times then start decrementing it and after each check, the lock
	// should still be there until we've decremented the 3rd time after which the lock should
	// NOT be there.
	//
	// First, create the key for the lock we are incrementally locking/unlocking
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^lvar")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "Don't")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 1, "Panic!")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 2)
	Assertnoerr(err, t)
	err = dbkey.LockIncrST(tptoken, nil, timeout) // Lock it 3 times
	Assertnoerr(err, t)
	err = dbkey.LockIncrST(tptoken, nil, timeout) // Lock it 3 times
	Assertnoerr(err, t)
	err = dbkey.LockIncrST(tptoken, nil, timeout) // Lock it 3 times
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, true, t)
	// Start decrementing the lock checking each time it still exists
	err = dbkey.LockDecrST(tptoken, nil)
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, true, t)
	err = dbkey.LockDecrST(tptoken, nil)
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, true, t)
	err = dbkey.LockDecrST(tptoken, nil) // Lock should be gone now
	Assertnoerr(err, t)
	if VerifyLockExists([]byte("^lvar(\"Don't\",\"Panic!\")"), &errors, false, t) {
		t.Error("Lock should be gone but is not")
		errors++
	}
	err = yottadb.LockST(tptoken, nil, 0) // Release all locks
	Assertnoerr(err, t)
}

func TestNodeNextST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var i, errors int
	var incrval, dbval1, dbval2 yottadb.BufferT
	var sublst yottadb.BufferTArray

	defer dbkey.Free()
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	defer dbval1.Free()
	dbval1.Alloc(128)
	defer dbval2.Free()
	dbval2.Alloc(128)
	defer incrval.Free()
	incrval.Alloc(16)
	defer sublst.Free()
	sublst.Alloc(AryDim, SubSiz)
	// Need to start with a clean slate (empty database) so do that first
	Dbdeleteall(tptoken, nil, &errors, t)
	var subs [3][]string
	subs[0] = []string{"sub0a", "sub0b", "sub0c", "sub0d"}
	subs[1] = []string{"sub1a", "sub1b"}
	subs[2] = []string{"sub2a", "sub2b", "sub2c"}
	err = yottadb.SetValE(tptoken, nil, "val0", "^node", subs[0])
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val1", "^node", subs[1])
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val2", "^node", subs[2])
	Assertnoerr(err, t)
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^node") // Initial search var
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 0)
	Assertnoerr(err, t)
	err = sublst.SetElemUsed(tptoken, nil, AryDim)
	Assertnoerr(err, t)
	// Loop to test NodeNextST()
	if DebugFlag {
		t.Log("   Starting NodeNextST() loop")
	}
	for i = 0; ; i++ {
		err = sublst.SetElemUsed(tptoken, nil, AryDim) // Reset each round to (re)set how many array elems are available
		Assertnoerr(err, t)
		err = dbkey.NodeNextST(tptoken, nil, &sublst)
		if nil != err {
			errorcode := yottadb.ErrorCode(err)
			if CheckErrorExpectYDB_ERR_NODEEND(errorcode) {
				// We've reached the end of the list - all done!
				break
			}
			if DebugFlag {
				t.Error("FAIL - NodeNextST() failed:", err)
			}
			Assertnoerr(err, t)
		}
		// Check if subscript list is as expected
		retsubsp, err := Buftary2strary(tptoken, nil, &sublst, t)
		Assertnoerr(err, t)
		if DebugFlag {
			t.Logf("   Retsubsp: %v [len=%d]\n", *retsubsp, len(*retsubsp))
		}
		if !Cmpstrary(retsubsp, &subs[i]) {
			t.Error("   Expected subscript array and return array not the same for index", i)
			t.Log("     Expected:", subs[i])
			t.Log("     Returned:", *retsubsp)
		}
		// Move sublst into dbkey.Subary using the retsubsp subscript array as the source
		for j, v := range *retsubsp {
			err = dbkey.Subary.SetValStr(tptoken, nil, uint32(j), &v)
			Assertnoerr(err, t)
		}
		err = dbkey.Subary.SetElemUsed(tptoken, nil, uint32(len(*retsubsp)))
		Assertnoerr(err, t)
	}
	if 3 != i {
		t.Errorf("Unexpected NodeNextST() loop count - expected 3 but got %d\n", i)
	}
	// Next run the loop in reverse to refetch things using NodePrev()
	dbkey.Subary.SetValStrLit(tptoken, nil, 0, "~~~~~~~~~~") // Set a high-subscript so we find the "last node" doing a prev
	dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	if DebugFlag {
		t.Log("   Starting NodePrevST() loop")
	}
	for i = 2; ; i-- {
		err = sublst.SetElemUsed(tptoken, nil, AryDim) // Reset each round to (re)set how many array elems are available
		Assertnoerr(err, t)
		err = dbkey.NodePrevST(tptoken, nil, &sublst)
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
		retsubsp, err := Buftary2strary(tptoken, nil, &sublst, t)
		Assertnoerr(err, t)
		if DebugFlag {
			t.Logf("   Retsubsp: %v [len=%d]\n", *retsubsp, len(*retsubsp))
		}
		if !Cmpstrary(retsubsp, &subs[i]) {
			t.Error("   Expected subscript array and return array not the same for index", i)
			t.Log("     Expected:", subs[i])
			t.Log("     Returned:", *retsubsp)
		}
		// Move sublst into dbkey.Subary using the retsubsp subscript array as the source
		for j, v := range *retsubsp {
			err = dbkey.Subary.SetValStr(tptoken, nil, uint32(j), &v)
			Assertnoerr(err, t)
		}
		err = dbkey.Subary.SetElemUsed(tptoken, nil, uint32(len(*retsubsp)))
		Assertnoerr(err, t)
	}
	if -1 != i {
		t.Errorf("Unexpected NodePrevST() loop count - expected -1 but got %d\n", i)
	}

}

func TestSetValST(t *testing.T) {
	// Already tested in tests for IncrST(), TpST() directly and several other tests using SetValE()
}

func TestSubNextST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var i, errors int
	var incrval, dbval1, dbval2, dbsub yottadb.BufferT
	var sublst yottadb.BufferTArray

	defer dbkey.Free()
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	defer dbval1.Free()
	dbval1.Alloc(128)
	defer dbval2.Free()
	dbval2.Alloc(128)
	defer incrval.Free()
	incrval.Alloc(16)
	defer sublst.Free()
	sublst.Alloc(AryDim, SubSiz)
	defer dbsub.Free()
	dbsub.Alloc(16)
	// Start with a clean slate
	Dbdeleteall(tptoken, nil, &errors, t)
	// Create a simple 4 element array
	err = yottadb.SetValE(tptoken, nil, "val0", "^dbvar", []string{"sub0"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val1", "^dbvar", []string{"sub1"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val2", "^dbvar", []string{"sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "val3", "^dbvar", []string{"sub3"})
	Assertnoerr(err, t)
	// Initialize key with null subscript so find first one
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^dbvar")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	Assertnoerr(err, t)
	// Start forward SubNextST() loop
	if DebugFlag {
		t.Log("   Starting SubNextST() loop")
	}
	for i = 0; ; i++ {
		err = dbkey.SubNextST(tptoken, nil, &dbsub)
		if nil != err {
			errorcode := yottadb.ErrorCode(err)
			if CheckErrorExpectYDB_ERR_NODEEND(errorcode) {
				// We've reached the end of the list - all done!
				break
			}
			if DebugFlag {
				t.Error("SubNext() failed:", err)
			}
			Assertnoerr(err, t) // Unknown error - cause panic
		}
		// Validate subname
		retsub, err := dbsub.ValStr(tptoken, nil)
		Assertnoerr(err, t)
		expectsub := "sub" + strconv.Itoa(i)
		if *retsub != expectsub {
			t.Errorf("Subscript not what was expected. Expected: %s but got %s\n", expectsub, *retsub)
		}
		// Set the returned subscript into dbkey
		err = dbkey.Subary.SetValStr(tptoken, nil, 0, retsub)
		Assertnoerr(err, t)
	}
	// Verify loop termination conditions
	if 4 != i {
		t.Error("Unexpected SubNextST() loop count - expected 4 but got", i)
	}
	// Now run the loop the other direction using SubPrevST()
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "~~~~~~~~~~")
	Assertnoerr(err, t)
	if DebugFlag {
		t.Log("   Starting SubPrevST() loop")
	}
	for i = 3; ; i-- {
		err = dbkey.SubPrevST(tptoken, nil, &dbsub)
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
		retsub, err := dbsub.ValStr(tptoken, nil)
		Assertnoerr(err, t)
		expectsub := "sub" + strconv.Itoa(i)
		if *retsub != expectsub {
			t.Errorf("Subscript not what was expected. Expected: %s but got %s\n", expectsub, *retsub)
		}
		// Set the returned subscript into dbkey
		err = dbkey.Subary.SetValStr(tptoken, nil, 0, retsub)
		Assertnoerr(err, t)
	}
	// Verify loop termination conditions
	if -1 != i {
		t.Error("Unexpected SubPrevST() loop count - expected -1 but got", i)
	}
}

func TestKeyTDumpToWriter(t *testing.T) {
	var value yottadb.KeyT
	var buf1 bytes.Buffer

	value.DumpToWriter(&buf1)
}

func TestKeyTNilRecievers(t *testing.T) {
	var value *yottadb.KeyT
	var tp = yottadb.NOTTP

	var safe = func() {
		r := recover()
		assert.NotNil(t, r)
	}

	var test_wrapper = func(f func()) {
		defer safe()
		f()
		assert.Fail(t, "panic expected, but did not occur")
	}

	test_wrapper(func() { value.Alloc(64, 64, 64) })
	test_wrapper(func() { value.Dump() })
	test_wrapper(func() { value.DumpToWriter(nil) })
	//test_wrapper(func() { value.Free() }) // Free does not panic if rec. nil
	test_wrapper(func() { value.DataST(tp, nil) })
	test_wrapper(func() { value.DeleteST(tp, nil, 0) })
	test_wrapper(func() { value.ValST(tp, nil, nil) })
	test_wrapper(func() { value.IncrST(tp, nil, nil, nil) })
	test_wrapper(func() { value.LockDecrST(tp, nil) })
	test_wrapper(func() { value.LockIncrST(tp, nil, 0) })
	test_wrapper(func() { value.NodeNextST(tp, nil, nil) })
	test_wrapper(func() { value.NodePrevST(tp, nil, nil) })
	test_wrapper(func() { value.SetValST(tp, nil, nil) })
	test_wrapper(func() { value.SubNextST(tp, nil, nil) })
	test_wrapper(func() { value.SubPrevST(tp, nil, nil) })
}

func TestKeyTGetValueThatWontFitInBuffer(t *testing.T) {
	// Get a value that doesn't fit in the provided buffer
	var key yottadb.KeyT
	var buff yottadb.BufferT
	var tptoken = yottadb.NOTTP
	var err error

	defer key.Free()
	key.Alloc(10, 1, 10)

	defer buff.Free()
	buff.Alloc(10)

	key.Varnm.SetValStrLit(tptoken, nil, "^MyVal")
	key.Subary.SetValStrLit(tptoken, nil, 0, "A")
	key.Subary.SetElemUsed(tptoken, nil, 1)

	err = yottadb.SetValE(tptoken, nil, "1234567890A", "^MyVal", []string{"A"})
	assert.Nil(t, err)
	err = yottadb.SetValE(tptoken, nil, "1234567890A", "^MyVal", []string{"V1234567890A"})
	assert.Nil(t, err)

	// Get the value
	err = key.ValST(tptoken, nil, &buff)
	assert.NotNil(t, err)
	errcode := yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)

	// Verify that getting len on the buffer results in error
	_, err = buff.LenUsed(tptoken, nil)
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)

	// Verify that getting val on the buffer results in error
	_, err = buff.ValBAry(tptoken, nil)
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)

	// Verify that getting len on the buffer results in error
	_, err = buff.ValStr(tptoken, nil)
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
}

func TestKeyTNodeNextWithSmallBufAry(t *testing.T) {
	// Get a value that doesn't fit in the provided buffer
	var key yottadb.KeyT
	var buftary yottadb.BufferTArray
	var tptoken = yottadb.NOTTP
	var err error
	var errcode int

	defer key.Free()
	key.Alloc(10, 1, 10)

	key.Varnm.SetValStrLit(tptoken, nil, "^MyVal")
	key.Subary.SetValStrLit(tptoken, nil, 0, "A")
	key.Subary.SetElemUsed(tptoken, nil, 1)

	err = yottadb.SetValE(tptoken, nil, "1234567890A", "^MyVal", []string{"A"})
	assert.Nil(t, err)
	err = yottadb.SetValE(tptoken, nil, "1234567890A", "^MyVal", []string{"V1234567890A"})
	assert.Nil(t, err)

	// Try the same thing BufferTArray
	defer buftary.Free()
	buftary.Alloc(1, 12)
	err = key.NodeNextST(tptoken, nil, &buftary)
	assert.Nil(t, err)

	buftary.Alloc(1, 5) // Make buffer too small
	err = key.NodeNextST(tptoken, nil, &buftary)
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)

	_, err = buftary.ValStr(tptoken, nil, 0)
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	buftary.SetElemUsed(tptoken, nil, 1)

	_, err = buftary.ValBAry(tptoken, nil, 0)
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	buftary.SetElemUsed(tptoken, nil, 1)

	err = buftary.SetValStrLit(tptoken, nil, 0, "Hello world")
	assert.NotNil(t, err)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	buftary.SetElemUsed(tptoken, nil, 1)
}

func TestKeyTGetWithUndefGlobal(t *testing.T) {
	var key yottadb.KeyT
	var errstr, out yottadb.BufferT

	tptoken := yottadb.NOTTP

	key.Alloc(10, 1, 10)
	key.Varnm.SetValStrLit(tptoken, nil, "^MyVal")
	key.Subary.SetValStrLit(tptoken, nil, 0, "")

	defer errstr.Free()
	errstr.Alloc(64)
	defer out.Free()
	out.Alloc(64)

	err := key.ValST(tptoken, &errstr, &out)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "YDB-E-GVUNDEF")
}

func TestKeyTSetWithDifferentErrors(t *testing.T) {
	var wg sync.WaitGroup

	tptoken := yottadb.NOTTP

	// Kick off procs to test
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			var errstr, out yottadb.BufferT
			var key1 yottadb.KeyT

			// GVUNDEF error
			key1.Alloc(10, 1, 10)
			key1.Varnm.SetValStrLit(tptoken, nil, "^MyVal")
			key1.Subary.SetValStrLit(tptoken, nil, 0, "")

			defer errstr.Free()
			errstr.Alloc(64)
			defer out.Free()
			out.Alloc(5)

			for j := 0; j < 1000; j++ {
				err := key1.ValST(tptoken, &errstr, &out)
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), "YDB-E-GVUNDEF")
			}
			wg.Done()
		}()
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			var errstr, tmp, out yottadb.BufferT
			var key2 yottadb.KeyT

			// INVSTRLEN error
			key2.Alloc(10, 1, 64)
			key2.Varnm.SetValStrLit(tptoken, nil, "^MyVal2")
			key2.Subary.SetValStrLit(tptoken, nil, 0, "")
			defer tmp.Free()
			tmp.Alloc(10)
			tmp.SetValStrLit(tptoken, nil, "1234567890")
			key2.SetValST(tptoken, nil, &tmp)

			defer errstr.Free()
			errstr.Alloc(64)
			defer out.Free()
			out.Alloc(5)

			for j := 0; j < 1000; j++ {
				err := key2.ValST(tptoken, &errstr, &out)
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), "YDB-E-INVSTRLEN")
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestKeyTSimpleAPITPDeadlock(t *testing.T) {
	t.Skipf("This test causes a deadlock; we do not currently believe this can be avoided")
	fn := func(tptoken uint64, errstr *yottadb.BufferT) int32 {
		err := yottadb.SetValE(yottadb.NOTTP, errstr, "Hello world", "^Hello", []string{})
		assert.NotNil(t, err)
		assert.Equal(t, "", err.Error())
		return 0
	}
	err := yottadb.TpE(yottadb.NOTTP, nil, fn, "BATCH", []string{})
	assert.NotNil(t, err)
	assert.Equal(t, "", err.Error())
}

func TestKeyTWithNil(t *testing.T) {
	var value yottadb.KeyT
	var buf1 bytes.Buffer
	var tp = yottadb.NOTTP

	total_panics := 0
	var expected_panic string

	var safe = func() {
		r := recover()
		if r != nil {
			total_panics += 1
			assert.Equal(t, expected_panic, r)
		}
	}

	wrapper_functions := []func(func()){func(f func()) {
		defer safe()
		value.Alloc(64, 10, 64)
		value.Varnm = nil
		expected_panic = "KeyT varname is not allocated, is nil, or has a 0 length"
		f()
	}, func(f func()) {
		defer safe()
		value.Alloc(64, 10 , 64)
		value.Varnm.SetValStrLit(tp, nil, "my_variable")
		value.Subary = nil
		expected_panic = "KeyT Subary is nil"
		f()
	}}

	for _, test_wrapper := range wrapper_functions {
		total_panics = 0
		test_wrapper(func() { value.Alloc(64, 10, 64) } )
		test_wrapper(func() { value.DataST(tp, nil) } )
		test_wrapper(func() { value.DeleteST(tp, nil, yottadb.YDB_DEL_TREE) } )
		test_wrapper(func() { value.DumpToWriter(&buf1) } )
		test_wrapper(func() { value.Free() } )
		test_wrapper(func() { value.IncrST(tp, nil, nil, nil) })
		test_wrapper(func() { value.LockDecrST(tp, nil) } )
		test_wrapper(func() { value.LockIncrST(tp, nil, 0) } )
		test_wrapper(func() { value.NodeNextST(tp, nil, nil) } )
		test_wrapper(func() { value.NodePrevST(tp, nil, nil) } )
		test_wrapper(func() { value.SetValST(tp, nil, nil) } )
		test_wrapper(func() { value.SubNextST(tp, nil, nil) } )
		test_wrapper(func() { value.SubPrevST(tp, nil, nil) } )
		test_wrapper(func() { value.ValST(tp, nil, nil) } )
		assert.NotEqual(t, 0, total_panics)
	}
}
