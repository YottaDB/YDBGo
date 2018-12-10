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
	"bytes"
	"github.com/stretchr/testify/assert"
	"lang.yottadb.com/go/yottadb"
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"testing"
)

// TestBufTAryDeleteExclST tests the DeleteExclST() method.
func TestBufTAryDeleteExclST(t *testing.T) {
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

// TestBufTAryTpSt tests the TpST() method by driving a transaction that sets a couple nodes and then verifies that they exist after the commit.
func TestBufTAryTpSt(t *testing.T) {
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

func TestBufTAryDump(t *testing.T) {
	var value, noalloc_value yottadb.BufferTArray
	var tp = yottadb.NOTTP
	var buf1 bytes.Buffer

	// Dump from a nil buffer
	noalloc_value.DumpToWriter(&buf1)

	defer value.Free()
	value.Alloc(10, 64)
	value.SetValStrLit(tp, 0, "Hello")
	value.SetElemUsed(tp, 1)
	value.DumpToWriter(&buf1)
	// BufferTArray dump does not show any info about included buffers, so no asserts for
	//  that; it really only mentions used/available buffers
	assert.Contains(t, buf1.String(), "10")
	assert.Contains(t, buf1.String(), "Hello")
}

func TestBufTAryAlloc(t *testing.T) {
	var value, noalloc_value yottadb.BufferTArray
	//var tp = yottadb.NOTTP

	// Try freeing a never Alloc'd buffer
	value.Free()

	// Try allocating a buffer then freeing it many times
	value.Alloc(10, 64)
	value.Free()
	value.Free()

	// Try allocing a size of
	value.Alloc(0, 64)

	// Try allocating a buffer multiple times without freeing
	value.Alloc(10, 64)
	value.Alloc(10, 64)

	// Verify allocated size
	r := value.ElemAlloc()
	assert.Equal(t, r, uint32(10))

	// Try getting ElemAlloc on non-alloced value
	r = noalloc_value.ElemAlloc()
	assert.Equal(t, r, uint32(0))
}

func TestBufTAryLenAlloc(t *testing.T) {
	var value, noalloc_value yottadb.BufferTArray
	var tp = yottadb.NOTTP

	// Try getting length of non-alloc'd array
	r, err := value.ElemLenAlloc(tp)
	assert.NotNil(t, err)
	// TODO: change this function to return 0
	assert.Equal(t, r, uint32(0))

	_, err = noalloc_value.ElemLenAlloc(tp)
	assert.NotNil(t, err)

	value.Alloc(10, 64)
	r, err = value.ElemLenAlloc(tp)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(64))

	t.Skipf("This still needs to be fixed")
	// Alloc a length of 0 and try to get it
	value.Alloc(0, 64)
	r, err = value.ElemLenAlloc(tp)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(0))
}

func TestBufTAryBAry(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP

	v := []byte("Hello")

	// Get value from non-allocd value
	r, err := value.ValBAry(tp, 0)
	assert.NotNil(t, err)
	assert.Nil(t, r)

	// Alloc, but get value past the end
	value.Alloc(10, 64)
	r, err = value.ValBAry(tp, 11)
	assert.NotNil(t, err)
	assert.Nil(t, r)

	// Get a valid value with no content
	r, err = value.ValBAry(tp, 0)

	// Get a value with some value

	err = value.SetValBAry(tp, 1, &v)
	assert.Nil(t, err)
	r, err = value.ValBAry(tp, 1)
	assert.Nil(t, err)
	assert.Equal(t, *r, v)

	// Try set a value on out of bounds element
	err = value.SetValBAry(tp, 11, &v)
	assert.NotNil(t, err)

	// Try to set a value on a freed structure
	value.Free()
	err = value.SetValBAry(tp, 0, &v)
	assert.NotNil(t, err)
	errcode := yottadb.ErrorCode(err)
	t.Skipf("We need to figure out what the expected result is")
	assert.True(t, CheckErrorExpectYDB_ERR_INSUFFSUBS(errcode))
}

func TestBufTAryMultipleThreads(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP

	t.Skipf("Currently causes a great many problems; skip for now")

	// Spawn off 100 threads which allocate the buffer
	for i := 0; i < 100; i++ {
		go (func() {
			for j := 0; j < 1000; j++ {
				value.Alloc(10, 64)
			}
		})()
	}

	// Spawn off 100 threads try to set things
	for i := 0; i < 100; i++ {
		go (func() {
			for j := 0; j < 1000; j++ {
				value.SetValStrLit(tp, 0, "Hello")
			}
		})()
	}
}

func TestBufTAryElemLenUsed(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP

	// Test non alloc'd structure
	err := value.SetElemLenUsed(tp, 0, 0)
	assert.NotNil(t, err)
	r, err := value.ElemLenUsed(tp, 0)
	assert.NotNil(t, err)
	assert.Equal(t, r, uint32(0))

	// Allocate, then test with an element past the end
	value.Alloc(10, 64)

	err = value.SetElemLenUsed(tp, 11, 5)
	assert.NotNil(t, err)
	r, err = value.ElemLenUsed(tp, 11)
	assert.NotNil(t, err)
	assert.Equal(t, r, uint32(0))

	// Set a valid subscript to an invalid length
	err = value.SetElemLenUsed(tp, 0, 100)
	assert.NotNil(t, err)
	r, err = value.ElemLenUsed(tp, 0)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(0))

	// Get a valid length
	err = value.SetElemLenUsed(tp, 0, 50)
	assert.Nil(t, err)
	r, err = value.ElemLenUsed(tp, 0)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(50))
}

func TestBufTAryValStr(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP

	// Test before Alloc
	r, err := value.ValStr(tp, 0)
	assert.NotNil(t, err)
	assert.Nil(t, r)

	value.Alloc(10, 50)

	// Test after alloc, before setting value outside of range
	r, err = value.ValStr(tp, 11)
	assert.NotNil(t, err)
	assert.Nil(t, r)

	// Test after alloc, valid except not defined
	r, err = value.ValStr(tp, 0)
	assert.Nil(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, *r, "")
}

func TestBufTAryElemUsed(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP

	// Test before Alloc
	r := value.ElemUsed()
	assert.Equal(t, r, uint32(0))

	// Test set
	err := value.SetElemUsed(tp, 0)
	assert.Nil(t, err)

	// Alloc, and test
	value.Alloc(10, 50)

	r = value.ElemUsed()
	assert.Equal(t, r, uint32(0))

	err = value.SetElemUsed(tp, 100)
	assert.NotNil(t, err)

	err = value.SetElemUsed(tp, 5)
	assert.Nil(t, err)

	r = value.ElemUsed()
	assert.Equal(t, r, uint32(5))
}

func TestBufferTAryNilRecievers(t *testing.T) {
	var value *yottadb.BufferTArray
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

	test_wrapper(func() { value.Alloc(10, 10) })
	test_wrapper(func() { value.Dump() })
	test_wrapper(func() { value.DumpToWriter(nil) })
	//test_wrapper(func() { value.Free() }) // Free doesn't panic as a nil rec.
	test_wrapper(func() { value.ElemAlloc() })
	test_wrapper(func() { value.ElemLenAlloc(0) })
	test_wrapper(func() { value.ElemLenUsed(tp, 0) })
	test_wrapper(func() { value.ElemUsed() })
	test_wrapper(func() { value.ValBAry(tp, 0) })
	test_wrapper(func() { value.ValStr(tp, 0) })
	test_wrapper(func() { value.SetElemLenUsed(tp, 0, 10) })
	test_wrapper(func() { value.SetElemUsed(tp, 32) })
	test_wrapper(func() { value.SetValBAry(tp, 0, nil) })
	test_wrapper(func() { value.SetValStr(tp, 0, nil) })
	test_wrapper(func() { value.SetValStrLit(tp, 0, "ok") })
	test_wrapper(func() { value.DeleteExclST(tp) })
	test_wrapper(func() { value.TpST(tp, nil, nil, "OK") })
}

func TestBufTAryTpSt2(t *testing.T) {
	var novars yottadb.BufferTArray
	var namelst yottadb.BufferTArray
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errors int

	namelst.Alloc(2, 10) // Need an array of two names not more than 10 bytes
	// Start with clean slate then drive TP transaction
	Dbdeleteall(tptoken, &errors, t)
	//err = novars.TpST(tptoken, TpRtn_cgo(), nil, "BATCH")
	err = novars.TpST2(tptoken, func(tp uint64) int {
		return TestTpRtn(tp, nil)
	}, "BATCH")
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
