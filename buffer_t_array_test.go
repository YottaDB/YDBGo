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
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"lang.yottadb.com/go/yottadb"
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"os"
	"runtime"
	"testing"
	"time"
)

// TestBufTAryDeleteExclST tests the DeleteExclST() method.
func TestBufTAryDeleteExclST(t *testing.T) {
	var namelst yottadb.BufferTArray
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errcode int

	namelst.Alloc(2, 10) // Need an array of two names not more than 10 bytes
	defer namelst.Free()
	// We need to create 4 local variables to test this so do that first (thus also testing KeyT.SetValE()
	err = yottadb.SetValE(tptoken, nil, "I have a value", "var1", []string{"sub1", "sub2"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "I wish I was a value", "var2", []string{})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "I was a value", "var3", []string{"sub1"})
	Assertnoerr(err, t)
	err = yottadb.SetValE(tptoken, nil, "I AM A VALUE", "var4", []string{})
	Assertnoerr(err, t)
	// Now delete var1 and var3 by exclusively keeping var2 and var 4
	err = namelst.SetValStr(tptoken, nil, 0, "var2")
	Assertnoerr(err, t)
	err = namelst.SetValStr(tptoken, nil, 1, "var4")
	Assertnoerr(err, t)
	err = namelst.SetElemUsed(tptoken, nil, 2)
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	Assertnoerr(err, t)
	// OK, delete done, see which vars exist
	_, err = yottadb.ValE(tptoken, nil, "var1", []string{"sub1", "sub2"}) // Expect this var to be gone
	if nil == err {
		t.Errorf("var1 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode {
		t.Error("The ValE() errorcode for val3(\"sub1\",\"sub2\") expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	_, err = yottadb.ValE(tptoken, nil, "var2", []string{})
	if nil != err {
		t.Errorf("var2 not found when it should still exist (if ever existed)")
	}
	_, err = yottadb.ValE(tptoken, nil, "var3", []string{"sub1"})
	if nil == err {
		t.Errorf("var3 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode {
		t.Error("The ValE() errorcode for val3(\"sub1\") expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	_, err = yottadb.ValE(tptoken, nil, "var4", []string{})
	if nil != err {
		t.Errorf("var4 not found when it should still exist (if ever existed)")
	}

	// Delete with an empty list which should clear var2 and var4
	namelst.SetElemUsed(tptoken, nil, 0)
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	Assertnoerr(err, t)
	_, err = yottadb.ValE(tptoken, nil, "var2", []string{})
	errcode = yottadb.ErrorCode(err)
	if nil == err {
		t.Errorf("var2 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode {
		t.Error("The ValE() errorcode for val2 expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
	_, err = yottadb.ValE(tptoken, nil, "var4", []string{})
	if nil == err {
		t.Errorf("var4 found when it should have been deleted (no error occurred when fetched")
	}
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_LVUNDEF != errcode {
		t.Error("The ValE() errorcode for val4 expected to be", yottadb.YDB_ERR_LVUNDEF, "but was", errcode)
	}
}

func TestBufTAryDeleteExclSTErrors(t *testing.T) {
	var namelst yottadb.BufferTArray
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errcode int

	defer namelst.Free()
	namelst.Alloc(yottadb.YDB_MAX_NAMES+1, 64)
	err = namelst.SetElemUsed(tptoken, nil, 1)
	Assertnoerr(err, t)

	// YDB_ERR_INVVARNAME - from passing a bad M name
	err = namelst.SetValStr(tptoken, nil, 0, "1")
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteExclST() errorcode for 1 expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_INVVARNAME - from special variable
	err = namelst.SetValStr(tptoken, nil, 0, "$ZCHSET")
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteExclST() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_INVVARNAME - from passing a global
	err = namelst.SetValStr(tptoken, nil, 0, "^a")
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The DeleteExclST() errorcode for ^a expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	err = namelst.SetValStr(tptoken, nil, 0, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0")
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The DeleteExclST() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_NAMECOUNT2HI
	for i := 0; i < yottadb.YDB_MAX_NAMES+1; i++ {
		err = namelst.SetValStr(tptoken, nil, uint32(i), fmt.Sprintf("a%d", i))
		Assertnoerr(err, t)
	}
	err = namelst.SetElemUsed(tptoken, nil, yottadb.YDB_MAX_NAMES+1)
	Assertnoerr(err, t)
	err = namelst.DeleteExclST(tptoken, nil)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_NAMECOUNT2HI != errcode {
		t.Error("The DeleteExclST() errorcode for node with 32 subscripts expected to be", yottadb.YDB_ERR_NAMECOUNT2HI, "but was", errcode)
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
	value.SetValStr(tp, nil, 0, "Hello")
	value.SetElemUsed(tp, nil, 1)
	value.DumpToWriter(&buf1)
	// BufferTArray dump does not show any info about included buffers, so no asserts for
	//  that; it really only mentions used/available buffers
	assert.Contains(t, buf1.String(), "10")
	assert.Contains(t, buf1.String(), "Hello")

	// Dump from a nil BufferTArray with an INVSTRLEN error
	value.Alloc(1, 0)
	value.SetValStr(tp, nil, 0, "Hello") // this should return an INVSTRLEN error
	value.DumpToWriter(&buf1)
}

func TestBufTAryAlloc(t *testing.T) {
	var value, noalloc_value yottadb.BufferTArray

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

	// Try getting length of non-alloc'd array
	r := value.ElemLenAlloc()
	assert.Equal(t, r, uint32(0))

	r = noalloc_value.ElemLenAlloc()
	assert.Equal(t, r, uint32(0))

	value.Alloc(10, 64)
	r = value.ElemLenAlloc()
	assert.Equal(t, r, uint32(64))

	// Alloc a length of 0 and try to get it
	value.Alloc(0, 64)
	r = value.ElemLenAlloc()
	assert.Equal(t, r, uint32(0))
}

func TestBufTAryBAry(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP
	var errcode int

	v := []byte("Hello")
	toolong := []byte("This byte array is too long")

	// Get value from non-allocd value
	r, err := value.ValBAry(tp, nil, 0)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	assert.Nil(t, r)

	// Alloc, but get value past the end
	value.Alloc(10, 16)
	r, err = value.ValBAry(tp, nil, 11)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	assert.Nil(t, r)

	// Get a valid value with no content
	r, err = value.ValBAry(tp, nil, 0)

	// Get a value with some value

	err = value.SetValBAry(tp, nil, 1, v)
	assert.Nil(t, err)
	r, err = value.ValBAry(tp, nil, 1)
	assert.Nil(t, err)
	assert.Equal(t, r, v)

	// Try set a value on out of bounds element
	err = value.SetValBAry(tp, nil, 11, v)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)

	// Try set a value longer than the buffer
	err = value.SetValBAry(tp, nil, 2, toolong)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	l, err := value.ElemLenUsed(tp, nil, 2)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(0))

	// Try to set a value on a freed structure
	value.Free()
	err = value.SetValBAry(tp, nil, 0, v)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
}

func TestBufTAryElemLenUsed(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP
	var errcode int

	// Test non alloc'd structure
	err := value.SetElemLenUsed(tp, nil, 0, 0)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	r, err := value.ElemLenUsed(tp, nil, 0)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)
	assert.Equal(t, r, uint32(0))

	// Allocate, then test with an element past the end
	value.Alloc(10, 64)

	err = value.SetElemLenUsed(tp, nil, 11, 5)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	r, err = value.ElemLenUsed(tp, nil, 11)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	assert.Equal(t, r, uint32(0))

	// Set a valid subscript to an invalid length
	err = value.SetElemLenUsed(tp, nil, 0, 100)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	r, err = value.ElemLenUsed(tp, nil, 0)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(0))

	// Get a valid length
	err = value.SetElemLenUsed(tp, nil, 0, 50)
	assert.Nil(t, err)
	r, err = value.ElemLenUsed(tp, nil, 0)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(50))
}

func TestBufTAryValStr(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP
	var errcode int

	// Test before Alloc
	r, err := value.ValStr(tp, nil, 0)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	assert.Equal(t, r, "")

	value.Alloc(10, 16)

	// Test after alloc, before setting value outside of range
	r, err = value.ValStr(tp, nil, 11)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
	assert.Equal(t, r, "")

	// Test after alloc, valid except not defined
	r, err = value.ValStr(tp, nil, 0)
	assert.Nil(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, r, "")

	// Set a value and check that it is defined
	err = value.SetValStr(tp, nil, 1, "Hello")
	assert.Nil(t, err)
	r, err = value.ValStr(tp, nil, 1)
	assert.Nil(t, err)
	assert.Equal(t, r, "Hello")

	// Try set a value on out of bounds element
	err = value.SetValStr(tp, nil, 11, "Out of Bounds")
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)

	// Try set a value longer than the buffer
	err = value.SetValStr(tp, nil, 2, "This string is too long")
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	l, err := value.ElemLenUsed(tp, nil, 2)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(0))

	// Try to set a value on a freed structure
	value.Free()
	err = value.SetValStr(tp, nil, 0, "Hello")
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)
}

func TestBufTAryElemUsed(t *testing.T) {
	var value yottadb.BufferTArray
	var tp = yottadb.NOTTP
	var errcode int

	// Test before Alloc
	r := value.ElemUsed()
	assert.Equal(t, uint32(0), r)

	// Test set
	err := value.SetElemUsed(tp, nil, 0)
	assert.Nil(t, err)

	// Alloc, and test
	value.Alloc(10, 50)

	// Test that ElemUsed() is 0 after alloc
	r = value.ElemUsed()
	assert.Equal(t, uint32(0), r)

	// Test that setting ElemUsed > Alloc'ed returns error
	err = value.SetElemUsed(tp, nil, 100)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INSUFFSUBS, errcode)

	// Test that setting ElemUsed < Alloc'ed returns nil
	err = value.SetElemUsed(tp, nil, 5)
	assert.Nil(t, err)

	// Test that ElemUsed is correct after set
	r = value.ElemUsed()
	assert.Equal(t, uint32(5), r)

	// Ensure that we can set the elems used back to 0 after
	//  they had been set to a different value
	err = value.SetElemUsed(tp, nil, 0)
	assert.Nil(t, err)

	r = value.ElemUsed()
	assert.Equal(t, uint32(0), r)
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
	test_wrapper(func() { value.ElemLenAlloc() })
	test_wrapper(func() { value.ElemLenUsed(tp, nil, 0) })
	test_wrapper(func() { value.ElemUsed() })
	test_wrapper(func() { value.ValBAry(tp, nil, 0) })
	test_wrapper(func() { value.ValStr(tp, nil, 0) })
	test_wrapper(func() { value.SetElemLenUsed(tp, nil, 0, 10) })
	test_wrapper(func() { value.SetElemUsed(tp, nil, 32) })
	test_wrapper(func() { value.SetValBAry(tp, nil, 0, nil) })
	test_wrapper(func() { value.SetValStr(tp, nil, 0, "") })
	test_wrapper(func() { value.SetValStr(tp, nil, 0, "ok") })
	test_wrapper(func() { value.DeleteExclST(tp, nil) })
	test_wrapper(func() { value.TpST(tp, nil, nil, "OK") })
}

func TestBufTAryTpSt(t *testing.T) {
	var novars yottadb.BufferTArray
	var namelst yottadb.BufferTArray
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errors int

	namelst.Alloc(2, 10) // Need an array of two names not more than 10 bytes
	// Start with clean slate then drive TP transaction
	Dbdeleteall(tptoken, nil, &errors, t)
	err = novars.TpST(tptoken, nil, func(tp uint64, errstr *yottadb.BufferT) int32 {
		return int32(TestTpRtn(tp, nil, nil))
	}, "BATCH")
	Assertnoerr(err, t)
	// Fetch the two nodes to make sure they are there and have correct values
	val1, err := yottadb.ValE(tptoken, nil, "^Variable1A", []string{"Index0", "Index1", "Index2"})
	Assertnoerr(err, t)
	if "The value of Variable1A" != val1 {
		t.Logf("FAIL - The fetched value of ^Variable1A(\"Index0\",\"Index1\",\"Index2\") was not correct\n")
		t.Logf("       Expected: 'The value of Variable1A', Received: '%s'\n", val1)
		t.Fail()
	}
	val2, err := yottadb.ValE(tptoken, nil, "^Variable2B", []string{"Idx0", "Idx1"})
	Assertnoerr(err, t)
	if "The value of Variable2B" != val2 {
		t.Logf("FAIL - The fetched value of ^Variable2B(\"Idx0\",\"Idx1\") was not correct\n")
		t.Logf("       Expected: 'The value of Variable2B', Received: '%s'\n", val2)
		t.Fail()
	}
}

func TestBufTAryTpNest(t *testing.T) {
	var tproutine func(uint64, *yottadb.BufferT) int32
	nest_limit := 130
	if os.Getenv("real_mach_type") == "armv7l" {
		nest_limit = 20
	}
	nest := 0
	tproutine = func(tptoken uint64, errstr *yottadb.BufferT) int32 {
		if nest < nest_limit {
			nest++
			e := yottadb.TpE(tptoken, nil, tproutine, "BATCH", []string{})
			if nil == e {
				return 0
			}
			return int32(yottadb.ErrorCode(e))
		}
		return 0
	}
	e := tproutine(yottadb.NOTTP, nil)
	// Only expect the ERR_TPTOODEEP if we went all the way down, which we don't on armv7l
	if os.Getenv("real_mach_type") != "armv7l" {
		assert.Equal(t, int32(yottadb.YDB_ERR_TPTOODEEP), e)
	}
}

func TestBufferTArrayFree(t *testing.T) {

	SkipTimedTests(t)
	SkipMemIntensiveTests(t)

	func() {
		var mem_before, mem_after int
		var allocation_size uint32 = 1024 * 1024 * 512
		var buffer [1024 * 1024 * 512]byte

		for i := uint32(0); i < allocation_size; i++ {
			buffer[uint(i)] = byte(i)
		}

		// Note starting memory
		mem_before = GetHeapUsage(t)

		func() {
			var buft yottadb.BufferTArray
			defer buft.Free()
			buft.Alloc(1, allocation_size)
			tt := buffer[:]
			err := buft.SetValBAry(yottadb.NOTTP, nil, 0, tt)
			Assertnoerr(err, t)
		}()
		// Trigger a garbage collection
		runtime.GC()

		// Verify that the difference between start and end is much less than 500MB
		mem_after = GetHeapUsage(t)
		assert.InEpsilon(t, mem_before, mem_after, .2)
	}()
}

func TestBufferTArrayFinalizerCleansCAlloc(t *testing.T) {

	SkipTimedTests(t)
	SkipMemIntensiveTests(t)

	func() {
		var mem_before, mem_after int
		var allocation_size uint32 = 1024 * 1024 * 512
		var buffer [1024 * 1024 * 512]byte

		for i := uint32(0); i < allocation_size; i++ {
			buffer[uint(i)] = byte(i)
		}

		// Note starting memory
		mem_before = GetHeapUsage(t)

		func() {
			var buft yottadb.BufferTArray
			buft.Alloc(1, allocation_size)
			tt := buffer[:]
			err := buft.SetValBAry(yottadb.NOTTP, nil, 0, tt)
			Assertnoerr(err, t)
		}()
		// Trigger a garbage collection
		runtime.GC()

		// It may take a moment for the finalizer to run; sleep for a smidgen
		time.Sleep(time.Millisecond * 100)

		// Verify that the difference between start and end is much less than 500MB
		mem_after = GetHeapUsage(t)
		assert.InEpsilon(t, mem_before, mem_after, .2)
	}()
}
