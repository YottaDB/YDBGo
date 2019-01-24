//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2019 YottaDB LLC. and/or its subsidiaries.//
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
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestStr2ZwrSTAndZwr2StrST tests the Str2ZwrST() and Zwr2StrST() methods
func TestStr2ZwrSTAndZwr2StrST(t *testing.T) {
	var ovalue, cvalue, noalloc_value yottadb.BufferT
	var outstrp *string
	var tptoken uint64 = yottadb.NOTTP
	var err error

	defer ovalue.Free()
	ovalue.Alloc(64)
	defer cvalue.Free()
	cvalue.Alloc(128)
	origstr := "This\tis\ta\ttest\tstring"
	if DebugFlag {
		fmt.Println("Original string unmodified:  ", origstr)
	}
	err = ovalue.SetValStr(tptoken, nil, &origstr)
	Assertnoerr(err, t)
	err = ovalue.Str2ZwrST(tptoken, nil, &cvalue)
	Assertnoerr(err, t)
	outstrp, err = cvalue.ValStr(tptoken, nil)
	Assertnoerr(err, t)
	if DebugFlag {
		t.Log("Str2ZwrS modified string:    ", *outstrp)
	}
	err = cvalue.Zwr2StrST(tptoken, nil, &ovalue)
	Assertnoerr(err, t)
	outstrp, err = ovalue.ValStr(tptoken, nil)
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
	// Try calling on a non-allocated value
	err = noalloc_value.Zwr2StrST(tptoken, nil, &cvalue)
	assert.NotNil(t, err)

	// Test Str2ZwrST with an allocated value in the second param
	err = ovalue.Str2ZwrST(tptoken, nil, &noalloc_value)
	assert.NotNil(t, err)

	err = ovalue.Zwr2StrST(tptoken, nil, &noalloc_value)
	assert.NotNil(t, err)

	// Test with nil as the second argument
	(func() {
		defer (func() {
			recover()
		})()
		err = ovalue.Str2ZwrST(tptoken, nil, nil)
	})()
	assert.NotNil(t, err)

	(func() {
		defer (func() {
			recover()
		})()
		err = ovalue.Zwr2StrST(tptoken, nil, nil)
	})()
	assert.NotNil(t, err)
}

func TestLenAlloc(t *testing.T) {
	var value yottadb.BufferT

	defer value.Free()
	value.Alloc(128)

	len, err := value.LenAlloc(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, len, uint32(128))
}

func TestAllocLargeValue(t *testing.T) {
	var value yottadb.BufferT
	var val uint32

	// Skip this test on 32-bit platforms
	if strconv.IntSize == 32 {
		t.Skipf("This test runs out of memory on 32 bit machines; skip")
	}

	val = 1 << 31

	defer value.Free()
	// Try allocating a large value
	value.Alloc(val)

	// Verify that the allocated value is the correct size
	len, err := value.LenAlloc(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, val, len)
}

func TestAlloc(t *testing.T) {
	var ovalue, cvalue, value yottadb.BufferT
	var tptoken uint64 = yottadb.NOTTP
	var err error

	_, err = value.LenAlloc(yottadb.NOTTP, nil)
	assert.NotNil(t, err)

	// Test Free with no Alloc
	value.Free()

	// Test Alloc followed by multiple frees
	value.Alloc(10)
	value.Free()
	value.Free()

	// Test Alloc without free
	defer ovalue.Free()
	ovalue.Alloc(64)
	defer cvalue.Free()
	cvalue.Alloc(128)

	origstr := "helloWorld"
	err = ovalue.SetValStr(tptoken, nil, &origstr)
	assert.Nil(t, err)

	// Try allocating again
	ovalue.Alloc(64)

	err = ovalue.SetValStr(tptoken, nil, &origstr)
	assert.Nil(t, err)

	// Try setting a buffer, reallocating to a smaller size
	ovalue.Alloc(10)
	err = ovalue.SetValStrLit(tptoken, nil, "Hello")
	assert.Nil(t, err)
	ovalue.Alloc(3)
	str, err := ovalue.ValStr(tptoken, nil)
	assert.Nil(t, err)
	assert.Equal(t, "", *str)
}

func TestLen(t *testing.T) {
	var value, noalloc_value yottadb.BufferT
	var length = uint32(128)

	l, err := value.LenUsed(yottadb.NOTTP, nil)
	assert.NotNil(t, err)

	value.Alloc(length)

	l, err = value.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(0))

	err = value.SetValStrLit(yottadb.NOTTP, nil, "Hello")
	assert.Nil(t, err)
	l, err = value.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(len("Hello")))

	// SetLenUsed to a valid value
	err = value.SetLenUsed(yottadb.NOTTP, nil, length-2)
	assert.Nil(t, err)

	// Set len used to an invalid value
	err = value.SetLenUsed(yottadb.NOTTP, nil, length+2)
	assert.NotNil(t, err)

	// Try setting length on non-allocated buffer
	err = noalloc_value.SetLenUsed(yottadb.NOTTP, nil, length-2)
	assert.NotNil(t, err)
}

func TestInvalidAllonLen(t *testing.T) {
	var value yottadb.BufferT
	var global_name = "hello"
	var length = uint32(len(global_name) - 1)
	// Try allocating a small buffer and overfilling

	defer value.Free()
	value.Alloc(length)

	err := value.SetValStr(yottadb.NOTTP, nil, &global_name)
	assert.NotNil(t, err)
}

func TestValStr(t *testing.T) {
	var value, value_store yottadb.BufferT
	var global_name = "hello"
	var length = uint32(len(global_name))

	// Get value before being init'd
	str, err := value.ValStr(yottadb.NOTTP, nil)
	assert.Nil(t, str)
	assert.NotNil(t, err)

	defer value.Free()
	value.Alloc(length + 1)
	defer value_store.Free()
	value_store.Alloc(length - 2)

	err = value.SetValStr(yottadb.NOTTP, nil, &global_name)
	assert.Nil(t, err)

	/*str, err = value.ValStr(yottadb.NOTTP)
	assert.Equal(t, *str, global_name)
	assert.Nil(t, err)*/

	//	std, err = value.
}

func TestValBAry(t *testing.T) {
	var value, noalloc_value yottadb.BufferT
	var tp = yottadb.NOTTP
	var str = "Hello"

	defer value.Free()
	value.Alloc(64)

	err := value.SetValStr(tp, nil, &str)
	assert.Nil(t, err)

	bytes, err := value.ValBAry(tp, nil)
	assert.Nil(t, err)
	assert.Equal(t, *bytes, []byte(str))

	// Try to set value on non-alloc'd value
	err = noalloc_value.SetValBAry(tp, nil, bytes)
	assert.NotNil(t, err)

	// Try to get value on non-alloc'd value
	val, err := noalloc_value.ValBAry(tp, nil)
	assert.NotNil(t, err)
	assert.Nil(t, val)
}

func TestDump(t *testing.T) {
	var value, noalloc_value yottadb.BufferT
	var tp = yottadb.NOTTP
	var buf1 bytes.Buffer

	// Dump from a nil buffer
	noalloc_value.DumpToWriter(&buf1)

	defer value.Free()
	value.Alloc(64)
	value.SetValStrLit(tp, nil, "Hello")
	value.DumpToWriter(&buf1)
	assert.Contains(t, buf1.String(), "Hello")
	assert.Contains(t, buf1.String(), "64")
}

func TestBufferTNilRecievers(t *testing.T) {
	var value *yottadb.BufferT
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

	test_wrapper(func() { value.Alloc(60) })
	test_wrapper(func() { value.Dump() })
	test_wrapper(func() { value.DumpToWriter(nil) })
	//test_wrapper(func() { value.Free() }) // Free won't panic, it'll just chill
	test_wrapper(func() { value.LenAlloc(tp, nil) })
	test_wrapper(func() { value.LenUsed(tp, nil) })
	test_wrapper(func() { value.ValBAry(tp, nil) })
	test_wrapper(func() { value.ValStr(tp, nil) })
	test_wrapper(func() { value.SetLenUsed(tp, nil, 1000) })
	test_wrapper(func() { value.SetValBAry(tp, nil, nil) })
	test_wrapper(func() { value.SetValStr(tp, nil, nil) })
	test_wrapper(func() { value.SetValStrLit(tp, nil, "ok") })
	test_wrapper(func() { value.Str2ZwrST(tp, nil, nil) })
	test_wrapper(func() { value.Zwr2StrST(tp, nil, nil) })
}

func TestBufferTFree(t *testing.T) {
	var mem_before, mem_after int
	var allocation_size uint32 = 1024 * 1024 * 512
	var buffer [1024 * 1024 * 512]byte
	for i := uint32(0); i < allocation_size; i++ {
		buffer[uint(i)] = byte(i)
	}

	// Note starting memory
	mem_before = GetHeapUsage(t)

	func() {
		var buft yottadb.BufferT
		defer buft.Free()
		buft.Alloc(allocation_size)
		tt := buffer[:]
		err := buft.SetValBAry(yottadb.NOTTP, nil, &tt)
		Assertnoerr(err, t)
	}()
	// Trigger a garbage collection
	runtime.GC()

	// Verify that the difference between start and end is much less than 500MB
	mem_after = GetHeapUsage(t)
	assert.InEpsilon(t, mem_before, mem_after, .2)
	fmt.Printf("start: %v end: %v\n", mem_before, mem_after)
}

func TestBufferTFinalizerCleansCAlloc(t *testing.T) {
	var mem_before, mem_after int
	var allocation_size uint32 = 1024 * 1024 * 512
	var buffer [1024 * 1024 * 512]byte
	for i := uint32(0); i < allocation_size; i++ {
		buffer[uint(i)] = byte(i)
	}

	// Note starting memory
	mem_before = GetHeapUsage(t)

	func() {
		var buft yottadb.BufferT
		buft.Alloc(allocation_size)
		tt := buffer[:]
		err := buft.SetValBAry(yottadb.NOTTP, nil, &tt)
		Assertnoerr(err, t)
	}()
	// Trigger a garbage collection
	runtime.GC()

	// It may take a moment for the finalizer to run; sleep for a smidgen
	time.Sleep(time.Millisecond * 100)

	// Verify that the difference between start and end is much less than 500MB
	mem_after = GetHeapUsage(t)
	assert.InEpsilon(t, mem_before, mem_after, .2)
}
