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
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TestStr2ZwrSTAndZwr2StrST tests the Str2ZwrST() and Zwr2StrST() methods
func TestStr2ZwrSTAndZwr2StrST(t *testing.T) {
	var ovalue, cvalue, noalloc_value yottadb.BufferT
	var outstr string
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var errcode int

	defer ovalue.Free()
	ovalue.Alloc(64)
	defer cvalue.Free()
	cvalue.Alloc(128)
	origstr := "This\tis\ta\ttest\tstring"
	if DebugFlag {
		fmt.Println("Original string unmodified:  ", origstr)
	}
	err = ovalue.SetValStr(tptoken, nil, origstr)
	Assertnoerr(err, t)
	err = ovalue.Str2ZwrST(tptoken, nil, &cvalue)
	Assertnoerr(err, t)
	outstr, err = cvalue.ValStr(tptoken, nil)
	Assertnoerr(err, t)
	if DebugFlag {
		t.Log("Str2ZwrS modified string:    ", outstr)
	}
	err = cvalue.Zwr2StrST(tptoken, nil, &ovalue)
	Assertnoerr(err, t)
	outstr, err = ovalue.ValStr(tptoken, nil)
	Assertnoerr(err, t)
	if DebugFlag {
		t.Log("Zwr2StrS re-modified string: ", outstr)
	}
	if outstr != origstr {
		t.Log("  Re-modified string should be same as original string but is not")
		t.Log("  Original string:", origstr)
		t.Log("  Modified string:", outstr)
		t.Fail()
	}
	// Try calling on a non-allocated value
	err = noalloc_value.Zwr2StrST(tptoken, nil, &cvalue)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	// Test Str2ZwrST with an allocated value in the second param
	err = ovalue.Str2ZwrST(tptoken, nil, &noalloc_value)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	err = ovalue.Zwr2StrST(tptoken, nil, &noalloc_value)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	// Test with nil as the second argument
	(func() {
		defer (func() {
			recover()
		})()
		err = ovalue.Str2ZwrST(tptoken, nil, nil)
	})()
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	(func() {
		defer (func() {
			recover()
		})()
		err = ovalue.Zwr2StrST(tptoken, nil, nil)
	})()
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)
}

func TestLenAlloc(t *testing.T) {
	var value yottadb.BufferT
	var errcode int

	defer value.Free()

	// Test return before Alloc() should return error
	len, err := value.LenAlloc(yottadb.NOTTP, nil)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	value.Alloc(128)

	len, err = value.LenAlloc(yottadb.NOTTP, nil)
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
	var errcode int

	_, err = value.LenAlloc(yottadb.NOTTP, nil)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

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
	err = ovalue.SetValStr(tptoken, nil, origstr)
	assert.Nil(t, err)

	// Try allocating again
	ovalue.Alloc(64)

	err = ovalue.SetValStr(tptoken, nil, origstr)
	assert.Nil(t, err)

	// Try setting a buffer, reallocating to a smaller size
	ovalue.Alloc(10)
	err = ovalue.SetValStr(tptoken, nil, "Hello")
	assert.Nil(t, err)
	ovalue.Alloc(3)
	str, err := ovalue.ValStr(tptoken, nil)
	assert.Nil(t, err)
	assert.Equal(t, "", str)

	// Alloc BufferT var1; Copy to another BufferT var2; Free copy var2; Alloc new BufferT var1
	for i := 0; i < 10; i++ {
		var prev yottadb.BufferT

		value.Alloc(uint32(i))
		// Randomly choose to set a string literal value to the allocated buffer
		if 0 != rand.Intn(2) {
			err = value.SetValStr(tptoken, nil, "Hello")
			if i < 5 {
				assert.Equal(t, yottadb.ErrorCode(err), yottadb.YDB_ERR_INVSTRLEN)
			} else {
				assert.Nil(t, err)
			}
		}
		prev = value
		prev.Free()
	}
}

func TestLen(t *testing.T) {
	var value, noalloc_value yottadb.BufferT
	var length = uint32(128)
	var errcode int

	// Test before Alloc()
	l, err := value.LenUsed(yottadb.NOTTP, nil)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	value.Alloc(length)

	l, err = value.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(0))

	err = value.SetValStr(yottadb.NOTTP, nil, "Hello")
	assert.Nil(t, err)
	l, err = value.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(len("Hello")))

	// SetLenUsed to a valid value
	err = value.SetLenUsed(yottadb.NOTTP, nil, length-2)
	assert.Nil(t, err)
	l, err = value.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, l, length-2)

	// Set len used to an invalid value
	err = value.SetLenUsed(yottadb.NOTTP, nil, length+2)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	r, err := value.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, r, length-2)

	// Try setting length on non-allocated buffer
	err = noalloc_value.SetLenUsed(yottadb.NOTTP, nil, length-2)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)
}

func TestValStr(t *testing.T) {
	var value, value_store yottadb.BufferT
	var global_name = "hello"
	var length = uint32(len(global_name))
	var errcode int

	// Get value before being init'd
	str, err := value.ValStr(yottadb.NOTTP, nil)
	assert.Equal(t, str, "")
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	// Test set before Alloc
	err = value.SetValStr(yottadb.NOTTP, nil, global_name)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	defer value.Free()
	value.Alloc(length + 1)
	defer value_store.Free()
	value_store.Alloc(length - 2)

	// Test that allocated unset buffer will not error when retreived
	str, err = value.ValStr(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, str, "")

	// Test that set works when buffer is properly sized
	err = value.SetValStr(yottadb.NOTTP, nil, global_name)
	assert.Nil(t, err)
	str, err = value.ValStr(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, global_name, str)

	// Test that set returns an error when buffer is not properly sized
	err = value_store.SetValStr(yottadb.NOTTP, nil, global_name)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	str, err = value_store.ValStr(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, str, "")
	r, err := value_store.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(0))
}

func TestValBAry(t *testing.T) {
	var value, value_store yottadb.BufferT
	var global_name = []byte("hello")
	var length = uint32(len(global_name))
	var errcode int

	// Get value before being init'd
	ary, err := value.ValBAry(yottadb.NOTTP, nil)
	assert.Nil(t, ary)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	// Test set before Alloc
	err = value.SetValBAry(yottadb.NOTTP, nil, global_name)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_STRUCTUNALLOCD, errcode)

	defer value.Free()
	value.Alloc(length + 1)
	defer value_store.Free()
	value_store.Alloc(length - 2)

	// Test that allocated unset buffer will not error when retreived
	ary, err = value.ValBAry(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, ary, []byte{})

	// Test that set works when buffer is properly sized
	err = value.SetValBAry(yottadb.NOTTP, nil, global_name)
	assert.Nil(t, err)
	ary, err = value.ValBAry(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, ary, global_name)

	// Test that set returns an error when buffer is not properly sized
	err = value_store.SetValBAry(yottadb.NOTTP, nil, global_name)
	errcode = yottadb.ErrorCode(err)
	assert.Equal(t, yottadb.YDB_ERR_INVSTRLEN, errcode)
	ary, err = value_store.ValBAry(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, ary, []byte{})
	r, err := value_store.LenUsed(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	assert.Equal(t, r, uint32(0))
}

func TestDump(t *testing.T) {
	var value, noalloc_value yottadb.BufferT
	var tp = yottadb.NOTTP
	var buf1 bytes.Buffer

	// Dump from a nil buffer
	noalloc_value.DumpToWriter(&buf1)

	defer value.Free()
	value.Alloc(64)
	value.SetValStr(tp, nil, "Hello")
	value.DumpToWriter(&buf1)
	assert.Contains(t, buf1.String(), "Hello")
	assert.Contains(t, buf1.String(), "64")
	value.Free()

	// Dump from a nil buffer with an INVSTRLEN error
	value.Alloc(0)
	err := value.SetValStr(tp, nil, "Hello") // this should return an INVSTRLEN error
	assert.Equal(t, yottadb.ErrorCode(err), yottadb.YDB_ERR_INVSTRLEN)
	value.DumpToWriter(&buf1)
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
	test_wrapper(func() { value.SetValStr(tp, nil, "") })
	test_wrapper(func() { value.SetValStr(tp, nil, "ok") })
	test_wrapper(func() { value.Str2ZwrST(tp, nil, nil) })
	test_wrapper(func() { value.Zwr2StrST(tp, nil, nil) })
}

func TestBufferTFree(t *testing.T) {
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
			var buft yottadb.BufferT
			defer buft.Free()
			buft.Alloc(allocation_size)
			tt := buffer[:]
			err := buft.SetValBAry(yottadb.NOTTP, nil, tt)
			Assertnoerr(err, t)
		}()
		// Trigger a garbage collection
		runtime.GC()

		// Verify that the difference between start and end is much less than 500MB
		mem_after = GetHeapUsage(t)
		assert.InEpsilon(t, mem_before, mem_after, .2)
	}()
}

func TestBufferTFinalizerCleansCAlloc(t *testing.T) {
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
			var buft yottadb.BufferT
			buft.Alloc(allocation_size)
			tt := buffer[:]
			err := buft.SetValBAry(yottadb.NOTTP, nil, tt)
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

func TestBufferTCopyAndFree(t *testing.T) {
	var buff, buff2 yottadb.BufferT
	var buffp *yottadb.BufferT

	buff.Alloc(1024)
	buff2 = buff
	buff2.Free()
	buff.Free()

	buffp = new(yottadb.BufferT)
	buffp.Alloc(1024)
	buff = *buffp
	buff.Free()
	buffp.Free()
}

func TestBufferTInStruct(t *testing.T) {
	// If this fails, it will fail with a panic
	tptoken := yottadb.NOTTP
	new_buf := func() yottadb.BufferT {
		type myStruct struct {
			buff1, buff2, buff3 yottadb.BufferT
		}
		var s myStruct
		s.buff1.Alloc(1024)
		s.buff2.Alloc(1024)
		s.buff3.Alloc(1024)
		return s.buff1
	}()
	val, err := new_buf.ValStr(tptoken, nil)
	assert.Nil(t, err)
	assert.Equal(t, val, "")
}
