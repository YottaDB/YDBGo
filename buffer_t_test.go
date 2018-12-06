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
	"lang.yottadb.com/go/yottadb"
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"testing"
	"github.com/stretchr/testify/assert"
	"bytes"
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

	// TODO: reenable this test after code is fixed
	// Try calling on a non-allocated value
	//err = noalloc_value.Zwr2StrST(tptoken, &cvalue)
	//assert.NotNil(t, err)

	// Test Str2ZwrST with an allocated value in the second param
	err = ovalue.Str2ZwrST(tptoken, &noalloc_value)
	assert.NotNil(t, err)

	err = ovalue.Zwr2StrST(tptoken, &noalloc_value)
	assert.NotNil(t, err)

	t.Skipf("Str2ZwrST needs to check second parameter for nil; currently crashes")
	// Test with nil as the second argument
	err = ovalue.Str2ZwrST(tptoken, nil)
	assert.NotNil(t, err)

	err = ovalue.Zwr2StrST(tptoken, nil)
	assert.NotNil(t, err)
}

func TestLenAlloc(t *testing.T) {
	var value yottadb.BufferT

	defer value.Free()
	value.Alloc(128)

	len, err := value.LenAlloc(yottadb.NOTTP)
	assert.Nil(t, err)
	assert.Equal(t, len, uint32(128))
}

func TestAllocLargeValue(t *testing.T) {
	var value yottadb.BufferT
	var val uint32
	
	val = 1 << 31

	defer value.Free()
	// Try allocating a large value
	value.Alloc(val)

	// Verify that the allocated value is the correct size
	len, err := value.LenAlloc(yottadb.NOTTP)
	assert.Nil(t, err)
	assert.Equal(t, val, len)
}

func TestAlloc(t *testing.T) {
	var ovalue, cvalue, value yottadb.BufferT
	var tptoken uint64 = yottadb.NOTTP
	var err error

	_, err = value.LenAlloc(yottadb.NOTTP)
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
	err = ovalue.SetValStr(tptoken, &origstr)
	assert.Nil(t, err)

	// Try allocating again
	ovalue.Alloc(64)

	err = ovalue.SetValStr(tptoken, &origstr)
	assert.Nil(t, err)

	// Try setting a buffer, reallocating to a smaller size
	ovalue.Alloc(10)
	err = ovalue.SetValStrLit(tptoken, "Hello")
	assert.Nil(t, err)
	ovalue.Alloc(3)
	str, err := ovalue.ValStr(tptoken)
	assert.Nil(t, err)
	assert.Equal(t, "", *str)
}

func TestLen(t *testing.T) {
	var value, noalloc_value yottadb.BufferT
	var length = uint32(128)

	l, err := value.LenUsed(yottadb.NOTTP)
	assert.NotNil(t, err)

	value.Alloc(length)

	l, err = value.LenUsed(yottadb.NOTTP)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(0))

	err = value.SetValStrLit(yottadb.NOTTP, "Hello")
	assert.Nil(t, err)
	l, err = value.LenUsed(yottadb.NOTTP)
	assert.Nil(t, err)
	assert.Equal(t, l, uint32(len("Hello")))

	// SetLenUsed to a valid value
	err = value.SetLenUsed(yottadb.NOTTP, length-2)
	assert.Nil(t, err)

	// Set len used to an invalid value
	err = value.SetLenUsed(yottadb.NOTTP, length+2)
	assert.NotNil(t, err)

	// Try setting length on non-allocated buffer
	err = noalloc_value.SetLenUsed(yottadb.NOTTP, length-2)
	assert.NotNil(t, err)
}

func TestInvalidAllonLen(t *testing.T) {
	var value yottadb.BufferT
	var global_name = "hello"
	var length = uint32(len(global_name) - 1)
	// Try allocating a small buffer and overfilling

	defer value.Free()
	value.Alloc(length)

	err := value.SetValStr(yottadb.NOTTP, &global_name)
	assert.NotNil(t, err)
}

func TestValStr(t *testing.T) {
	var value, value_store yottadb.BufferT
	var global_name = "hello"
	var length = uint32(len(global_name))

	// Get value before being init'd
	str, err := value.ValStr(yottadb.NOTTP)
	assert.Nil(t, str)
	assert.NotNil(t, err)

	defer value.Free()
	value.Alloc(length+1)
	defer value_store.Free()
	value_store.Alloc(length-2)

	err = value.SetValStr(yottadb.NOTTP, &global_name)
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

	err := value.SetValStr(tp, &str)
	assert.Nil(t, err)

	bytes, err := value.ValBAry(tp)
	assert.Nil(t, err)
	assert.Equal(t, *bytes, []byte(str))

	// Try to set value on non-alloc'd value
	err = noalloc_value.SetValBAry(tp, bytes)
	assert.NotNil(t, err)

	// Try to get value on non-alloc'd value
	val, err := noalloc_value.ValBAry(tp)
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
	value.SetValStrLit(tp, "Hello")
	value.DumpToWriter(&buf1)
	assert.Contains(t, buf1.String(), "Hello")
	assert.Contains(t, buf1.String(), "64")
}
