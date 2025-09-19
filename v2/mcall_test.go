//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"fmt"
	"strings"
	"testing"

	assert "github.com/stretchr/testify/require"
	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// ---- Tests

func TestImport(t *testing.T) {
	conn := SetupTest(t)
	funcs := conn.MustImport("AddVerbose: string[1024] addVerbose^arithmetic(string, int64, int64)")
	strType := "ydb_buffer_t"
	if dbHandle.YDBRelease < 1.36 {
		strType = "ydb_string_t"
	}
	assert.Equal(t, fmt.Sprintf("AddVerbose: %s* addVerbose^arithmetic(I:%s*, I:ydb_int64_t*, I:ydb_int64_t*)", strType, strType), funcs.Table.YDBTable)

	// Test helper function to import and return error code
	importer := func(s string) int {
		_, err := conn.Import(s)
		if err == nil {
			return 0
		}
		return err.(*Error).Code
	}

	// Test all the import error conditions
	assert.Equal(t, ydberr.MCallTypeUnhandled, importer("name: name^name(.5long)"))
	assert.Equal(t, ydberr.MCallBadAsterisk, importer("name: name^name(int*)"))
	assert.Equal(t, ydberr.MCallPreallocRequired, importer("name: name^name(*string)"))
	assert.Equal(t, ydberr.MCallPreallocInvalid, importer("name: name^name(int[5])"))
	assert.Equal(t, ydberr.MCallPreallocInvalid, importer("name: name^name(string[5])"))
	assert.Equal(t, ydberr.MCallInvalidPrototype, importer("abc:"))
	assert.Panics(t, func() { conn.MustImport("abc:") }) // check that MustImport panics on error
	assert.Equal(t, ydberr.MCallEntrypointInvalid, importer("name: na.me^name()"))
	assert.Equal(t, ydberr.MCallTypeUnknown, importer("name: int95 name^name()"))
	assert.Equal(t, ydberr.MCallTypeUnknown, importer("name: float32 name^name()")) // unknown *return type* as opposed to unknown of any type tested above
	assert.Equal(t, ydberr.MCallTypeMismatch, importer("name: *int name^name()"))
	assert.Equal(t, ydberr.MCallTypeMissing, importer("name: name^name(int,,int)"))
	assert.Equal(t, ydberr.MCallTooManyParameters, importer("name: name^name(int"+strings.Repeat(",int", YDB_MAX_PARMS)+")"))
	conn.MustImport("test/v2calltab.ci") // test importing from a file
	assert.Equal(t, ydberr.ImportRead, importer("test/v2calltab-does-not-exist.ci"))

	// Test that import removes the temporary call-in file works in both debug modes -- for coverage tests
	originalDebugMode := DebugMode.Load()
	DebugMode.Store(0)
	conn.MustImport("name: name^name()")
	DebugMode.Store(1)
	conn.MustImport("name: name^name()")
	DebugMode.Store(originalDebugMode)
}

func TestCallM(t *testing.T) {
	conn := SetupTest(t)
	assert.PanicsWithError(t, "routine data passed to Conn.CallM() must not be nil", func() { conn.callM(nil, []any{}) })

	table := `
		AddVerbose: string[1024] addVerbose^arithmetic(*string[10], *int, int, string)
		Add: int64 add^arithmetic(int64, int64)
		Sub: int64 sub^arithmetic(int64, int64)
		AddFloat32: float64 add^arithmetic(float32, float32)
		AddFloat: float64 add^arithmetic(float64, float64)
		Noop: noop^arithmetic()
		Param32: string[1024] param32^arithmetic(int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int,int)
	`
	// Declare these in separate imports
	m := conn.MustImport(table)
	assert.PanicsWithError(t, "3 parameters supplied whereas the M-call table specifies 2", func() { m.Call("Add", 3, 4, 5) })

	// Test AddVerbose with pointer and non-pointer types
	s, n := "test", 3
	result := m.Call("AddVerbose", &s, &n, 4, "100").(string)
	assert.Equal(t, ":test:107", result)
	assert.Equal(t, ":test:", s)
	assert.Equal(t, 107, n)
	// Do the same using CallErr()
	s, n = "test", 3
	ret, err := m.CallErr("AddVerbose", &s, &n, 4, "100")
	result = ret.(string)
	assert.Nil(t, err)
	assert.Equal(t, ":test:107", result)
	assert.Equal(t, ":test:", s)
	assert.Equal(t, 107, n)

	// Temporarily set YDBRelease to 1.34 to test that special ydb_string_t handling for that verison works
	prealloc := 10
	func() {
		originalRelease := dbHandle.YDBRelease
		dbHandle.YDBRelease = 1.34
		defer func() { dbHandle.YDBRelease = originalRelease }()
		m := conn.MustImport(fmt.Sprintf("AddVerbose: string[1024] addVerbose^arithmetic(*string[%d], *int, int, string)", prealloc))
		s, n := "test", 3
		result := m.Call("AddVerbose", &s, &n, 4, "100").(string)
		assert.Equal(t, ":test:107", result)
		// Note that in YDB r1.34 the output of an IO parameter is truncated to the length of the input string since ydb_string_t does not contain len_alloc.
		assert.Equal(t, ":tes", s)
		assert.Equal(t, 107, n)
	}()

	// Test CallErr returning an error: create an error by having AddVerbose append to a max-sized string
	bigString := strings.Repeat("A", prealloc)
	ret, err = m.CallErr("AddVerbose", &bigString, &n, 4, "100")
	assert.NotNil(t, err)
	// Test that Wrap panics on error
	assert.Panics(t, func() { m.Wrap("AddVerbose")(&bigString, &n, 4, "100") })
	assert.Panics(t, func() { m.WrapRetInt("AddVerbose")(&bigString, &n, 4, "100") })
	assert.Panics(t, func() { m.WrapRetString("AddVerbose")(&bigString, &n, 4, "100") })
	assert.Panics(t, func() { m.WrapRetFloat("AddVerbose")(&bigString, &n, 4, "100") })
	assert.Panics(t, func() { m.WrapRetFloat("Undefined") })

	// Test code path where all concatenated return parameters may not fit into YDB_MAX_STR
	big1 := "1" + strings.Repeat(" ", YDB_MAX_STR-1)
	big2 := "2" + strings.Repeat(" ", YDB_MAX_STR-1)
	m3 := conn.MustImport("Add: int add^arithmetic(string, string)")
	assert.Equal(t, 3, m3.Call("Add", big1, big2).(int))
	m3 = conn.MustImport(fmt.Sprintf("Add: int add^arithmetic(*string[%d], *string[%d])", YDB_MAX_STR, YDB_MAX_STR))
	assert.Equal(t, 3, m3.Call("Add", &big1, &big2).(int))

	// Test the various arithmetic functions with various types
	assert.Equal(t, -6, conn.MustImport("Add: int add^arithmetic(int, int)").Call("Add", 5, -11).(int))
	assert.Equal(t, -6, conn.MustImport("Add: int add^arithmetic(*int, *int)").Call("Add", &[]int{5}[0], &[]int{-11}[0]).(int))
	assert.Equal(t, 7, conn.MustImport("Add: int add^arithmetic(uint, uint)").Call("Add", uint(5), uint(2)).(int))
	assert.Equal(t, 7, conn.MustImport("Add: int add^arithmetic(*uint, *uint)").Call("Add", &[]uint{5}[0], &[]uint{2}[0]).(int))
	assert.Equal(t, -6, conn.MustImport("Add: int add^arithmetic(int32, int32)").Call("Add", int32(5), int32(-11)).(int))
	assert.Equal(t, -6, conn.MustImport("Add: int add^arithmetic(*int32, *int32)").Call("Add", &[]int32{5}[0], &[]int32{-11}[0]).(int))
	assert.Equal(t, 7, conn.MustImport("Add: int add^arithmetic(uint32, uint32)").Call("Add", uint32(5), uint32(2)).(int))
	assert.Equal(t, 7, conn.MustImport("Add: int add^arithmetic(*uint32, *uint32)").Call("Add", &[]uint32{5}[0], &[]uint32{2}[0]).(int))
	assert.Equal(t, int64(-6), conn.MustImport("Add: int64 add^arithmetic(int64, int64)").Call("Add", int64(5), int64(-11)).(int64))
	assert.Equal(t, int64(-6), conn.MustImport("Add: int64 add^arithmetic(*int64, *int64)").Call("Add", &[]int64{5}[0], &[]int64{-11}[0]).(int64))
	assert.Equal(t, int64(7), conn.MustImport("Add: int64 add^arithmetic(uint64, uint64)").Call("Add", uint64(5), uint64(2)).(int64))
	assert.Equal(t, int64(7), conn.MustImport("Add: int64 add^arithmetic(*uint64, *uint64)").Call("Add", &[]uint64{5}[0], &[]uint64{2}[0]).(int64))
	assert.Equal(t, float64(-6.1), conn.MustImport("Add: float64 add^arithmetic(float32, float32)").Call("Add", float32(5.2), float32(-11.3)).(float64))
	assert.Equal(t, float64(-6.1), conn.MustImport("Add: float64 add^arithmetic(*float32, *float32)").Call("Add", &[]float32{5.2}[0], &[]float32{-11.3}[0]).(float64))
	assert.Equal(t, float64(-6.1), conn.MustImport("Add: float64 add^arithmetic(float64, float64)").Call("Add", float64(5.2), float64(-11.3)).(float64))
	assert.Equal(t, float64(-6.1), conn.MustImport("Add: float64 add^arithmetic(*float64, *float64)").Call("Add", &[]float64{5.2}[0], &[]float64{-11.3}[0]).(float64))
	// Test that passing of an invalid type panics
	assert.Panics(t, func() { conn.MustImport("Add: int add^arithmetic(int, int)").Call("Add", nil, -11) })

	// Test funcs imported from a different table
	m2 := conn.MustImport("Sub: string[10] sub^arithmetic(int32, uint32)")
	assert.Equal(t, "-100", m2.Call("Sub", int32(5), uint32(105)).(string))

	// Make sure calling original table funcs still works even after using other table
	assert.Equal(t, 6.7, m.Call("AddFloat", 5.5, 1.2).(float64))
	assert.Equal(t, 6.7, m.Call("AddFloat32", float32(5.5), float32(1.2)).(float64))
	assert.Equal(t, int64(-6), m.Call("Sub", int64(5), int64(11)).(int64))

	// Test creation of function that do the calling
	addFloat := m.Wrap("AddFloat")
	assert.Equal(t, 6.7, addFloat(5.5, 1.2).(float64))
	add := m.Wrap("Add")
	assert.Equal(t, int64(-6), add(int64(5), int64(-11)).(int64))
	sub := m2.Wrap("Sub")
	assert.Equal(t, "-100", sub(int32(5), uint32(105)).(string))
	// Test WrapRetString
	s, n = "test", 3
	addWrap := m.WrapRetString("AddVerbose")
	result = addWrap(&s, &n, 4, "100")
	assert.Equal(t, ":test:107", result)
	assert.Equal(t, ":test:", s)
	assert.Equal(t, 107, n)
	// Test WrapErr
	s, n = "test", 3
	addWrapErr := m.WrapErr("AddVerbose")
	retAny, err := addWrapErr(&s, &n, 4, "100")
	assert.Nil(t, err)
	result = retAny.(string)
	assert.Equal(t, ":test:107", result)
	assert.Equal(t, ":test:", s)
	assert.Equal(t, 107, n)

	// Test calling a noop and 32-parameter function
	assert.Nil(t, m.Call("Noop"))
	params := m.Call("Param32", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32)
	assert.Equal(t, "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32", params)
}
