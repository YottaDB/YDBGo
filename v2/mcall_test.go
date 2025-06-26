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
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func setPath(conn *Conn) {
	zroutines := conn.Node("$ZROUTINES")
	zroutines.Set("./test " + zroutines.Get())
}

func TestImport(t *testing.T) {
	conn := SetupTest(t)
	setPath(conn)
	funcs, err := conn.Import("AddVerbose: string[1024] addVerbose^arithmetic(string, int64, int64)")
	if err != nil {
		panic(err)
	}
	strType := "ydb_buffer_t"
	if internalDB.YDBRelease < 1.36 {
		strType = "ydb_string_t"
	}
	assert.Equal(t, fmt.Sprintf("AddVerbose: %s* addVerbose^arithmetic(I:%s*, I:ydb_int64_t*, I:ydb_int64_t*)", strType, strType), funcs.Table.YDBTable)
}

func TestCallM(t *testing.T) {
	table := `
		AddVerbose: string[1024] addVerbose^arithmetic(*string[10], *int, int, string)
		Add: int64 add^arithmetic(int64, int64)
		Sub: int64 sub^arithmetic(int64, int64)
		AddFloat32: float64 add^arithmetic(float32, float32)
		AddFloat: float64 add^arithmetic(float64, float64)`
	conn := SetupTest(t)
	setPath(conn)
	s := "test"
	n := 3
	// Declare these in separate
	m, err := conn.Import(table)
	if err != nil {
		panic(err)
	}
	m2, err := conn.Import("Sub: string[10] sub^arithmetic(int32, uint32)")
	if err != nil {
		panic(err)
	}

	// Test AddVerbose with pointer and non-pointer types
	result := m.Call("AddVerbose", &s, &n, 4, "100").(string)
	assert.Equal(t, "test:107", result)
	assert.Equal(t, "test:", s)
	assert.Equal(t, 107, n)

	// Test other arithmetic functions with various types
	assert.Equal(t, int64(-6), m.Call("Add", int64(5), int64(-11)).(int64))
	assert.Equal(t, int64(-6), m.Call("Sub", int64(5), int64(11)).(int64))

	// Test funcs imported from a different table
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
}
