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
	"strconv"
	"testing"

	assert "github.com/stretchr/testify/require"
	v1 "lang.yottadb.com/go/yottadb"
)

// ---- Benchmarks

// Benchmark calling M numerical Sum repeatedly
func BenchmarkMCallVoid(b *testing.B) {
	conn := SetupTest(b)
	funcs, err := conn.Import(`Noop: noop^arithmetic()`)
	panicIf(err)
	noop := funcs.Wrap("Noop")

	for range b.N {
		noop()
	}
}

func BenchmarkMCallArgs(b *testing.B) {
	conn := SetupTest(b)
	funcs, err := conn.Import(`Add: int64 add^arithmetic(int, int)`)
	panicIf(err)
	add := funcs.Wrap("Add")

	for i := range b.N {
		retval := add(i, i*2).(int64)
		if retval != int64(i*3) {
			assert.Equal(b, int64(i*3), retval)
		}
	}
}

func BenchmarkV1MCallVoid(b *testing.B) {
	var errstr v1.BufferT
	table, err := v1.CallMTableOpenT(v1.NOTTP, &errstr, "test/calltab.ci")
	panicIf(err)
	_, err = table.CallMTableSwitchT(v1.NOTTP, &errstr)
	panicIf(err)

	for range b.N {
		_, err := v1.CallMT(v1.NOTTP, nil, 0, "Noop")
		panicIf(err)
	}
}

func BenchmarkV1MCallArgs(b *testing.B) {
	var errstr v1.BufferT
	table, err := v1.CallMTableOpenT(v1.NOTTP, &errstr, "test/calltab.ci")
	panicIf(err)
	_, err = table.CallMTableSwitchT(v1.NOTTP, &errstr)
	panicIf(err)

	for i := range b.N {
		retval, err := v1.CallMT(v1.NOTTP, nil, 1024, "Add", i, i*2)
		panicIf(err)
		retnum, err := strconv.Atoi(retval)
		panicIf(err)
		if retnum != i*3 {
			assert.Equal(b, strconv.Itoa(i*3), retval)
		}
	}
}
