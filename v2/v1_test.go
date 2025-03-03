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

// Benchmarks for YDBGo v1 (for comparison with v2).

package yottadb_test

import (
	"testing"
)

// Import YDBGo v1 to benchmark EasyAPI and SimpleAPI
import yottadb "lang.yottadb.com/go/yottadb"
import v2 "lang.yottadb.com/go/yottadb/v2"

func BenchmarkEasy(b *testing.B) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			err := yottadb.SetValE(yottadb.NOTTP, nil, v2.Randstr(), "x", []string{})
			if err != nil {
				panic(err)
			}
		}
	})
}

// Benchmark Setting a node repeatedly to new values each time.
func smplBenchmarkSet(b *testing.B) {
	// Set up database key named `x`
	var k yottadb.KeyT
	k.Alloc(100, 10, 100) // varname length 100, up to 10 subscripts, of up to 100 chars each
	defer k.Free()
	k.Subary.SetElemUsed(yottadb.NOTTP, nil, 0) // just varname; no subscripts
	err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "x")
	if err != nil {
		panic(err)
	}

	// Set up buffer for value to store into `x`
	var value yottadb.BufferT
	value.Alloc(100) // allow a value of up to this many chars
	defer value.Free()

	// Iterate the SET command to benchmark it
	for i := 0; b.Loop(); i++ {
		err = value.SetValStr(yottadb.NOTTP, nil, v2.Randstr())
		if err != nil {
			panic(err)
		}
		err = k.SetValST(yottadb.NOTTP, nil, &value)
		if err != nil {
			panic(err)
		}
	}
}

// Benchmark Setting a node with randomly located node, where each node has 5 random subscripts.
func smplBenchmarkSetVariantSubscripts(b *testing.B) {
	// Set up buffer for value to store into `x`
	var value yottadb.BufferT
	value.Alloc(100) // allow a value of up to this many chars
	defer value.Free()

	const nSubs = 5
	for i := 0; b.Loop(); i++ {
		var k yottadb.KeyT
		k.Alloc(100, nSubs, 100)
		// Store varname
		err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "var")
		if err != nil {
			panic(err)
		}
		// Store each subscript
		subs := k.Subary
		for j := range nSubs {
			sub := v2.Randstr()
			err := subs.SetValStr(yottadb.NOTTP, nil, uint32(j), sub)
			if err != nil {
				panic(err)
			}
		}
		subs.SetElemUsed(yottadb.NOTTP, nil, nSubs)

		err = value.SetValStr(yottadb.NOTTP, nil, v2.Randstr())
		if err != nil {
			panic(err)
		}
		err = k.SetValST(yottadb.NOTTP, nil, &value)
		if err != nil {
			panic(err)
		}

		k.Free()
	}
}

func BenchmarkSmpl(b *testing.B) {
	b.Run("Set", smplBenchmarkSet)
	b.Run("SetVariantSubscripts", smplBenchmarkSetVariantSubscripts)
}
