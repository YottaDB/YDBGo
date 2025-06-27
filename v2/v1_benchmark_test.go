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

	assert "github.com/stretchr/testify/require"

	// Import YDBGo v1 to benchmark EasyAPI and SimpleAPI
	yottadb "lang.yottadb.com/go/yottadb"

	v2 "lang.yottadb.com/go/yottadb/v2"
)

// panicIf panics if err is not nil. For use in tests.
func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func BenchmarkV1Easy(b *testing.B) {
	b.Run("Set", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			err := yottadb.SetValE(yottadb.NOTTP, nil, v2.Randstr(), "var", []string{})
			panicIf(err)
		}
	})
}

// Benchmark repeated creation of a key with a new varname each time.
func smplBenchmarkNewKey(b *testing.B) {
	for b.Loop() {
		var k yottadb.KeyT
		k.Alloc(32, 10, 100)                        // varname length 32, up to 10 subscripts, of up to 100 chars each
		k.Subary.SetElemUsed(yottadb.NOTTP, nil, 0) // just varname; no subscripts
		err := k.Varnm.SetValStr(yottadb.NOTTP, nil, v2.Randstr())
		panicIf(err)
		k.Free()
	}
}

// Benchmark setting a node repeatedly to set new values each time
func smplBenchmarkSet(b *testing.B) {
	// Set up database key named `var`
	var k yottadb.KeyT
	k.Alloc(100, 10, 100) // varname length 100, up to 10 subscripts, of up to 100 chars each
	defer k.Free()
	k.Subary.SetElemUsed(yottadb.NOTTP, nil, 0) // just varname; no subscripts
	err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "var")
	panicIf(err)

	// Set up buffer for value to store into `var`
	var value yottadb.BufferT
	value.Alloc(100) // allow a value of up to this many chars
	defer value.Free()

	// Iterate the SET command to benchmark it
	for b.Loop() {
		err = value.SetValStr(yottadb.NOTTP, nil, v2.Randstr())
		panicIf(err)
		err = k.SetValST(yottadb.NOTTP, nil, &value)
		panicIf(err)
	}
}

// Benchmark getting a node repeatedly
func smplBenchmarkGet(b *testing.B) {
	// Set up database key named `var`
	var k yottadb.KeyT
	k.Alloc(100, 10, 100) // varname length 100, up to 10 subscripts, of up to 100 chars each
	defer k.Free()
	k.Subary.SetElemUsed(yottadb.NOTTP, nil, 0) // just varname; no subscripts
	err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "var")
	panicIf(err)

	// Set up buffer for sent and received value
	var value yottadb.BufferT
	value.Alloc(100) // allow a value of up to this many chars
	defer value.Free()
	err = value.SetValStr(yottadb.NOTTP, nil, "12345678") // Store something into it so we can get it back
	panicIf(err)
	err = k.SetValST(yottadb.NOTTP, nil, &value)
	panicIf(err)

	// Iterate the GET command to benchmark it
	for b.Loop() {
		err = k.ValST(yottadb.NOTTP, nil, &value)
		panicIf(err)
		_, err = value.ValStr(yottadb.NOTTP, nil)
		panicIf(err)
	}
}

// Benchmark setting a randomly located node, where each node has 5 random subscripts.
// Since the benchmark always does 5-subscript nodes of known length, it could preallocate these
// and speed up the benchmark. However, not pre-allocating them simulates a real application
// which will create nodes with an arbitrary number of subscripts, each of arbitrary length.
func smplBenchmarkSetVariantSubscripts(b *testing.B) {
	// Set up buffer for value to store into `var`
	var value yottadb.BufferT
	value.Alloc(100) // allow a value of up to this many chars
	defer value.Free()

	v2.RandstrReset() // access the same nodes to be subsequently fetched by matching Get() benchmark
	const nSubs = 5
	for b.Loop() {
		var k yottadb.KeyT
		k.Alloc(100, nSubs, 100)
		// Store varname
		err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "var")
		panicIf(err)
		// Store each subscript
		subs := k.Subary
		for j := range nSubs {
			sub := v2.Randstr()
			err := subs.SetValStr(yottadb.NOTTP, nil, uint32(j), sub)
			panicIf(err)
		}
		subs.SetElemUsed(yottadb.NOTTP, nil, nSubs)

		err = value.SetValStr(yottadb.NOTTP, nil, v2.Randstr())
		panicIf(err)
		err = k.SetValST(yottadb.NOTTP, nil, &value)
		panicIf(err)

		k.Free()
	}
}

// Benchmark getting a randomly located node, where each node has 5 random subscripts.
func smplBenchmarkGetVariantSubscripts(b *testing.B) {
	// Set up buffer for value to store into `var`
	var value yottadb.BufferT
	value.Alloc(100) // allow a value of up to this many chars
	defer value.Free()
	const nSubs = 5

	v2.RandstrReset() // access the same nodes previously stored by matching Set() benchmark
	// set up database locals to Get shortly
	for range b.N {
		var k yottadb.KeyT
		k.Alloc(100, nSubs, 100)
		// Store varname
		err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "var")
		panicIf(err)
		// Store each subscript
		subs := k.Subary
		for j := range nSubs {
			sub := v2.Randstr()
			err := subs.SetValStr(yottadb.NOTTP, nil, uint32(j), sub)
			panicIf(err)
		}
		subs.SetElemUsed(yottadb.NOTTP, nil, nSubs)

		err = value.SetValStr(yottadb.NOTTP, nil, v2.Randstr())
		panicIf(err)
		err = k.SetValST(yottadb.NOTTP, nil, &value)
		panicIf(err)

		k.Free()
	}
	b.ResetTimer()

	v2.RandstrReset() // access the same nodes previously stored by matching Set() benchmark
	for range b.N {
		var k yottadb.KeyT
		// In this case we could re-use the allocation each loop, but the purpose of this
		// benchmark is to simulate allocation of new arbitrary nodes like in a real application
		k.Alloc(100, nSubs, 100)
		// Store varname
		err := k.Varnm.SetValStr(yottadb.NOTTP, nil, "var")
		panicIf(err)
		// Store each subscript
		subs := k.Subary
		for j := range nSubs {
			sub := v2.Randstr()
			err := subs.SetValStr(yottadb.NOTTP, nil, uint32(j), sub)
			panicIf(err)
		}
		subs.SetElemUsed(yottadb.NOTTP, nil, nSubs)

		v2.Randstr() // increment random string index to match strings with Set() benchmark
		err = k.ValST(yottadb.NOTTP, nil, &value)
		assert.Nil(b, err, "Database locals not properly set up for this test")
		_, err = value.ValStr(yottadb.NOTTP, nil)
		panicIf(err)

		k.Free()
	}
}

func smplBenchmarkStr2Zwr(b *testing.B) {
	// Set up output buffer
	var out yottadb.BufferT
	out.Alloc(100)
	defer out.Free()

	// Set up value to convert into ZWR format
	var in yottadb.BufferT
	in.Alloc(100)
	defer in.Free()
	err := in.SetValStr(yottadb.NOTTP, nil, `"X"_$C(0)_"ABC"`)
	if err != nil {
		panic(err)
	}

	// Iterate the command to benchmark it
	for b.Loop() {
		err = in.Str2ZwrST(yottadb.NOTTP, nil, &out)
		panicIf(err)
		_, err = out.ValStr(yottadb.NOTTP, nil)
		panicIf(err)
	}
}

func BenchmarkV1Simple(b *testing.B) {
	b.Run("NewKey", smplBenchmarkNewKey)
	b.Run("Set", smplBenchmarkSet)
	b.Run("SetVariantSubscripts", smplBenchmarkSetVariantSubscripts)
	b.Run("Get", smplBenchmarkGet)
	b.Run("GetVariantSubscripts", smplBenchmarkGetVariantSubscripts)
	b.Run("smplStr2Zwr", smplBenchmarkStr2Zwr)
}
