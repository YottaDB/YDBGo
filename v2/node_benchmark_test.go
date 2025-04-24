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
	"os"
	"runtime"
	"sync/atomic"
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Benchmarks

// Retain results of BenchmarkDiff for printing by _testMain()
var pathA, pathB atomic.Int64
var cpuIndex atomic.Int64

// Benchmark difference between two separate functions running in parallel.
// This was envisaged to diminish the effects of CPU warm-up, but it's not accurate enough.
// For best results run it with perflock and use test flag: -benchtime=5s
// Then it will typically have about 1% variance
func BenchmarkDiff(b *testing.B) {
	if testing.Short() {
		b.Skip()
	}

	var cpus int = -1
	pathA.Store(0)
	pathB.Store(0)
	cpuIndex.Store(0)
	for _, arg := range os.Args {
		fmt.Sscanf(arg, "-test.cpu=%d", &cpus)
	}
	if cpus == -1 {
		cpus = int(runtime.NumCPU()) // Go sets -cpu to this by default, so we should, too
	}
	if cpus == -1 || cpus%2 != 0 {
		panic("You must set test flag: -cpu=<even number>, preferably large like 100")
	}

	b.RunParallel(func(pb *testing.PB) {
		i := cpuIndex.Add(1)
		for int(cpuIndex.Load()) < cpus {
		}
		tconn := SetupTest(b)
		for pb.Next() {
			if i%2 == 0 {
				tconn.Node(Randstr())
				pathA.Add(1)
			} else {
				tconn.Node(Randstr())
				pathB.Add(1)
			}
		}
	})
}

// Benchmark setting a node repeatedly to new values each time.
func BenchmarkNode(b *testing.B) {
	tconn := SetupTest(b)
	for b.Loop() {
		tconn.Node(Randstr())
	}
}

// Benchmark setting a node repeatedly to new values each time.
func BenchmarkSet(b *testing.B) {
	tconn := SetupTest(b)
	n := tconn.Node("var")
	for b.Loop() {
		n.Set(Randstr())
	}
}

// Benchmark getting a node repeatedly.
func BenchmarkGet(b *testing.B) {
	tconn := SetupTest(b)
	n := tconn.Node("var")
	for b.Loop() {
		n.Get()
	}
}

// Benchmark setting a node with randomly located node, where each node has 5 random subscripts.
func BenchmarkSetVariantSubscripts(b *testing.B) {
	tconn := SetupTest(b)
	subs := make([]string, 5)
	RandstrReset() // access the same nodes to be subsequently fetched by matching Get() benchmark
	for b.Loop() {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := tconn.Node("var", subs...)
		n.Set(Randstr())
	}
}

// Benchmark getting a node with randomly located node, where each node has 5 random subscripts.
func BenchmarkGetVariantSubscripts(b *testing.B) {
	tconn := SetupTest(b)
	subs := make([]string, 5)

	// set up database locals to Get shortly
	RandstrReset() // access the same nodes to be subsequently fetched by matching Get() benchmark
	for range b.N {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := tconn.Node("var", subs...)
		n.Set(Randstr())
	}
	b.ResetTimer()

	RandstrReset() // access the same nodes previously stored by matching Set() benchmark
	for range b.N {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := tconn.Node("var", subs...)
		Randstr() // increment random string index to match strings with Set() benchmark
		_, err := n.GetIf()
		assert.Nil(b, err, "Database locals not properly set up for this test")
	}
}

func BenchmarkZwr2Str(b *testing.B) {
	tconn := SetupTest(b)
	str := `"X"_$C(0)_"ABC"`
	for b.Loop() {
		_, err := tconn.Zwr2Str(str)
		assert.Nil(b, err)
	}
}
