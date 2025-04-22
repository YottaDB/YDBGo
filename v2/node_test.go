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
	"strings"
	"sync/atomic"
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Examples (testable) and tests

// Example of converting a ZWRITE-formatted string to a Go string
func ExampleConn_Zwr2Str() {
	conn := NewConn()
	str, err := conn.Zwr2Str(`"X"_$C(0)_"ABC"`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v", str)
	// Output: "X\x00ABC"
}

// Example of converting a Go string to a ZWRITE-formatted string
func ExampleConn_Str2Zwr() {
	conn := NewConn()
	str, err := conn.Str2Zwr("X\x00ABC")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v", str)
	// Output: "X"_$C(0)_"ABC"
}

// Example of viewing a Node instance as a string.
func ExampleNode_String() {
	conn := NewConn()
	n := conn.Node("var", "sub1", "sub2")
	numsubs := conn.Node("var2", "1", "2")
	fmt.Println(n)
	fmt.Println(numsubs)
	// Output:
	// var("sub1","sub2")
	// var2(1,2)
}

// Example of traversing a database tree
func ExampleNode_TreeNext() {
	conn := NewConn()
	n1 := conn.Node("X", "1")
	n1.Child("2", "3").Set("123")
	n1.Child("2", "3", "7").Set("1237")
	n1.Child("2", "4").Set("124")
	n1.Child("2", "5", "9").Set("1259")
	n1.Child("6").Set("16")
	nb := conn.Node("X", "B")
	nb.Child("1").Set("AB")

	x := conn.Node("X")
	for {
		x = x.TreeNext()
		if x == nil {
			break
		}
		fmt.Printf("%s=%s\n", x, Quote(x.Get("")))
	}
	// Output:
	// X(1,2,3)=123
	// X(1,2,3,7)=1237
	// X(1,2,4)=124
	// X(1,2,5,9)=1259
	// X(1,6)=16
	// X("B",1)="AB"
}

// Test cases that ExampleNode_TreeNext() did not catch
func TestTreeNext(t *testing.T) {
	tconn := NewConn()

	// Ensure TreeNext will work even if it has to allocate new subscript memory up to the size of YDB_MAX_STR
	bigstring := strings.Repeat("A", YDB_MAX_STR)
	n1 := tconn.Node("X", bigstring)
	n1.Child("2", "3").Set("Big23")
	n1.Child("5", bigstring).Set("Big5Big")

	x := tconn.Node("X")
	output := ""
	for {
		x = x.TreeNext()
		if x == nil {
			break
		}
		output += fmt.Sprintf("%s=%s ", x, Quote(x.Get("")))
	}
	assert.Equal(t, `X("`+bigstring+`",2,3)="Big23" X("`+bigstring+`",5,"`+bigstring+`")="Big5Big" `, output)
}

// Test Node creation.
func TestNode(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("var", "sub1", "sub2")
	assert.Equal(t, `var("sub1","sub2")`, fmt.Sprintf("%v", n))
	n2 := n.Child("sub3", "sub4")
	assert.Equal(t, `var("sub1","sub2","sub3","sub4")`, fmt.Sprintf("%v", n2))
}

func TestKillLocalsExcept(t *testing.T) {
	tconn := SetupTest(t)
	n1 := tconn.Node("var1")
	n2 := tconn.Node("var2")
	n3 := tconn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(1, 1, 11), multi(n1.Data(), n2.Data(), n3.Data()))
	tconn.KillLocalsExcept("var1", "var3")
	assert.Equal(t, multi(1, 0, 11), multi(n1.Data(), n2.Data(), n3.Data()))
	tconn.KillLocalsExcept()
	assert.Equal(t, multi(0, 0, 0), multi(n1.Data(), n2.Data(), n3.Data()))
}

func TestSetGet(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("var")
	val, err := n.GetIf()
	assert.Equal(t, "", val)
	assert.NotNil(t, err)
	assert.Equal(t, "", n.Get())
	assert.Equal(t, "default", n.Get("default"))

	assert.Equal(t, "value", n.Set("value"))
	assert.Equal(t, "value", n.Get())
	assert.Equal(t, "value", n.Get("default"))
}

func TestData(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("var")
	assert.Equal(t, 0, n.Data())
	assert.Equal(t, true, n.HasNone())
	assert.Equal(t, false, n.HasValue())
	assert.Equal(t, false, n.HasTree())
	assert.Equal(t, false, n.HasTreeAndValue())

	n.Set("value")
	assert.Equal(t, 1, n.Data())
	assert.Equal(t, false, n.HasNone())
	assert.Equal(t, true, n.HasValue())
	assert.Equal(t, false, n.HasTree())
	assert.Equal(t, false, n.HasTreeAndValue())

	n.Child("sub1", "sub2").Set("valsub2")
	assert.Equal(t, 11, n.Data())
	assert.Equal(t, false, n.HasNone())
	assert.Equal(t, true, n.HasValue())
	assert.Equal(t, true, n.HasTree())
	assert.Equal(t, true, n.HasTreeAndValue())

	n2 := n.Child("sub1")
	assert.Equal(t, 10, n2.Data())
	assert.Equal(t, false, n2.HasNone())
	assert.Equal(t, false, n2.HasValue())
	assert.Equal(t, true, n2.HasTree())
	assert.Equal(t, false, n2.HasTreeAndValue())
}

func TestKill(t *testing.T) {
	tconn := SetupTest(t)
	n1 := tconn.Node("var1")
	n2 := tconn.Node("var2")
	n3 := tconn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(1, 1, 11), multi(n1.Data(), n2.Data(), n3.Data()))
	n2.Kill()
	assert.Equal(t, multi(1, 0, 11), multi(n1.Data(), n2.Data(), n3.Data()))
	n3.Kill()
	assert.Equal(t, multi(1, 0, 0), multi(n1.Data(), n2.Data(), n3.Data()))
}

func TestClear(t *testing.T) {
	tconn := SetupTest(t)
	n1 := tconn.Node("var1")
	n2 := tconn.Node("var2")
	n3 := tconn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(1, 1, 11), multi(n1.Data(), n2.Data(), n3.Data()))
	n2.Clear()
	assert.Equal(t, multi(1, 0, 11), multi(n1.Data(), n2.Data(), n3.Data()))
	n3.Clear()
	assert.Equal(t, multi(1, 0, 10), multi(n1.Data(), n2.Data(), n3.Data()))
	n3.Child("sub1").Clear()
	assert.Equal(t, multi(1, 0, 0), multi(n1.Data(), n2.Data(), n3.Data()))
}

func TestIncr(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("var")
	assert.Equal(t, 1.0, n.Incr())
	assert.Equal(t, "1", n.Get())
	assert.Equal(t, 3.0, n.Incr(2))
	assert.Equal(t, 4.5, n.Incr(1.5))
	assert.Equal(t, 0.0, n.Incr(-4.5))
	assert.Equal(t, -4.5, n.Incr(-4.5))

	n.Set("0")
	assert.Equal(t, 1.0, n.Incr(""))
	assert.Equal(t, "1", n.Get())
	assert.Equal(t, 3.0, n.Incr("2"))
	assert.Equal(t, 4.5, n.Incr("1.5"))
	assert.Equal(t, 0.0, n.Incr("-4.5"))
	assert.Equal(t, -4.5, n.Incr("-4.5"))
	assert.Equal(t, -3.5, n.Incr("1abcdefg"))
}

func TestLock(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("^var", "Don't", "Panic!")
	// Increment lock 3 times
	assert.Equal(t, true, n.Grab(0.1))
	assert.Equal(t, true, n.Grab(0.1))
	assert.Equal(t, true, n.Grab(0.1))

	// Check that lock now exists
	lockpath := fmt.Sprint(n)
	assert.Equal(t, true, lockExists(lockpath))

	// Decrement 3 times and each time check whether lock exists
	n.Release()
	assert.Equal(t, true, lockExists(lockpath))
	n.Release()
	assert.Equal(t, true, lockExists(lockpath))
	n.Release()
	assert.Equal(t, false, lockExists(lockpath))

	// Now lock two paths and check that Lock(0) releases them
	n2 := tconn.Node("^var2")
	n.Grab()
	n2.Grab()
	assert.Equal(t, true, lockExists(fmt.Sprint(n)))
	assert.Equal(t, true, lockExists(fmt.Sprint(n2)))
	assert.Equal(t, true, tconn.Lock(0)) // Release all locks
	assert.Equal(t, false, lockExists(fmt.Sprint(n)))
	assert.Equal(t, false, lockExists(fmt.Sprint(n2)))

	// Now lock both using Lock() and make sure they get locked and unlocked
	assert.Equal(t, true, tconn.Lock(0.1, n, n2)) // Release all locks
	assert.Equal(t, true, lockExists(fmt.Sprint(n)))
	assert.Equal(t, true, lockExists(fmt.Sprint(n2)))
	assert.Equal(t, true, tconn.Lock(0)) // Release all locks
	assert.Equal(t, false, lockExists(fmt.Sprint(n)))
	assert.Equal(t, false, lockExists(fmt.Sprint(n2)))
}

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
