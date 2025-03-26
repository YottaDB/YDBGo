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
	assert "github.com/stretchr/testify/require"
	"testing"
)

// ---- Examples (testable)

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
	fmt.Println(n)
	// Output: var("sub1")("sub2")
}

// ---- Tests

// Test Node creation.
func TestNode(t *testing.T) {
	tconn := NewConn()
	t.Run("String", func(t *testing.T) {
		n := tconn.Node("var", "sub1", "sub2")
		ans := fmt.Sprintf("%v", n)
		expect := "var(\"sub1\")(\"sub2\")"
		if ans != expect {
			t.Errorf("got %s, want %s", ans, expect)
		}
	})
}

func TestSetGet(t *testing.T) {
	tconn := NewConn()
	n := tconn.Node("var")
	assert.Nil(t, n.Set("value"))
	assert.Equal(t, multi("value", nil), multi(n.Get()))
}

// ---- Benchmarks

// Benchmark setting a node repeatedly to new values each time.
func BenchmarkSet(b *testing.B) {
	tconn := NewConn()
	n := tconn.Node("var")
	for b.Loop() {
		assert.Nil(b, n.Set(Randstr()))
	}
}

// Benchmark getting a node repeatedly.
func BenchmarkGet(b *testing.B) {
	tconn := NewConn()
	n := tconn.Node("var")
	for b.Loop() {
		_, err := n.Get()
		assert.Nil(b, err)
	}
}

// Benchmark setting a node with randomly located node, where each node has 5 random subscripts.
func BenchmarkSetVariantSubscripts(b *testing.B) {
	tconn := NewConn()
	subs := make([]string, 5)
	RandstrReset() // access the same nodes to be subsequently fetched by matching Get() benchmark
	for b.Loop() {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := tconn.Node("var", subs...)
		assert.Nil(b, n.Set(Randstr()))
	}
}

// Benchmark getting a node with randomly located node, where each node has 5 random subscripts.
func BenchmarkGetVariantSubscripts(b *testing.B) {
	tconn := NewConn()
	subs := make([]string, 5)
	RandstrReset() // access the same nodes previously stored by matching Set() benchmark
	for b.Loop() {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := tconn.Node("var", subs...)
		Randstr() // increment random string index to match strings with Set() benchmark
		_, err := n.Get()
		if err != nil {
			assert.Nil(b, err, "Make sure to run the Set benchmark first to init values to read")
		}
	}
}

func BenchmarkZwr2Str(b *testing.B) {
	tconn := NewConn()
	str := `"X"_$C(0)_"ABC"`
	for b.Loop() {
		_, err := tconn.Zwr2Str(str)
		assert.Nil(b, err)
	}
}
