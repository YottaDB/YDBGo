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
)

// ---- Examples (testable)

// Example of viewing a Node instance as a string.
func ExampleNode_String() {
	n := NewConn().Node("var", "sub1", "sub2")
	fmt.Println(n)
	// Output: var("sub1")("sub2")
}

// ---- Tests

// Test Node creation.
func TestNode(t *testing.T) {
	t.Run("String", func(t *testing.T) {
		n := NewConn().Node("var", "sub1", "sub2")
		ans := fmt.Sprintf("%v", n)
		expect := "var(\"sub1\")(\"sub2\")"
		if ans != expect {
			t.Errorf("got %s, want %s", ans, expect)
		}
	})
}

func TestGetSet(t *testing.T) {
	n := conn.Node("var") // or: db.New("varname", "sub1", "sub2")
	assert(n.Set(Randstr()))
}

// ---- Benchmarks

// Benchmark Setting a node repeatedly to new values each time.
func benchmarkSet(b *testing.B) {
	n := conn.Node("var") // or: db.New("varname", "sub1", "sub2")
	for i := 0; b.Loop(); i++ {
		assert(n.Set(Randstr()))
	}
}

// Benchmark Setting a node with randomly located node, where each node has 5 random subscripts.
func benchmarkSetVariantSubscripts(b *testing.B) {
	subs := make([]string, 5)
	for i := 0; b.Loop(); i++ {
		for j := range subs {
			subs[j] = Randstr()
		}
		n := conn.Node("var", subs...)
		assert(n.Set(Randstr()))
	}
}

// Run all Node benchmarks.
func BenchmarkNode(b *testing.B) {
	b.Run("Set", benchmarkSet)
	b.Run("SetVariantSubscripts", benchmarkSetVariantSubscripts)
}
