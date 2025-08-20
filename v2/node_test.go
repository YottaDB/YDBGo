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
	"time"
	"unsafe"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func TestNode(t *testing.T) {
	conn := SetupTest(t)
	// Check various input types convert property to subscript strings
	n := conn.Node("var", "str", []byte("bytes"), int(-1), int32(-32), int64(-64), uint(1), uint32(32), uint64(64), float32(32.32), float64(64.64))
	assert.Equal(t, `var("str","bytes",-1,-32,-64,1,32,64,32.32,64.64)`, n.String())
	assert.Panics(t, func() { conn.Node("var", true) })

	// Test that various code paths that panic on programmer errors
	invalidNode := conn.Node("!@#$")
	assert.Panics(t, func() { invalidNode.Set(3) })
	assert.Panics(t, func() { invalidNode.Get() })
	assert.Panics(t, func() { invalidNode.HasValue() })
	assert.Panics(t, func() { invalidNode.Kill() })
	assert.Panics(t, func() { invalidNode.Clear() })
	assert.Panics(t, func() { invalidNode.Incr(10) })
	assert.Panics(t, func() { invalidNode.Next() })
	assert.Panics(t, func() { invalidNode.Lock(1 * time.Millisecond) })
	assert.Panics(t, func() { conn.Lock(1*time.Millisecond, invalidNode) })
	assert.Panics(t, func() { invalidNode.Unlock() })
	assert.Panics(t, func() {
		for range invalidNode.Tree() {
		}
	})
}

func TestConn(t *testing.T) {
	conn1 := SetupTest(t)
	conn2 := NewConn()
	n1 := conn1.Node("$invalidSVN")
	n2 := conn2.Node("var")

	// Produce different errors for conn1 and conn2 and ensure that the latter doesn't clobber the former
	assert.Panics(t, func() { n1.Get() })
	lastErr := n1.Conn.lastError(n1.Conn.lastCode())
	assert.Contains(t, lastErr.Error(), "INVSVN")
	bigString := strings.Repeat("A", YDB_MAX_STR)
	_, lastErr = conn2.Str2Zwr(bigString)
	assert.Contains(t, lastErr.Error(), "MAXSTRLEN")
	// Alter tptoken in n2's conn -- ensure it also does not change the last message that's still stored in conn1
	n2.Conn.tptoken.Add(1)
	assert.NotEqual(t, n1.Conn.tptoken.Load(), n2.Conn.tptoken.Load())

	// Now ensure error buf of n1 has remained unchanged
	lastErr = n1.Conn.lastError(n1.Conn.lastCode())
	assert.Contains(t, lastErr.Error(), "INVSVN")
}

// checkBuffers verifies that all the buffers in n point consecutively into the n's own string storage space
func (n *Node) checkBuffers(t *testing.T) {
	cnode := n.cnode
	lastbuf := bufferIndex(cnode.buffers, int(cnode.len-1))
	start := int(uintptr(unsafe.Pointer(cnode.buffers)))
	end := int(uintptr(unsafe.Pointer(bufferIndex(cnode.buffers, int(cnode.len)))))
	for _, s := range n.Subscripts() {
		end += len(s)
	}
	end += int(lastbuf.len_alloc) - int(lastbuf.len_used) // Adjust for last string being overallocated
	for i := range int(cnode.len) {
		buf := bufferIndex(cnode.buffers, i)
		assert.GreaterOrEqualf(t, int(uintptr(unsafe.Pointer(buf))), start, "%s subscript %d buffer is located before memory allocation", n, i)
		assert.LessOrEqualf(t, int(uintptr(unsafe.Pointer(buf))), end, "%s subscript %d buffer is located after allocation", n, i)
		assert.GreaterOrEqualf(t, int(uintptr((unsafe.Pointer(buf.buf_addr)))), start, "%s subscript %d string is located before allocation", n, i)
		assert.LessOrEqualf(t, int(uintptr(unsafe.Add(unsafe.Pointer(buf.buf_addr), int(buf.len_used)))), end, "%s subscript %d string ends after allocation end", n, i)
		assert.LessOrEqualf(t, int(uintptr(unsafe.Add(unsafe.Pointer(buf.buf_addr), int(buf.len_alloc)))), end, "%s subscript %d string space ends after allocation end", n, i)
	}
}

func TestCloneNode(t *testing.T) {
	conn1 := SetupTest(t)
	conn2 := NewConn()
	n := conn1.Node("var", "abc")
	clone1 := conn1.CloneNode(n)
	clone2 := conn2.CloneNode(n)
	assert.NotEqual(t, fmt.Sprintf("%p", clone1.Conn.cconn), fmt.Sprintf("%p", clone2.Conn.cconn))
	assert.NotEqual(t, fmt.Sprintf("%p", clone1.Conn.cconn.errstr.buf_addr), fmt.Sprintf("%p", clone2.Conn.cconn.errstr.buf_addr))
	assert.NotEqual(t, fmt.Sprintf("%p", clone1.Conn.cconn.value.buf_addr), fmt.Sprintf("%p", clone2.Conn.cconn.value.buf_addr))

	assert.Equal(t, "", clone1.Get())
	clone2.Set(3)
	assert.Equal(t, "3", clone1.Get())

	// Verify that a panic results if programmer tries to clone a mutable node
	mutable := n.Index(1)
	assert.Panics(t, func() { conn2.CloneNode(mutable) })
}

func TestIteration(t *testing.T) {
	conn := SetupTest(t)
	// Test that iteration yields mutable nodes
	n := conn.Node("person")
	n.Child(1).Set(1)
	assert.Equal(t, false, n.IsMutable())
	for i, subscript := range n.Children() {
		assert.Equal(t, "1", subscript)
		assert.Equal(t, true, i.IsMutable())
	}
	// Test that Next() on a mutable node finds the correct parent to return an index of
	n.Child(1, "age").Set(51)
	n.Child(1, "height").Set(178)
	assert.Equal(t, `person(1,"height")`, n.Index(1, "age").Next().String())
	assert.Equal(t, false, n.IsMutable())
	assert.Equal(t, true, n.Index("def").IsMutable())
	assert.Equal(t, false, n.IsMutable())
	assert.Equal(t, false, n.Index("def").Clone().IsMutable())

	// Traverse early exit code path in _children() for code coverage
	for range n.Children() {
		break
	}

	// Code coverage for when Next() gets an INVSTRLEN error and has to retry
	longString := strings.Repeat("A", YDB_MAX_STR)
	conn2 := NewConn() // new conn to ensure no large pre-allocated buffer
	n = conn2.Node("var")
	n.Child(1).Set(1)
	n.Child(longString).Set(2)
	assert.Equal(t, longString, n.Child(1).Next().Subscript(-1))
	assert.Equal(t, "1", n.Child(longString).Prev().Subscript(-1))
	conn.KillAllLocals()
	assert.Nil(t, n.Child(1).Prev())
}

func TestChild(t *testing.T) {
	conn := SetupTest(t)
	n1 := conn.Node("var", "abc")
	n2 := n1.Index("def")
	n3 := n2.Child("ghi", "jkl")
	n4 := n3.Index("mno")
	n5 := n3.Index(1, 2, 3, 4, 5)
	n6 := n5.Clone()
	n1.checkBuffers(t)
	n2.checkBuffers(t)
	n3.checkBuffers(t)
	n4.checkBuffers(t)
	n6.checkBuffers(t)
	assert.Equal(t, `var("abc","def","ghi","jkl",1,2,3,4,5)`, n6.String())
}

func TestSubscript(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var1", "asdf", "jkl;")
	assert.Equal(t, "var1", n.Subscript(0))
	assert.Equal(t, "asdf", n.Subscript(1))
	assert.Equal(t, "jkl;", n.Subscript(2))
	assert.Equal(t, "jkl;", n.Subscript(-1))
	assert.Equal(t, "asdf", n.Subscript(-2))
	assert.Equal(t, "var1", n.Subscript(-3))
	assert.Panics(t, func() { n.Subscript(3) })
	assert.Panics(t, func() { n.Subscript(-4) })
}

func TestIndex(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var1")
	assert.Panics(t, func() { n.Index() }) // call with no parameters should panic
	n2 := n.Index("jkl")
	n3 := n2.Child("qwerty")
	assert.Equal(t, `var1("jkl","qwerty")`, n3.String())

	// Test that Index(...).Index(...) works
	n = conn.Node("person")
	n.Child(1, "name").Set("fred")
	n.Child(2, "name").Set("daph")
	n.Child(1, "address").Set("2 Rocklands Rd")
	n.Child(2, "address").Set("5 Moonshine St")
	n.Child(1, "address", "postcode").Set(1234)
	n.Child(2, "address", "postcode").Set(5678)
	n.Child(1, "age").Set(59)
	n.Child(2, "age").Set(14)
	age := map[string]int{}
	for person := range n.Children() {
		name := person.Index("name").Get()
		age[name] = person.Index("age").GetInt()
	}
	assert.Equal(t, map[string]int{"fred": 59, "daph": 14}, age)

	// Check that mutable node reallocation works when new subscript doesn't fit into mutable node
	// Create fresh node without an indexed mutation attached
	n = conn.Node("person")
	index1 := n.Index(1)
	index2 := n.Index(2)
	// Check that index1 is also changed to the new depth-0 subscript of index2 -- i.e. node reused, not reallocated
	assert.Equal(t, index1.String(), index2.String())
	// Force reallocation: make sure new subscript is big enough to cause a reallocation
	bigSubscript := preallocSubscript + "ABC"
	index3 := n.Index(bigSubscript)
	// This NotEqual test only works because internally an index of depth 1 points to a real mutation, not a stub
	// The mutation has been reallocated but the old index1 still points to the previous one
	// This means depth-1 indexes last slightly longer than depth >1 but anyway programmers should not be counting
	// on it lasting past the next use of index (per documentation), so their own fault if they depend on this fact.
	assert.NotEqual(t, index1.String(), index3.String())

	// Check that a sequence of using a longer-shorter-longer index depth doesn't require reallocation the second time
	n = conn.Node("person")
	n.Index(1, "a")
	index1 = n.Index(bigSubscript) // force reallocation of depth 1 -- but it should retain depth 2 capacity
	index2 = n.Index(3, "b")
	// Check that index1 is also changed to the new depth-0 subscript of index2 -- i.e. node reused, not reallocated
	assert.Equal(t, "3", index1.Subscript(1))

	// Check indexing a node with more than YDB_MAX_SUBS panics
	subs := []any{}
	for range YDB_MAX_SUBS - 1 {
		subs = append(subs, "")
	}
	n = conn.Node("person", subs...)
	// should work with exactly YDB_MAX_SUBS
	n.Index(1).Get()
	// should fail with YDB_MAX_SUBS+1
	assert.Panics(t, func() { n.Index(1, 2) })

	// Same test but with one more sub -- so requiring one less index depth to fail
	subs = append(subs, "")
	n = conn.Node("person", subs...)
	// should fail YDB_MAX_SUBS+1
	assert.Panics(t, func() { n.Index(1) })
}

func TestSetGet(t *testing.T) {
	conn := SetupTest(t)
	// Test Lookup and Get-with-default on non-existent var
	testDefault := func(n *Node) {
		val, ok := n.Lookup()
		assert.Equal(t, "", val)
		assert.False(t, ok)
		valbytes, ok := n.LookupBytes()
		assert.Equal(t, []byte{}, valbytes)
		assert.False(t, ok)
		assert.Equal(t, "", n.Get())
		assert.Equal(t, 0, n.GetInt())
		assert.Equal(t, 0.0, n.GetFloat())
		assert.Equal(t, "default", n.Get("default"))
		assert.Equal(t, 10, n.GetInt(10))
		assert.Equal(t, 10.0, n.GetFloat(10.0))
		assert.Equal(t, "", n.Get())
		assert.Equal(t, []byte("default"), n.GetBytes([]byte("default")))
		assert.Equal(t, []byte{}, n.GetBytes())
		assert.Equal(t, []byte(""), n.GetBytes([]byte("")))
	}
	testDefault(conn.Node("var"))
	testDefault(conn.Node("var", 1))

	// Test Set()
	n := conn.Node("var")
	n.Set("value")
	val, ok := n.Lookup()
	assert.Equal(t, "value", val)
	assert.True(t, ok)
	valbytes, ok := n.LookupBytes()
	assert.Equal(t, []byte("value"), valbytes)
	assert.True(t, ok)
	assert.Equal(t, "value", n.Get())
	assert.Equal(t, "value", n.Get("default"))

	n.Set([]byte("Hello"))
	assert.Equal(t, "Hello", n.Get())
	assert.Equal(t, "Hello", n.Get("defaultvalue"))
	assert.Equal(t, []byte("Hello"), n.GetBytes([]byte("defaultvalue")))

	// Set to a really big value to exercise _Lookup's code path to re-try lookup after YDB says buffer isn't big enough
	longString := strings.Repeat("A", YDB_MAX_STR)
	n.Set(longString)
	conn2 := NewConn() // new conn to ensure no large pre-allocated buffer
	n2 := conn2.CloneNode(n)
	assert.Equal(t, longString, n2.Get())

	// Test Set to a number
	n.Set(5)
	assert.Equal(t, "5", n.Get())
	n.Set(5.6)
	assert.Equal(t, "5.6", n.Get())

	n.Set("abc")
	assert.Equal(t, 0, n.GetInt())
	assert.Equal(t, float64(0), n.GetFloat())
	n.Set("-12")
	assert.Equal(t, -12, n.GetInt())
	assert.Equal(t, float64(-12), n.GetFloat())
	n.Set("-12abc")
	assert.Equal(t, 0, n.GetInt())
	assert.Equal(t, float64(0), n.GetFloat())
	// Test Set to various input types
	n.Set(int(-1))
	assert.Equal(t, "-1", n.Get())
	n.Set(int32(-32))
	assert.Equal(t, "-32", n.Get())
	n.Set(int64(-64))
	assert.Equal(t, "-64", n.Get())
	n.Set(uint(1))
	assert.Equal(t, "1", n.Get())
	n.Set(uint32(32))
	assert.Equal(t, "32", n.Get())
	n.Set(uint64(64))
	assert.Equal(t, "64", n.Get())
	n.Set(float32(32.32))
	assert.Equal(t, "32.32", n.Get())
	n.Set(float64(64.64))
	assert.Equal(t, "64.64", n.Get())
	assert.Panics(t, func() { n.Set(true) })
}

func TestData(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var")
	assert.Equal(t, true, n.HasNone())
	assert.Equal(t, false, n.HasValue())
	assert.Equal(t, false, n.HasTree())
	assert.Equal(t, false, n.HasBoth())

	n.Set("value")
	assert.Equal(t, false, n.HasNone())
	assert.Equal(t, true, n.HasValue())
	assert.Equal(t, false, n.HasTree())
	assert.Equal(t, false, n.HasBoth())

	n.Child("sub1", "sub2").Set("valsub2")
	assert.Equal(t, false, n.HasNone())
	assert.Equal(t, true, n.HasValue())
	assert.Equal(t, true, n.HasTree())
	assert.Equal(t, true, n.HasBoth())

	n2 := n.Child("sub1")
	assert.Equal(t, false, n2.HasNone())
	assert.Equal(t, false, n2.HasValue())
	assert.Equal(t, true, n2.HasTree())
	assert.Equal(t, false, n2.HasBoth())
}

func TestKill(t *testing.T) {
	conn := SetupTest(t)
	n1 := conn.Node("var1")
	n2 := conn.Node("var2")
	n3 := conn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasValueOnly(), n3.HasBoth()))
	n2.Kill()
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasBoth()))
	n3.Kill()
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasNone()))
}

func TestClear(t *testing.T) {
	conn := SetupTest(t)
	n1 := conn.Node("var1")
	n2 := conn.Node("var2")
	n3 := conn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasValueOnly(), n3.HasBoth()))
	n2.Clear()
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasBoth()))
	n3.Clear()
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasTreeOnly()))
	n3.Child("sub1").Clear()
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasNone()))
}

func TestIncr(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var")
	assert.Equal(t, "1", n.Incr(1))
	assert.Equal(t, "1", n.Get())
	assert.Equal(t, "3", n.Incr(2))
	assert.Equal(t, "4.5", n.Incr(1.5))
	assert.Equal(t, "0", n.Incr(-4.5))
	assert.Equal(t, "-4.5", n.Incr(-4.5))
	assert.Equal(t, "-4.5", n.Incr(0))
	assert.Panics(t, func() { n.Incr("") })

	n.Set("0")
	assert.Equal(t, "1", n.Incr("1"))
	assert.Equal(t, "2", n.Incr("1abcdefg"))
}

// Test traversal of a database tree
func TestDump(t *testing.T) {
	conn := NewConn()
	conn.KillAllLocals()
	n := conn.Node("tree", 1)
	assert.Panics(t, func() { n.Dump(3, 4, 5) })
	// The rest is tested in ExampleNode_Dump
}
