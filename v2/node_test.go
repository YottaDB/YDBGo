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

func TestCloneNode(t *testing.T) {
	conn1 := SetupTest(t)
	conn2 := NewConn()
	n1 := conn1.Node("var1")
	n2 := conn2.Node("var2")

	// Make sure the LVUNDEF error ends with "var1"
	n1.Get()
	status := n1.Conn.lastCode()
	err := n1.Conn.lastError(status)
	assert.Contains(t, err.Error(), ": var1")

	// Make sure conn2 LVUNDEF error returns "var2"
	n2.Get()
	status = n1.Conn.lastCode()
	err = n2.Conn.lastError(status)
	assert.Contains(t, err.Error(), ": var2")
	// Alter tptoken in n2's conn
	n2.Conn.cconn.tptoken++

	// Above should not have changed the last message that's still stored in conn1
	status = n1.Conn.lastCode()
	err = n1.Conn.lastError(status)
	assert.Contains(t, err.Error(), ": var1")
	// Check that a changed tptoken in n2's conn doesn't affect n1's conn
	assert.NotEqual(t, n1.Conn.cconn.tptoken, n2.Conn.cconn.tptoken)
}

func (n *Node) checkBuffers(t *testing.T) {
	cnode := n.cnode
	lastbuf := bufferIndex(&cnode.buffers, int(cnode.len-1))
	start := int(uintptr(unsafe.Pointer(&cnode.buffers)))
	end := int(uintptr(unsafe.Pointer(bufferIndex(&cnode.buffers, int(cnode.len)))))
	for _, s := range n.Subscripts() {
		end += len(s)
	}
	end += int(lastbuf.len_alloc) - int(lastbuf.len_used) // Adjust for last string being overallocated
	for i := range int(cnode.len) {
		buf := bufferIndex(&cnode.buffers, i)
		assert.GreaterOrEqualf(t, int(uintptr(unsafe.Pointer(buf))), start, "%s subscript %d buffer is located before memory allocation", n, i)
		assert.LessOrEqualf(t, int(uintptr(unsafe.Pointer(buf))), end, "%s subscript %d buffer is located after allocation", n, i)
		assert.GreaterOrEqualf(t, int(uintptr((unsafe.Pointer(buf.buf_addr)))), start, "%s subscript %d string is located before allocation", n, i)
		assert.LessOrEqualf(t, int(uintptr(unsafe.Add(unsafe.Pointer(buf.buf_addr), int(buf.len_used)))), end, "%s subscript %d string ends after allocation end", n, i)
		assert.LessOrEqualf(t, int(uintptr(unsafe.Add(unsafe.Pointer(buf.buf_addr), int(buf.len_alloc)))), end, "%s subscript %d string space ends after allocation end", n, i)
	}
}

func testCloneNode(t *testing.T) {
	conn1 := SetupTest(t)
	conn2 := NewConn()
	n1 := conn1.Node("var", "abc")
	n2 := conn2.CloneNode(n1)
	assert.NotEqual(t, n1.Conn.cconn, n2.Conn.cconn)
	assert.NotEqual(t, n1.Conn.cconn.errstr.buf_addr, n2.Conn.cconn.errstr.buf_addr)
	assert.NotEqual(t, n1.Conn.cconn.value.buf_addr, n2.Conn.cconn.value.buf_addr)

	assert.Equal(t, "", n1.Get())
	n2.Set(3)
	assert.Equal(t, "3", n1.Get())
}

func TestIteration(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var")
	n.Child(1).Set(1)
	assert.Equal(t, false, n.IsMutable())
	for i, subscript := range n.Children() {
		assert.Equal(t, "1", subscript)
		assert.Equal(t, true, i.IsMutable())
	}
	assert.Equal(t, false, n.IsMutable())
	assert.Equal(t, true, n.MutableChild("def").IsMutable())
	assert.Equal(t, false, n.IsMutable())
	assert.Equal(t, false, n.MutableChild("def").Clone().IsMutable())

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
	n2 := n1.MutableChild().Mutate("def")
	n3 := n2.Child("ghi", "jkl")
	n4 := n3.MutableChild().Mutate("mno")
	n1.checkBuffers(t)
	n2.checkBuffers(t)
	n3.checkBuffers(t)
	n4.checkBuffers(t)
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

func TestMutateNode(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var1", "asdf")
	n2 := n.MutableChild().Mutate("jkl")
	n3 := n2.Child("qwerty")
	assert.Equal(t, `var1("jkl","qwerty")`, n3.String())
}

func TestSetGet(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("var")
	val, ok := n.Lookup()
	assert.Equal(t, "", val)
	assert.False(t, ok)
	valbytes, ok := n.LookupBytes()
	assert.Equal(t, []byte{}, valbytes)
	assert.False(t, ok)
	assert.Equal(t, "", n.Get())
	assert.Equal(t, "default", n.Get("default"))
	assert.Equal(t, "", n.Get())
	assert.Equal(t, []byte("default"), n.GetBytes([]byte("default")))
	assert.Equal(t, []byte{}, n.GetBytes())
	assert.Equal(t, []byte(""), n.GetBytes([]byte("")))

	n.Set("value")
	val, ok = n.Lookup()
	assert.Equal(t, "value", val)
	assert.True(t, ok)
	valbytes, ok = n.LookupBytes()
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
