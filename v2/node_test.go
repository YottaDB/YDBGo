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
	"unsafe"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

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
	tconn := SetupTest(t)
	n := tconn.Node("var")
	val, ok := n.Lookup()
	assert.Equal(t, "", val)
	assert.False(t, ok)
	assert.Equal(t, "", n.Get())
	assert.Equal(t, "default", n.Get("default"))
	assert.Equal(t, "", n.Get())
	assert.Equal(t, []byte("default"), n.GetBytes([]byte("default")))
	assert.Equal(t, []byte(""), n.GetBytes([]byte("")))

	n.Set("value")
	assert.Equal(t, "value", n.Get())
	assert.Equal(t, "value", n.Get("default"))

	n.Set([]byte("Hello"))
	assert.Equal(t, "Hello", n.Get())
	assert.Equal(t, "Hello", n.Get("defaultvalue"))
	assert.Equal(t, []byte("Hello"), n.GetBytes([]byte("defaultvalue")))

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
}

func TestData(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("var")
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
	tconn := SetupTest(t)
	n1 := tconn.Node("var1")
	n2 := tconn.Node("var2")
	n3 := tconn.Node("var3")
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
	tconn := SetupTest(t)
	n1 := tconn.Node("var1")
	n2 := tconn.Node("var2")
	n3 := tconn.Node("var3")
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
	tconn := SetupTest(t)
	n := tconn.Node("var")
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

// Example of getting next subscript
func ExampleNode_Next() {
	conn := NewConn()
	n := conn.Node("X", 1)
	n.Child(2, "3").Set("123")
	n.Child(2, 3, 7).Set(1237)
	n.Child(2, 4).Set(124)

	x := conn.Node("X", 1, "2", "")
	x = x.Next()
	for x != nil {
		fmt.Printf("%s=%s\n", x, x.Get())
		x = x.Next()
	}
	// Output:
	// X(1,2,3)=123
	// X(1,2,4)=124
}

// Example of listing all local database variable names
func ExampleNode_Next_varnames() {
	conn := NewConn()
	conn.Node("X", 1).Set("X1")
	conn.Node("X", 1, 2).Set("X12")
	conn.Node("Y", 2).Set("Y2")

	fmt.Println("Display all top-level database variable names, starting after '%' (which is the first possible name in sort order)")
	x := conn.Node("%")
	x = x.Next()
	for x != nil {
		fmt.Printf("%s\n", x)
		x = x.Next()
	}
	// Output:
	// Display all top-level database variable names, starting after '%' (which is the first possible name in sort order)
	// X
	// Y
}

// Example of getting all child nodes
func ExampleNode_Children() {
	conn := NewConn()
	n := conn.Node("X", 1)
	n.Child(2, 3).Set(123)
	n.Child(2, 4).Set(124)
	n.Child(2, 3, "person").Set(1237)

	// Note that the following person fields will come out in alphabetical order below
	n.Child(2, 3, "person", "address").Set("2 Rocklands Rd")
	n.Child(2, 3, "person", "address", "postcode").Set(1234)
	n.Child(2, 3, "person", "occupation").Set("engineer")
	n.Child(2, 3, "person", "age").Set(42)
	n.Child(2, 3, "person", "sex").Set("male")

	n = conn.Node("X", 1, 2)
	for x := range n.Children() {
		fmt.Printf("%s=%s\n", x, x.Get())
	}

	fmt.Println("Do the same in reverse:")
	for x := range n.ChildrenBackward() {
		fmt.Printf("%s=%s\n", x, x.Get())
	}

	n = conn.Node("X", 1, 2, 3, "person")
	fmt.Printf("Person fields: (")
	for _, sub := range n.Children() {
		fmt.Printf("%s ", sub)
	}
	fmt.Println(")")

	// Output:
	// X(1,2,3)=123
	// X(1,2,4)=124
	// Do the same in reverse:
	// X(1,2,4)=124
	// X(1,2,3)=123
	// Person fields: (address age occupation sex )
}

// Example of fast iteration of a node to increment only children with subscripts 0..999999.
func ExampleNode_Mutate() {
	conn := NewConn()
	n := conn.Node("counter").MutableChild("")
	n.Mutate(1000000).Set("untouched")
	for i := range 1000000 {
		n.Mutate(i).Incr(1)
	}

	fmt.Printf("%s: %s\n", n.Mutate(0), n.Get())
	fmt.Printf("%s: %s\n", n.Mutate(999999), n.Get())
	fmt.Printf("%s: %s\n", n.Mutate(1000000), n.Get())
	// Output:
	// counter(0): 1
	// counter(999999): 1
	// counter(1000000): untouched
}

// Example of traversing a database tree
func ExampleNode_GoString() {
	conn := NewConn()
	n := conn.Node("tree", 1)
	n.Child(2, 3).Set(123)
	n.Child(2, 3, 7).Set("Hello!")
	n.Child(2, 4).Set(124)

	fmt.Printf("Dump is:\n%#v", n)

	// Output:
	// Dump is:
	// tree(1,2,3)=123
	// tree(1,2,3,7)="Hello!"
	// tree(1,2,4)=124
}

// Example of traversing a database tree
func ExampleNode_Dump() {
	conn := NewConn()
	n := conn.Node("tree", 1)
	n.Child(6).Set(16)
	n.Child(2, 3).Set(123)
	n.Child(2, 3, 7).Set("Hello!")
	n.Child(2, 4).Set(124)
	n.Child(2, 5, 9).Set(1259)
	nb := conn.Node("tree", "B")
	nb.Child(1).Set("AB")

	fmt.Println(n.Dump())

	n.Child(2, 3).Set("~ A\x00\x7f" + strings.Repeat("A", 1000))
	fmt.Print(n.Dump(2, 8))

	// Output:
	// tree(1,2,3)=123
	// tree(1,2,3,7)="Hello!"
	// tree(1,2,4)=124
	// tree(1,2,5,9)=1259
	// tree(1,6)=16
	//
	// tree(1,2,3)="~ A"_$C(0,127)_"AAA"...
	// tree(1,2,3,7)="Hello!"
	// ...
}

// Example of traversing a database tree
func ExampleNode_TreeNext() {
	conn := NewConn()
	n := conn.Node("tree", 1)
	n.Child(2, 3).Set(123)
	n.Child(2, 3, 7).Set(1237)
	n.Child(2, 4).Set(124)
	n.Child(2, 5, 9).Set("Hello!")
	n.Child(6).Set(16)
	nb := conn.Node("tree", "B")
	nb.Child(1).Set("AB")

	x := conn.Node("tree").TreeNext()
	for x != nil {
		fmt.Printf("%s=%s\n", x, conn.Quote(x.Get()))
		x = x.TreeNext()
	}

	fmt.Println("Re-start half way through and go in reverse order:")
	x = conn.Node("tree", 1, 2, 4)
	for x != nil {
		fmt.Printf("%s=%s\n", x, conn.Quote(x.Get()))
		x = x.TreePrev()
	}

	// Output:
	// tree(1,2,3)=123
	// tree(1,2,3,7)=1237
	// tree(1,2,4)=124
	// tree(1,2,5,9)="Hello!"
	// tree(1,6)=16
	// tree("B",1)="AB"
	// Re-start half way through and go in reverse order:
	// tree(1,2,4)=124
	// tree(1,2,3,7)=1237
	// tree(1,2,3)=123
}

// Example of traversing a database tree
func ExampleNode_Tree() {
	conn := NewConn()
	n := conn.Node("tree", 1)
	n.Child(2, 3).Set(123)
	n.Child(2, 3, 7).Set(1237)
	n.Child(2, 4).Set(124)
	n.Child(2, 5, 9).Set(1259)
	n.Child(6).Set(16)
	nb := conn.Node("tree", "B")
	nb.Child(1).Set("AB")

	for x := range n.Child(2).Tree() {
		fmt.Printf("%s=%s\n", x, conn.Quote(x.Get()))
	}

	// Output:
	// tree(1,2,3)=123
	// tree(1,2,3,7)=1237
	// tree(1,2,4)=124
	// tree(1,2,5,9)=1259
}

// Test cases that ExampleNode_TreeNext() did not catch
func TestTreeNext(t *testing.T) {
	tconn := NewConn()

	// Ensure TreeNext will work even if it has to allocate new subscript memory up to the size of YDB_MAX_STR
	bigstring := strings.Repeat("A", YDB_MAX_STR)
	n := tconn.Node("X", bigstring)
	n.Child(2, 3).Set("Big23")
	n.Child(5, bigstring).Set("Big5Big")

	x := tconn.Node("X")
	output := ""
	for {
		x = x.TreeNext()
		if x == nil {
			break
		}
		output += fmt.Sprintf("%s=%s ", x, x.Get())
	}
	assert.Equal(t, `X("`+bigstring+`",2,3)=Big23 X("`+bigstring+`",5,"`+bigstring+`")=Big5Big `, output)
}
