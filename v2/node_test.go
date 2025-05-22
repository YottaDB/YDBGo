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
	status := n1.conn.lastCode()
	err := n1.conn.lastError(status)
	assert.Contains(t, err.Error(), ": var1")

	// Make sure conn2 LVUNDEF error returns "var2"
	n2.Get()
	status = n1.conn.lastCode()
	err = n2.conn.lastError(status)
	assert.Contains(t, err.Error(), ": var2")
	// Alter tptoken in n2's conn
	n2.conn.cconn.tptoken++

	// Above should not have changed the last message that's still stored in conn1
	status = n1.conn.lastCode()
	err = n1.conn.lastError(status)
	assert.Contains(t, err.Error(), ": var1")
	// Check that a changed tptoken in n2's conn doesn't affect n1's conn
	assert.NotEqual(t, n1.conn.cconn.tptoken, n2.conn.cconn.tptoken)
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
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasValueOnly(), n3.HasBoth()))
	tconn.KillLocalsExcept("var1", "var3")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasBoth()))
	tconn.KillLocalsExcept()
	assert.Equal(t, multi(true, true, true), multi(n1.HasNone(), n2.HasNone(), n3.HasNone()))

	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	tconn.KillAllLocals()
	assert.Equal(t, multi(true, true, true), multi(n1.HasNone(), n2.HasNone(), n3.HasNone()))
}

func TestSetGet(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("var")
	val, ok := n.Lookup()
	assert.Equal(t, "", val)
	assert.False(t, ok)
	assert.Equal(t, "", n.Get())
	assert.Equal(t, "default", n.Get("default"))

	n.Set("value")
	assert.Equal(t, "value", n.Get())
	assert.Equal(t, "value", n.Get("default"))

	// Test Set to a number
	n.Set(5)
	assert.Equal(t, "5", n.Get())
	n.Set(5.6)
	assert.Equal(t, "5.6", n.Get())
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
	assert.Equal(t, 1.0, n.Incr(1))
	assert.Equal(t, "1", n.Get())
	assert.Equal(t, 3.0, n.Incr(2))
	assert.Equal(t, 4.5, n.Incr(1.5))
	assert.Equal(t, 0.0, n.Incr(-4.5))
	assert.Equal(t, -4.5, n.Incr(-4.5))

	n.Set("0")
	assert.Equal(t, 1.0, n.Incr("1"))
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
	assert.Equal(t, true, n.Lock(100*time.Millisecond))
	assert.Equal(t, true, n.Lock(100*time.Millisecond))
	assert.Equal(t, true, n.Lock(100*time.Millisecond))

	// Check that lock now exists
	lockpath := fmt.Sprint(n)
	assert.Equal(t, true, lockExists(lockpath))

	// Decrement 3 times and each time check whether lock exists
	n.Unlock()
	assert.Equal(t, true, lockExists(lockpath))
	n.Unlock()
	assert.Equal(t, true, lockExists(lockpath))
	n.Unlock()
	assert.Equal(t, false, lockExists(lockpath))

	// Now lock two paths and check that Lock(0) releases them
	n2 := tconn.Node("^var2")
	n.Lock()
	n2.Lock()
	assert.Equal(t, true, lockExists(fmt.Sprint(n)))
	assert.Equal(t, true, lockExists(fmt.Sprint(n2)))
	assert.Equal(t, true, tconn.Lock(0)) // Release all locks
	assert.Equal(t, false, lockExists(fmt.Sprint(n)))
	assert.Equal(t, false, lockExists(fmt.Sprint(n2)))

	// Now lock both using Lock() and make sure they get locked and unlocked
	assert.Equal(t, true, tconn.Lock(100*time.Millisecond, n, n2)) // Release all locks
	assert.Equal(t, true, lockExists(fmt.Sprint(n)))
	assert.Equal(t, true, lockExists(fmt.Sprint(n2)))
	assert.Equal(t, true, tconn.Lock(time.Duration(0))) // Release all locks
	assert.Equal(t, false, lockExists(fmt.Sprint(n)))
	assert.Equal(t, false, lockExists(fmt.Sprint(n2)))
}

// Example of getting next subscript
func ExampleNode_Next() {
	conn := NewConn()
	n := conn.Node("X", "1")
	n.Child("2", "3").Set("123")
	n.Child("2", "3", "7").Set("1237")
	n.Child("2", "4").Set("124")

	x := conn.Node("X", "1", "2", "")
	x = x.Next()
	for x != nil {
		fmt.Printf("%s=%s\n", x, Quote(x.Get()))
		x = x.Next()
	}
	// Output:
	// X(1,2,3)=123
	// X(1,2,4)=124
}

// Example of listing all local database variable names
func ExampleNode_Next_varnames() {
	conn := NewConn()
	conn.Node("X", "1").Set("X1")
	conn.Node("X", "1", "2").Set("X12")
	conn.Node("Y", "2").Set("Y2")

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

// Example of getting next subscript
func ExampleNode_Children() {
	conn := NewConn()
	n := conn.Node("X", "1")
	n.Child("2", "3").Set("123")
	n.Child("2", "3", "7").Set("1237")
	n.Child("2", "4").Set("124")

	n = conn.Node("X", "1", "2")
	for x := range n.Children() {
		fmt.Printf("%s=%s\n", x, Quote(x.Get()))
	}

	fmt.Println("Do the same in reverse:")
	for x := range n.ChildrenBackward() {
		fmt.Printf("%s=%s\n", x, Quote(x.Get()))
	}
	// Output:
	// X(1,2,3)=123
	// X(1,2,4)=124
	// Do the same in reverse:
	// X(1,2,4)=124
	// X(1,2,3)=123
}

// Example of getting a mutable version of node
func ExampleNode_mutate() {
	conn := NewConn()
	n := conn.Node("X", "1", "2", "3")
	mutation1 := n.mutate("4")
	mutation2 := n.mutate("text")
	fmt.Println(n)
	fmt.Println(mutation1)
	fmt.Println(mutation2)
	// Output:
	// X(1,2,3)
	// X(1,2,4)
	// X(1,2,"text")
}

// Example of traversing a database tree
func ExampleNode_TreeNext() {
	conn := NewConn()
	n1 := conn.Node("tree", "1")
	n1.Child("2", "3").Set("123")
	n1.Child("2", "3", "7").Set("1237")
	n1.Child("2", "4").Set("124")
	n1.Child("2", "5", "9").Set("1259")
	n1.Child("6").Set("16")
	nb := conn.Node("tree", "B")
	nb.Child("1").Set("AB")

	x := conn.Node("tree").TreeNext()
	for x != nil {
		fmt.Printf("%s=%s\n", x, Quote(x.Get()))
		x = x.TreeNext()
	}

	fmt.Println("Re-start half way through and go in reverse order:")
	x = conn.Node("tree", "1", "2", "4")
	for x != nil {
		fmt.Printf("%s=%s\n", x, Quote(x.Get()))
		x = x.TreePrev()
	}

	// Output:
	// tree(1,2,3)=123
	// tree(1,2,3,7)=1237
	// tree(1,2,4)=124
	// tree(1,2,5,9)=1259
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
	n1 := conn.Node("tree", "1")
	n1.Child("2", "3").Set("123")
	n1.Child("2", "3", "7").Set("1237")
	n1.Child("2", "4").Set("124")
	n1.Child("2", "5", "9").Set("1259")
	n1.Child("6").Set("16")
	nb := conn.Node("tree", "B")
	nb.Child("1").Set("AB")

	for x := range n1.Child("2").Tree() {
		fmt.Printf("%s=%s\n", x, Quote(x.Get()))
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
		output += fmt.Sprintf("%s=%s ", x, Quote(x.Get()))
	}
	assert.Equal(t, `X("`+bigstring+`",2,3)="Big23" X("`+bigstring+`",5,"`+bigstring+`")="Big5Big" `, output)
}
