//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025-2026 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb_test

import (
	"fmt"
	"strings"
	"testing"

	assert "github.com/stretchr/testify/require"
	"lang.yottadb.com/go/yottadb/v2"
)

// ---- Examples

func ExampleConn_Node() {
	conn := yottadb.NewConn()
	n := conn.Node("var", "sub1", "sub2")
	n2 := n.Child("sub3", "sub4")
	fmt.Printf("%v\n", n)
	fmt.Printf("%v\n", n2)
	// Output:
	// var("sub1","sub2")
	// var("sub1","sub2","sub3","sub4")
}

func ExampleNode_Subscripts() {
	conn := yottadb.NewConn()
	n := conn.Node("var", "sub1", "sub2")
	fmt.Println(n)
	fmt.Println(n.Subscripts())
	// Output:
	// var("sub1","sub2")
	// [var sub1 sub2]
}

// Example of converting a ZWRITE-formatted string to a Go string:
func ExampleConn_Zwr2Str() {
	conn := yottadb.NewConn()
	str, err := conn.Zwr2Str(`"X"_$C(0)_"ABC"`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v", str)
	// Output: "X\x00ABC"
}

// Example of converting a Go string to a ZWRITE-formatted string:
func ExampleConn_Str2Zwr() {
	conn := yottadb.NewConn()
	str, err := conn.Str2Zwr("X\x00ABC")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v", str)
	// Output: "X"_$C(0)_"ABC"
}

// Example of viewing a Node instance as a string:
func ExampleNode_String() {
	conn := yottadb.NewConn()
	n := conn.Node("var", "sub1", "sub2")
	numsubs := conn.Node("var2", 1, 2)
	fmt.Println(n)
	fmt.Println(numsubs)
	// Output:
	// var("sub1","sub2")
	// var2(1,2)
}

// Example of getting next subscript
func ExampleNode_Next() {
	conn := yottadb.NewConn()
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
	conn := yottadb.NewConn()
	conn.KillAllLocals()
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
	conn := yottadb.NewConn()
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

// Example of fast iteration of a node to increment only children with subscripts 0..99999.
func ExampleNode_Index() {
	conn := yottadb.NewConn()
	n := conn.Node("counter")
	n.Index(100000).Set("untouched")
	for i := range 100000 {
		n.Index(i).Incr(1)
	}

	fmt.Printf("%s: %s\n", n.Index(0), n.Index(0).Get())
	fmt.Printf("%s: %s\n", n.Index(99999), n.Index(99999).Get())
	fmt.Printf("%s: %s\n", n.Index(100000), n.Index(100000).Get())
	// Output:
	// counter(0): 1
	// counter(99999): 1
	// counter(100000): untouched
}

// Example of when not to use Node.Index
func ExampleNode_Index_incorrect() {
	conn := yottadb.NewConn()
	person := conn.Node("person")
	first := person.Index("first")
	last := person.Index("last") // This overwrites the person index to be 'last' so first now points to the wrong thing
	first.Set("Joe")
	last.Set("Bloggs")
	// Trying to set the first name failed; instead setting the last name,
	// which was then overwritten when setting the last name
	fmt.Printf("Retrieving the stored names yields first='%s', last='%s'\n", first.Get(), last.Get())
	// Output:
	// Retrieving the stored names yields first='Bloggs', last='Bloggs'
}

// Example of traversing a database tree
func ExampleNode_GoString() {
	conn := yottadb.NewConn()
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
	conn := yottadb.NewConn()
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
	conn := yottadb.NewConn()
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
	conn := yottadb.NewConn()
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
	conn := yottadb.NewConn()

	// Ensure TreeNext will work even if it has to allocate new subscript memory up to the size of YDB_MAX_STR
	bigstring := strings.Repeat("A", yottadb.YDB_MAX_STR)
	n := conn.Node("X", bigstring)
	n.Child(2, 3).Set("Big23")
	n.Child(5, bigstring).Set("Big5Big")

	x := conn.Node("X")
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
