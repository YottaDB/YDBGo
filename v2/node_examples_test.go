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

package yottadb_test

import (
	"fmt"

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

// Example of converting a ZWRITE-formatted string to a Go string
func ExampleConn_Zwr2Str() {
	conn := yottadb.NewConn()
	str, err := conn.Zwr2Str(`"X"_$C(0)_"ABC"`)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v", str)
	// Output: "X\x00ABC"
}

// Example of converting a Go string to a ZWRITE-formatted string
func ExampleConn_Str2Zwr() {
	conn := yottadb.NewConn()
	str, err := conn.Str2Zwr("X\x00ABC")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v", str)
	// Output: "X"_$C(0)_"ABC"
}

// Example of viewing a Node instance as a string.
func ExampleNode_String() {
	conn := yottadb.NewConn()
	n := conn.Node("var", "sub1", "sub2")
	numsubs := conn.Node("var2", "1", "2")
	fmt.Println(n)
	fmt.Println(numsubs)
	// Output:
	// var("sub1","sub2")
	// var2(1,2)
}
