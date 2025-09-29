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
	"math"

	"lang.yottadb.com/go/yottadb/v2"
)

// ---- Examples

// Test the example given in the package doc at the top of doc.go
func ExampleNewConn() {
	// Note: this example is also used in the README.md
	defer yottadb.Shutdown(yottadb.MustInit())
	conn := yottadb.NewConn()

	// Store unicode greeting into node ^hello("world")
	greeting := conn.Node("^hello", "world")
	greeting.Set("สวัสดี") // Sawadee (hello in Thai)
	fmt.Println(greeting.Get())

	// Output:
	// สวัสดี
}

// Example
func ExampleNode_Kill() {
	// Note: this example is also used in the README.md
	defer yottadb.Shutdown(yottadb.MustInit())
	conn := yottadb.NewConn()

	hi := conn.Node("^howdy") // create Node instance pointing to YottaDB global ^hello
	hi.Set("western")
	cowboy := hi.Child("cowboy") // new variable pointing to subnode "cowboy" subscript
	cowboy.Set("Howdy partner!") // set ^hello("cowboy") to "Howdy partner!"
	ranches := cowboy.Child("ranches")
	ranches.Incr(2) // Increment empty node by 2 to get 2

	fmt.Printf("First dump:\n%#v\n", hi) // %#v produces the same string as hi.Dump()
	cowboy.Kill()                        // delete node, its children, and all values
	fmt.Printf("Second dump:\n%#v\n", hi)
	hi.Clear() // clear this node's value, too

	// Output:
	// First dump:
	// ^howdy="western"
	// ^howdy("cowboy")="Howdy partner!"
	// ^howdy("cowboy","ranches")=2
	//
	// Second dump:
	// ^howdy="western"
}

// Calculate the height of 3 oak trees, based on their shadow length and the angle of the sun.
func ExampleNode_GetFloat() {
	// Note: this example is used in the README.md
	defer yottadb.Shutdown(yottadb.MustInit())
	conn := yottadb.NewConn()

	// capture initial data values into a Go map
	data := []map[string]int{
		{"shadow": 10, "angle": 30},
		{"shadow": 13, "angle": 30},
		{"shadow": 15, "angle": 45},
	}

	// Enter data into the database
	trees := conn.Node("^oaks") // node object pointing to YottaDB global ^oaks
	for i, items := range data {
		for key, value := range items {
			trees.Child(i+1, key).Set(value)
		}
	}

	// Iterate data in the database and calculate results
	for tree, i := range trees.Children() {
		tree.Child("height").Set(tree.Child("shadow").GetFloat() *
			math.Tan(tree.Child("angle").GetFloat()*math.Pi/180))
		fmt.Printf("Oak %s is %.1fm high\n", i, tree.Child("height").GetFloat())
	}
	// Output:
	// Oak 1 is 5.8m high
	// Oak 2 is 7.5m high
	// Oak 3 is 15.0m high
}
