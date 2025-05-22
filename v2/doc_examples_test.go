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

// ---- Tests and Examples

// ExampleDoc tests the example given in the package doc at the top of doc.go
func ExampleNewConn() {
	db := yottadb.Init()
	defer yottadb.Shutdown(db)
	conn := yottadb.NewConn()
	n := conn.Node("person", "name")
	n.Child("first").Set("Joe")
	n.Child("last").Set("Bloggs")
	for x := range n.Children() {
		fmt.Printf("%s = %s\n", x, x.Get())
	}
	// Output:
	// person("name","first") = Joe
	// person("name","last") = Bloggs
}
