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

/*
Package yottadb is a Go wrapper for a YottaDB database - a mature, high performance, transactional NoSQL engine with proven speed and stability.

The package requires Go 1.24. It uses CGo to interface between this Go wrapper and the YottaDB engine written in C.
Its use of the `Node` type to pin memory references to database subscript strings gives it optimal speed.

# Example

	db := yottadb.Init()
	defer yottadb.Shutdown(db)
	conn := yottadb.NewConn()
	n := conn.Node("person", "name")
	n.Child("first").Set("Joe")
	n.Child("last").Set("Bloggs")
	for x := range n.Iterate() {
		fmt.Printf("%s = %s\n", x, yottadb.Quote(x.Get()))
	}

Output:

	person("name","first") = "Joe"
	person("name","last") = "Bloggs"

# Installation

Prerequisites: [install YottaDB] and consider reading the introduction to [YottaDB's data model].

Install the Go wrapper:

	go get lang.yottadb.com/go/yottadb

[YottaDB's data model]: https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#concepts
[install YottaDB]: https://yottadb.com/product/get-started/

[ACID transactions]: https://en.wikipedia.org/wiki/ACID_(computer_science)
[YottaDB transactions]: https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#transaction-processing
*/
package yottadb
