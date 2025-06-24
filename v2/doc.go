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

Warning: YDBGo v2 is in alpha testing.

The package requires minimum versions of Go 1.24 and YottaDB r1.34. It uses CGo to interface between this Go wrapper and the YottaDB engine written in C.
Its use of the `Node` type to pin memory references to database subscript strings gives it optimal speed.

# Example

	package main

	import "lang.yottadb.com/go/yottadb/v2"

	func main() {
		defer yottadb.Shutdown(yottadb.InitPanic())
		conn := yottadb.NewConn()

		n := conn.Node("person", "name")
		n.Child("first").Set("Joe")
		n.Child("last").Set("Bloggs")
		for x := range n.Children() {
			println(x.String(), "=", x.Get())
		}

		// Store unicode greeting into global node ^hello("world")
		greeting := conn.Node("^hello", "world")
		greeting.Set("สวัสดี") // Sawadee (hello in Thai)
		println(greeting.Get(), n.Child("first").Get(), n.Child("last").Get())
	}

Output:

	person("name","first") = Joe
	person("name","last") = Bloggs
	สวัสดี Joe Bloggs

# Prerequisites

[Install YottaDB] and consider reading the introduction to [YottaDB's data model].

[YottaDB's data model]: https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#concepts
[Install YottaDB]: https://yottadb.com/product/get-started/

[ACID transactions]: https://en.wikipedia.org/wiki/ACID_(computer_science)
[YottaDB transactions]: https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#transaction-processing
*/
package yottadb
