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

The package requires minimum versions of Go 1.24 and YottaDB r1.34. It uses CGo to interface between this Go wrapper and the YottaDB engine written in C.
Its use of the `Node` type to pin memory references to database subscript strings gives it optimal speed.
To aid migration of YDBGo v1 to v2, it is possible to use [both in one application].

# Example

	package main

	import "fmt"
	import "lang.yottadb.com/go/yottadb/v2"

	func main() {
		defer yottadb.Shutdown(yottadb.MustInit())
		conn := yottadb.NewConn()

		// Store unicode greeting into node ^hello("world")
		greeting := conn.Node("^hello", "world")
		greeting.Set("สวัสดี") // Sawadee (hello in Thai)
		fmt.Println(greeting.Get())

		// Output:
		// สวัสดี
	}

# Prerequisites

[Install YottaDB] and consider reading the introduction to [YottaDB's data model].

[YottaDB's data model]: https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#concepts
[Install YottaDB]: https://yottadb.com/product/get-started/
[both in one application]: https://gitlab.com/YottaDB/Lang/YDBGo/-/blob/master/v2/README.md#mixing-ydbgo-v1-and-v2

[ACID transactions]: https://en.wikipedia.org/wiki/ACID_(computer_science)
[YottaDB transactions]: https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#transaction-processing
*/
package yottadb
