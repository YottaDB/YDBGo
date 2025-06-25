//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2025 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

/*
Package yottadb provides a Go wrapper for YottaDB - a mature, high performance, transactional NoSQL engine with proven speed and stability.

# v2

v2 docs are at [pkg/lang.yottadb.com/go/yottadb/v2].

Deprecated: v1 will be deprecated once v2 has reached v2.0.2 production release.

# YottaDB Quick Start

Before starting, consider reading the introduction to YottaDB's data model at https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#concepts

The YottaDB Go wrapper requires a minimum YottaDB version of r1.34 and
is tested with a minimum Go version of 1.18. If the Go packages on
your operating system are older, and the Go wrapper does not work,
please obtain and install a newer Go implementation.

This quickstart assumes that YottaDB has already been installed as
described at https://yottadb.com/product/get-started/.

After installing YottaDB, install the Go wrapper:

	go get lang.yottadb.com/go/yottadb

# Easy API

The Easy API provides a set of functions that are very easy to use at the expense of
some additional copies for each operation. These functions all end with the letter 'E',
and are available in the yottadb package. They include:

  - DataE
  - DeleteE
  - DeleteExclE
  - IncrE
  - LockDecrE
  - LockIncrE
  - LockE
  - NodeNextE
  - NodePrevE
  - SetValE
  - SubNextE
  - SubPrevE
  - TpE
  - ValE

Please see the Easy API example below for usage.

# Simple API

The simple API provides a better one-to-one mapping to the underlying C API, and
provides better performance at the cost of convenience. These functions are mostly
encapsulated in the BufferT, BufferTArray, and KeyT data structures, with the only function
belonging to this API existing outside of these data types, being LockST.

The structures in the Simple API include anchors for C
allocated memory that need to be freed when the structures go out of scope. There are
automated "Finalizers" that will perform these frees when the structures are garbage
collected but if any really large buffers (or many little ones) are allocated,
an application may have better memory performance (i.e. smaller working set) if the structures are intentionally freed
by using the struct.Free() methods. These structure frees can be setup in advance using defer statements when
the structures are allocated. For example, the Easy API creates these structures and buffers for most
calls and specifically releases them at the end of each call for exactly this reason.
If the creation of these blocks is infrequent, they can be left to be handled in an automated fashion.
Note - freeing a never allocated or already freed structure does NOT cause an error - it is ignored.

Please see the Simple API example below for usage.

# Transactions in YottaDB

YottaDB implements strong ACID transactions see https://en.wikipedia.org/wiki/ACID_(computer_science).
Please review the documentation related to transactions in YottaDB at https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#transaction-processing

To use YottaDB transactions in Go, please see the Transaction Example below for further information.

# Go Error Interface

YottaDB has a comprehensive set of error return codes. Each has a unique
number and a mnemonic. Thus, for example, to return an error that a
buffer allocated for a return value is not large enough, YottaDB uses
the INVSTRLEN error code, which has the value
yottadb.YDB_ERR_INVSTRLEN. YottaDB attempts to maintain
the stability of the numeric values and mnemonics from release to release,
to ensure applications remain compatible when the underlying YottaDB
releases are upgraded. The Go "error" interface
provides for a call to return an error as a string (with
"nil" for a successful return).
*/
package yottadb
