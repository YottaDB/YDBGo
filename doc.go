//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

/*
Package yottadb provides a Go wrapper for YottaDB, a high performance, transactional, NoSQL engine with proven speed and stability.

YottaDB Quick Start

Before starting, consider reading the introduction to YottaDB's data model at https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#concepts

The YottaDB Go wrapper requires a minimum YottaDB version of r1.24 and
is tested with a minimum Go version of 1.6.2. If the Golang packages on
your operating system are older, and the Go wrapper does not work,
please obtain and install a newer Golang implementation.

This quickstart assumes that YottaDB has already been installed as
described in at https://yottadb.com/product/get-started/. 

After installing YottaDB, install the Go wrapper:

    get get lang.yottadb.com/go/yottadb

Easy API

The easy API provides a set of functions which are very easy to use at the expense of
some additional copies for each operation. These functions all end with the letter 'E',
and are available in the yottadb package. They include:

    - DataE
    - DeleteE
    - DeleteExclE
    - IncrE
    - LockDecrE
    - LockE
    - NodeNextE
    - NodePrevE
    - SetValE
    - SubNextE
    - SubPrevE
    - TpE
    - ValE

Please see the Easy API example below for usage.

Simple API

The simple API provides a better one-to-one mapping to the underlying C API, and
provides better performance at the cost of convenience. These functions are mostly
encapsulated in the BufferT, BufferTArray, and KeyT data types, with the only function
beloning to this API existing outside of these data types being LockST.

When using any of the structure from the Simple API, it is very important to ensure that
myvar.Free() gets called on each of the allocated structures. The structures allocate underlying
C structures which Go does not know how to free, and if Free() is not called that memory will
be leaked.

Please see the Simple API example below for usage.

Transactions in YottaDB

Transactions provide a way to ensure that a series of database operations is all or nothing.
Please review the documentation related to transactions in YottaDB at https://docs.yottadb.com/MultiLangProgGuide/MultiLangProgGuide.html#transaction-processing

To use YottaDB transactions in Go, some "glue code" must be generated for each Go routine callback.
Please see the Transaction Example below for further information.

Go Error Interface

YottaDB has a comprehensive set of error return codes. Each has a unique
number and a mnemonic. Thus, for example, to return an error that a
buffer allocated for a return value is not large enough, YottaDB uses
the INVSTRLEN error code, which has the numeric value
``C.YDB_ERR_INVSTRLEN``. YottaDB attempts to maintain
stability of the numeric values and mnemonics from release to release,
to ensure applications remain compatible when the underlying YottaDB
releases are upgraded. Go ``error`` interface
provides for a call to return an error as a string (with
``nil`` for a successful return).
*/
package yottadb
