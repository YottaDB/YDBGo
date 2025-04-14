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

// Define C structures to implement Conn and Node types in Go

// Create a goroutine-specific 'connection' object for calling the YottaDB API.
typedef struct conn {
	uint64_t tptoken;	// place to store tptoken for thread-safe ydb_*_st() function calls
	ydb_buffer_t errstr;	// space for YottaDB to return an error string
	ydb_buffer_t value;	// temporary space to store in or out value for get/set

	// variadic parameter (vp) list used to call callg_nc() via ydb_call_variadic_list_func_st().
	// Since this structure's contents contain pointers to C-allocated storage, it is only safe for
	// concurrent access if a new instance is allocated for in each goroutine (hence, connection).
	// It is lazily allocated on demand as not all connections will need it.
	gparam_list *vplist;  // point to per-conn space used for calling variadic C functions like ydb_lock_st()
} conn;

// Create a representation of a database node, including a cache of its subscript strings for fast calls to the YottaDB API.
typedef struct node {
	conn *conn;
	int len;		// number of buffers[] allocated to store subscripts/strings
	int datasize;		// length of string `data` field (all strings and subscripts concatenated)
	int mutable;		// whether the node is mutable (these are only emitted by node iterators)
	ydb_buffer_t buffers;	// first of an array of buffers (typically varname)
	ydb_buffer_t buffersn[];	// rest of array
	// char *data;		// stored after `buffers` (however large they are), which point into this data
} node;
