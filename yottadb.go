//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2019 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb // import "lang.yottadb.com/go/yottadb"

// The following comment block is a Golang/cgo preamble defining C related things we need in this package

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
import "C"

// NOTTP contains the tptoken value to use when NOT in a TP transaction callback routine.
const NOTTP uint64 = 0

// Release version constants - be sure to change all of them appropriately

// WrapperRelease - (string) The Golang wrapper release value for YottaDB SimpleAPI
const WrapperRelease string = "v1.0.0"

// MinimumYDBReleaseMajor - (int) Minimum major release number required by this wrapper of the linked YottaDB
const MinimumYDBReleaseMajor int = 1

// MinimumYDBReleaseMinor - (int) Minimum minor release number required by this wrapper of the linked YottaDB
const MinimumYDBReleaseMinor int = 28

// MinimumYDBRelease - (string) Minimum YottaDB release name required by this wrapper
const MinimumYDBRelease string = "r1.28"

const dbgPrintEPHdrs bool = false    // Print entry point headers when routine is entered
const dbgInitMalloc bool = false     // Initialize C malloc'd storage (already initialized to zeroes)
const dbgInitMallocChar C.int = 0xff // Single byte value that malloc'd storage is set to
const dbgInitFree bool = false       // (Re)Initialize C malloc code on free to prevent use after free
const dbgInitFreeChar C.int = 0xfe   // Char to initialize released memory to

var easyAPIDefaultDataSize uint32 = 256  // Init value - may grow - Base allocation for returned data values
var easyAPIDefaultSubscrCnt uint32 = 10  // Init value - may grow - Base subscript count allocation for returned subscr list
var easyAPIDefaultSubscrSize uint32 = 32 // Init value - may grow - Base subscript size allocation for returned subscr list

//go:generate ./scripts/gen_error_codes.sh
