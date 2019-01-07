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

package yottadb // import "lang.yottadb.com/go/yottadb"

// The following comment block is a Golang/cgo preamble defining C related things we need in this package

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
import "C"

// NOTTP contains the tptoken value to use when NOT in a TP transaction callback routine.
const NOTTP uint64 = 0

const debugFlag bool = false

var easyAPIDefaultDataSize uint32 = 256  // Init value - may grow - Base allocation for returned data values
var easyAPIDefaultSubscrCnt uint32 = 10  // Init value - may grow - Base subscript count allocation for returned subscr list
var easyAPIDefaultSubscrSize uint32 = 32 // Init value - may grow - Base subscript size allocation for returned subscr list

//go:generate ./scripts/gen_error_codes.sh
