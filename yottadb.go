//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2022 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

// This package is a Go wrapper for a YottaDB database using the SimplaAPI interface. While the functional part
// of this package runs OK with Go 1.10, certain external packages are used by the internal unit tests invoked
// by 'go test' that have been updated to use facilities only present in Go 1.13.0 and later.
//
// This wrapper makes significant use of the 'cgo' facility to interface between this Go wrapper and the YottaDB
// engine written in C. The cgo facility is described here: https://golang.org/cmd/cgo - Note that cgo imposes
// a number of restrictions that the wrapper works very hard to work around - no array subscript references to C
// values, no passing of Go routine pointers to C code, no variadic calls, plus restrictions on parameters. But
// cgo allows us full access to C structures, enums, routines declarations, and simple #define values just by
// including the appropriate C header file and invoking cgo in the prescribed manner as shown below.

package yottadb // import "lang.yottadb.com/go/yottadb"

// The following comment block is a Go/cgo preamble defining C related things we need in this package

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
import "C"

// NOTTP contains the tptoken value to use when NOT in a TP transaction callback routine.
const NOTTP uint64 = 0

// Maximum panic exit wait specifies the wait in seconds that yottadb.Exit() will wait for ydb_exit() to run before
// giving up and forcing the process to exit. Note the normal exit wait is longer as we expect ydb_exit() to be
// successful so can afford to wait as long as needed to do the sync but for a signal exit, the rundown is likely
// already done (exit handler called by the signal processing itself) but if ydb_exit() is not able to get
// the system lock and is likely to hang, 3 seconds is about as much as we can afford to wait.

// MaximumPanicExitWait is maximum wait when a panic caused by a signal has occured (unlikely able to run Exit())
const MaximumPanicExitWait int = 3 // wait in seconds

// MaximumNormalExitWait is maximum wait for a normal shutdown when no system lock hang in Exit() is likely
const MaximumNormalExitWait int = 60 // wait in seconds

// MaximumCloseWait is maximum wait to close down signal handling goroutines (shouldn't take this long)
const MaximumCloseWait int = 5 // wait in seconds

// Release version constants - be sure to change all of them appropriately

// WrapperRelease - (string) The Go wrapper release value for YottaDB SimpleAPI. Note the third piece of this version
// will be even for a production release and odd for a development release (branch develop). When released, depending
// on new content, either the 3rd digit with be bumped to an even value or the second value will be bumped by 1 and the
// third value set to 0.
const WrapperRelease string = "v1.1.1"

// MinimumYDBReleaseMajor - (int) Minimum major release number required by this wrapper of the linked YottaDB
const MinimumYDBReleaseMajor int = 1

// MinimumYDBReleaseMinor - (int) Minimum minor release number required by this wrapper of the linked YottaDB
const MinimumYDBReleaseMinor int = 30

// MinimumYDBRelease - (string) Minimum YottaDB release name required by this wrapper
const MinimumYDBRelease string = "r1.33"

// MinimumGoRelease - (string) Minimum version of Go to fully support this wrapper (including tests)
const MinimumGoRelease string = "go1.13"

const dbgPrintEPHdrs bool = false    // Print entry point headers when routine is entered
const dbgInitMalloc bool = false     // Initialize C malloc'd storage (already initialized to zeroes)
const dbgInitMallocChar C.int = 0xff // Single byte value that malloc'd storage is set to
const dbgInitFree bool = false       // (Re)Initialize C malloc code on free to prevent use after free
const dbgInitFreeChar C.int = 0xfe   // Char to initialize released memory to
const dbgSigHandling bool = false    // Print extra info when running if true

const easyAPIDefaultDataSize uint32 = C.DEFAULT_DATA_SIZE     // Base allocation for returned data values
const easyAPIDefaultSubscrCnt uint32 = C.DEFAULT_SUBSCR_CNT   // Base subscript count allocation for returned subscr list
const easyAPIDefaultSubscrSize uint32 = C.DEFAULT_SUBSCR_SIZE // Base subscript size allocation for returned subscr list

var ydbInitialized uint32    // Atomic: Set to 1 when YDB has been initialized with a call to ydb_main_lang_init()
var ydbSigPanicCalled uint32 // True when our exit is panic drive due to a signal

//go:generate ./scripts/gen_error_codes.sh
