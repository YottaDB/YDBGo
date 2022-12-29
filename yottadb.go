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

package yottadb

import "time"

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

// DefaultMaximumPanicExitWait is default/initial value for MaximumPanicExitWait
const DefaultMaximumPanicExitWait time.Duration = 3 // wait in seconds
// MaximumPanicExitWait is the maximum wait when a panic caused by a signal has occured (unlikely able to run Exit()
var MaximumPanicExitWait time.Duration = DefaultMaximumPanicExitWait

// DefaultMaximumNormalExitWait is default/initial value for MaximumNormalExitWait
const DefaultMaximumNormalExitWait time.Duration = 60 // wait in seconds
// MaximumNormalExitWait is maximum wait for a normal shutdown when no system lock hang in Exit() is likely
var MaximumNormalExitWait time.Duration = DefaultMaximumNormalExitWait

// DefaultMaximumSigShutDownWait is default/initial value for MaximumSigShutDownWait
const DefaultMaximumSigShutDownWait time.Duration = 5 // wait in seconds
// MaximumSigShutDownWait is maximum wait to close down signal handling goroutines (shouldn't take this long)
var MaximumSigShutDownWait time.Duration = DefaultMaximumSigShutDownWait

// DefaultMaximumSigAckWait is default/initial value for MaximumSigAckWait
const DefaultMaximumSigAckWait time.Duration = 10 // wait in seconds
// MaximumSigAckWait is maximum wait for notify via acknowledgement channel that a notified signal handler is
// done handling the signal.
var MaximumSigAckWait time.Duration = DefaultMaximumSigAckWait

// Release version constants - be sure to change all of them appropriately

// WrapperRelease - (string) The Go wrapper release value for YottaDB SimpleAPI. Note the third piece of this version
// will be even for a production release and odd for a development release (branch develop). When released, depending
// on new content, either the third piece of the version will be bumped to an even value or the second piece of the
// version will be bumped by 1 and the third piece of the version set to 0. On rare occasions, we may bump the first
// piece of the version and zero the others when the changes are significant.
const WrapperRelease string = "v1.2.2"

// MinimumYDBReleaseMajor - (int) Minimum major release number required by this wrapper of the linked YottaDB
const MinimumYDBReleaseMajor int = 1

// MinimumYDBReleaseMinor - (int) Minimum minor release number required by this wrapper of the linked YottaDB
const MinimumYDBReleaseMinor int = 34

// MinimumYDBRelease - (string) Minimum YottaDB release name required by this wrapper
const MinimumYDBRelease string = "r1.34"

// MinimumGoRelease - (string) Minimum version of Go to fully support this wrapper (including tests)
const MinimumGoRelease string = "go1.13"

// YDBHandlerFlag type is the flag type passed to yottadb.RegisterSignalNotify() to indicate when or if the driver should run the
// YottaDB signal handler.
type YDBHandlerFlag int

// Use iota to get enum like auto values starting at 1
const (
	// NotifyBeforeYDBSigHandler - Request sending notification BEFORE running YDB signal handler
	NotifyBeforeYDBSigHandler YDBHandlerFlag = iota + 1
	// NotifyAfterYDBSigHandler - Request sending notification AFTER running YDB signal handler
	NotifyAfterYDBSigHandler
	// NotifyAsyncYDBSigHandler - Notify user and run YDB handler simultaneously (non-fatal signals only)
	NotifyAsyncYDBSigHandler
	// NotifyInsteadOfYDBSigHandler - Do the signal notification but do NOT drive the YDB handler
	NotifyInsteadOfYDBSigHandler
)

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
var ydbSigPanicCalled uint32 // Atomic: True when our exit is panic driven due to a signal
var inInit uint32            // Atomic: We are in initializeYottaDB() so don't force re-init in ValE()

//go:generate ./scripts/gen_error_codes.sh
