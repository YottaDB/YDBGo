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

// See package doc in doc.go

package yottadb

// go 1.24 required for the use of AddCleanup() instead of SetFinalizer(), and to run tests: testing.Loop
// go 1.23 required for iterators, used to iterate database subscripts
// go 1.22 required for the range clause
// go 1.19 required for sync/atomic -- safer than previous options

import (
	"sync/atomic"
	"time"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
import "C"

// ---- Release version constants - be sure to change all of them appropriately

// MinYDBRelease - (string) Minimum YottaDB release name required by this wrapper.
// This is checked on init.
const MinYDBRelease string = "r1.34"

// WrapperRelease - (string) The Go wrapper release version for YottaDB SimpleAPI. Note the third piece of this version
// will be even for a production release and odd for a development release. When released, depending
// on new content, either the third piece of the version will be bumped to an even value or the second piece of the
// version will be bumped by 1 and the third piece of the version set to 0. On rare occasions, we may bump the first
// piece of the version and zero the others when the changes are significant.
// Also, the version numbers may be followed by a hyphen and text, e.g. "v2.0.2-alpha"
const WrapperRelease string = "v2.0.2-alpha4"

// ---- Wait times

// Set default exit wait times. The user may change these.
var (
	// MaxPanicExitWait is the maximum wait when a panic caused by a signal has occurred (likely unable to run Exit().
	// It specifies the wait in seconds that yottadb.Exit() will wait for ydb_exit() to run before
	// giving up and forcing the process to exit. Note the normal exit wait is longer as we expect ydb_exit() to be
	// successful so can afford to wait as long as needed to do the sync but for a signal exit, the rundown is likely
	// already done (exit handler called by the signal processing itself) but if ydb_exit() is not able to get
	// the system lock and is likely to hang, 3 seconds is about as much as we can afford to wait.
	MaxPanicExitWait time.Duration = 3 * time.Second

	// MaxNormalExitWait is maximum wait for a normal shutdown when no system lock hang in Exit() is likely.
	MaxNormalExitWait time.Duration = 60 * time.Second

	// MaxSigShutdownWait is maximum wait to close down signal handling goroutines (shouldn't take this long).
	MaxSigShutdownWait time.Duration = 5 * time.Second

	// MaxSigAckWait is maximum wait for notify via acknowledgement channel that a notified signal handler is
	// done handling the signal.
	MaxSigAckWait time.Duration = 10 * time.Second
)

// ---- Debug settings

// DebugMode greater than zero (1, 2, or 3) increases logging output
//   - DebugMode=0: no debug logging (default)
//   - DebugMode=1: log at entrypoint of M functions or Go signal callbacks; and don't remove temporary callback table file
//   - DebugMode=2: in addition, log extra signal processing info
var DebugMode atomic.Int64 // increasing values 1, 2 or 3 for increasing log output

// ---- Utility functions

// calloc allocates c memory and clears it; a wrapper for C.calloc() that panics on error.
// It must be used instead of C.malloc() if Go will write to pointers within the allocation.
// This is due to documented bug: https://golang.org/cmd/cgo/#hdr-Passing_pointers.
// The user must call C.free() to free the allocation.
func calloc(size C.size_t) unsafe.Pointer {
	// Use calloc: can't let Go store pointers in uninitialized C memory per CGo bug: https://golang.org/cmd/cgo/#hdr-Passing_pointers
	mem := C.calloc(1, size)
	if mem == nil {
		panic(errorf(ydberr.OutOfMemory, "out of memory"))
	}
	return mem
}

//go:generate ../scripts/gen_error_codes.sh ydberr/errorcodes ydbconst ydberr
