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

// This package is a Go wrapper for a YottaDB database using the SimplaAPI interface.
// It requires Go 1.24 to achieve best speed and its use of AddCleanup() instead of SetFinalizer().
//
// This wrapper uses 'cgo' to interface between this Go wrapper and the YottaDB engine written in C.
// Its use of the `node` type to pin memory references to database subscript strings gives it optimal speed.

package yottadb

import (
	"sync/atomic"
	"time"
)

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
import "C"

// ---- Release version constants - be sure to change all of them appropriately

// WrapperRelease - (string) The Go wrapper release version for YottaDB SimpleAPI. Note the third piece of this version
// will be even for a production release and odd for a development release. When released, depending
// on new content, either the third piece of the version will be bumped to an even value or the second piece of the
// version will be bumped by 1 and the third piece of the version set to 0. On rare occasions, we may bump the first
// piece of the version and zero the others when the changes are significant.
const WrapperRelease string = "v1.2.6"

// MinimumYDBRelease - (string) Minimum YottaDB release name required by this wrapper
// This is checked on init
const MinimumYDBRelease string = "r1.34"

// MinimumGoRelease - Minimum version of Go to fully support this wrapper (including tests)
//
//	go 1.24 required to run tests: testing.Loop
//	go 1.23 required for iterators, used to iterate database subscripts
//	go 1.22 required for the range clause
//	go 1.19 required for sync/atomic -- safer than
//
// Note: this is not checked at runtime. The compile will fail if undefined Go features are used
const MinimumGoRelease string = "go1.24"

// ---- Wait times

const defaultMaximumPanicExitWait time.Duration = 3   // wait in seconds
const defaultMaximumNormalExitWait time.Duration = 60 // wait in seconds
const defaultMaximumSigShutDownWait time.Duration = 5 // wait in seconds
const defaultMaximumSigAckWait time.Duration = 10     // wait in seconds

// MaximumPanicExitWait is the maximum wait when a panic caused by a signal has occured (likely unable to run Exit().
// It specifies the wait in seconds that yottadb.Exit() will wait for ydb_exit() to run before
// giving up and forcing the process to exit. Note the normal exit wait is longer as we expect ydb_exit() to be
// successful so can afford to wait as long as needed to do the sync but for a signal exit, the rundown is likely
// already done (exit handler called by the signal processing itself) but if ydb_exit() is not able to get
// the system lock and is likely to hang, 3 seconds is about as much as we can afford to wait.
var MaximumPanicExitWait time.Duration = defaultMaximumPanicExitWait

// MaximumNormalExitWait is maximum wait for a normal shutdown when no system lock hang in Exit() is likely.
var MaximumNormalExitWait time.Duration = defaultMaximumNormalExitWait

// MaximumSigShutDownWait is maximum wait to close down signal handling goroutines (shouldn't take this long).
var MaximumSigShutDownWait time.Duration = defaultMaximumSigShutDownWait

// MaximumSigAckWait is maximum wait for notify via acknowledgement channel that a notified signal handler is
// done handling the signal.
var MaximumSigAckWait time.Duration = defaultMaximumSigAckWait

// ---- Enums for signal functions

// YDBHandlerFlag type is the flag type passed to yottadb.RegisterSignalNotify() to indicate when or whether the driver
// should run the YottaDB signal handler. It must be set to one of the constants defined below.
type YDBHandlerFlag int

const (
	NotifyBeforeYDBSigHandler    YDBHandlerFlag = iota + 1 // Request sending notification BEFORE running YDB signal handler
	NotifyAfterYDBSigHandler                               // Request sending notification AFTER running YDB signal handler
	NotifyAsyncYDBSigHandler                               // Notify user and run YDB handler simultaneously (non-fatal signals only)
	NotifyInsteadOfYDBSigHandler                           // Do the signal notification but do NOT drive the YDB handler
)

const dbgSigHandling bool = false // Print extra info when running if true

// ydbEntryPoint is passed to validateNotifySignal() to indicate which entry point is calling it. Set it to constants below.
type ydbEntryPoint uint32

const (
	ydbEntryRegisterSigNotify ydbEntryPoint = iota + 1
	ydbEntryUnRegisterSigNotify
)

// ---- Init locks

var ydbInitialized atomic.Bool    // Set to true when YDB has been initialized with a call to ydb_main_lang_init()
var ydbSigPanicCalled atomic.Bool // True when our exit is panic driven due to a signal

// ---- Debug settings

const dbgPrintEPHdrs bool = false // Print entry point headers when routine is entered

//go:generate ../scripts/gen_error_codes.sh
