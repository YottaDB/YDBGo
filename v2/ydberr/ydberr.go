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

// ydberr contains YottaDB constants generated from YottaDB source.
package ydberr

// The above line with the following import lets errorcodes.go find libyottadb

// #cgo pkg-config: yottadb
import "C"

// yottadb.Error codes for use by YDBGo. These are positive numbers as negative numbers are used by YottatDB (see errorcodes.go).
const (
	Init                         = iota + 1
	Shutdown                     // Error during shutdown
	ShutdownIncomplete           // Shutdown is incomplete
	InvalidStringLength          // String is too long. Same as YottaDB INVSTRLEN but caught in YDBGo
	InvalidValueType             // Value type is not a string or number
	InvalidSubscriptIndex        // Subscript index is out of bounds
	OutOfMemory                  // Not enough memory for attempted allocation
	YDBMessageInvalid            // Failure decoding message prefixes from YottaDB
	YDBMessageRecoveryFailure    // Failure calling ydb_message_t() to get a YottaDB message matching a code
	Syslog                       // Error trying to write a syslog entry
	SignalUnsupported            // Specified signal is not supported for YottaDB-specific signal notification
	SignalHandling               // Error during signal handling
	SignalFatal                  // Fatal signal occurred
	CallbackWrongGoroutine       // Callback invoked from the wrong goroutine
	Variadic                     // Programmer error in Variadic call
	IncrementEmpty               // Cannot increment by the empty string
	IncrementReturnInvalid       // ydb_incr_st return a non-floating-point string
	MCallNotFound                // M-call table does not contain the specified M routine
	MCallNil                     // Tried to call a nil M routine
	MCallWrongNumberOfParameters // Wrong number of parameters pass to M routine call
	MCallTypeMismatch            // Type mismatch
	MCallTypeUnhandled           // Type not handled for YDBGo M calls
	MCallTypeUnknown             // Unknown type incorrectly reached mCall() - YDBGo error
	MCallTypeMissing             // Type not supplied
	MCallBadAsterisk             // Asterisk should be at the beginning, not the end, for a Go type
	MCallPreallocRequired        // Preallocation must be specified after *string type in M-call file
	MCallPreallocInvalid         // Preallocation specified for a non-string type
	MCallInvalidPrototype        // Line in an M-call file invalid format
	MCallEntrypointInvalid       // Invalid characters in entrypoint name
	MCallTooManyParameters       // More than YDB_MAX_PARMS parameters
	ImportRead                   // Could not read call-in table file
	ImportParse                  // Import parsing error that wraps other parsing errors
	ImportTemp                   // Error opening temporary call-in file
	ImportOpen                   // ydb_ci_tab_open_t error when YottaDB tried to open temp import table
	InvalidZwriteFormat          // String supplied to ZwrStr() contains invalid Zwrite format
)
