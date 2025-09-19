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

// ---- Error types for YottaDB errors, YDBGo Init errors, and YDBGo Signal errors

package yottadb

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// #include "libyottadb.h"
import "C"

// Error type holds YDBGo and YottaDB errors including a numeric error code.
// YDBGo error strategy is as follows. Database setup functions like Init, Import (and its returned functions) return errors.
// However, Node functions panic on errors because Node errors are caused by either programmer blunders (like invalid variable name)
// or system-level events (like out of memory). This approach greatly simplifies the use of Node methods.
//
// If a particular panic needs to be captured this can be done with recover as YDBGo ensures that all its errors and panics
// are of type yottadb.Error to facilitate capture of the specific cause using the embedded code:
//   - All YottaDB errors are formated in $ZSTATUS format message and the YottaDB numeric error code with negative value,
//     defined in ydberr/errorscodes.go.
//   - All YDBGo errors likewise have a message string, but have a positive error code, defined in ydberr.ydberr.go.
//
// The yottadb.Error type implements the method [yottatdb.Error.Is] so you can identify a specific error with [ErrorIs](err, ydberr.<ErrorCode>).
type Error struct {
	Message string  // The error string - generally from $ZSTATUS when available
	Code    int     // The error value (e.g. ydberr.INVSTRLEN, etc)
	chain   []error // Lists any errors wrapped by this one
}

// Error is a type method of [yottadb.Error] to return the error message string.
func (err *Error) Error() string {
	return err.Message // The error code's name is already included in the message from YottaDB, so don't add the code
}

// Unwrap allows yottadb.Error to wrap other underlying errors. See [errors.Unwrap].
func (err *Error) Unwrap() []error {
	return err.chain
}

// Is lets [errors.Is]() search an error or chain of wrapped errors for a yottadb.Error with a matching ydberr code.
// See [ErrorIs]() for a more practical way to use this capability.
// Only the error code is matched, not the message: this supports matching errors even when YottaDB messages vary.
func (err *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && err.Code == t.Code
}

// ErrorIs uses [errors.Is]() to search an error or chain of wrapped errors for a yottadb.Error with a matching ydberr code.
// Only the error code is matched, not the message, to support YottaDB error messages that vary.
// For example, to test for YottaDB INVSTRLEN error:
//
//	if yottadb.ErrorIs(err, ydberr.INVSTRLEN) {
//
// is a short equivalent of:
//
//	if errors.Is(err, &Error{Code: ydberr.INVSTRLEN}) {
//
// It differs from a simple type test using yottadb.Error.Code in that it searches for a match in the entire chain of wrapped errors
func ErrorIs(err any, code int) bool {
	err2, ok := err.(error)
	return ok && errors.Is(err2, &Error{Code: code})
}

// newError returns error code and message as a [yottadb.Error] error type.
// Any errors supplied in wrapErrors are wrapped in the returned error and their messages appended (after a colon) to the given message.
// For YDBGo error strategy see [yottadb.Error]
func newError(code int, message string, wrapErrors ...error) error {
	for _, err := range wrapErrors {
		newMessage := err.Error()
		if newMessage != "" {
			message = message + ": " + newMessage
		}
	}
	return &Error{Code: code, Message: message, chain: wrapErrors}
}

// errorf same as fmt.Errorf except that it returns error type yottadb.Error with specified code; and doesn't handle %w.
func errorf(code int, format string, args ...any) error {
	return newError(code, fmt.Sprintf(format, args...))
}

// ---- Error Functions dependent on yottadb.Conn to fetch message strings from YottaDB

// getErrorString returns a copy of conn.cconn.errstr as a Go string.
func (conn *Conn) getErrorString() string {
	// len_used should never be greater than len_alloc since all errors should fit into errstr, but just in case, take the min
	errstr := conn.cconn.errstr
	r := C.GoStringN(errstr.buf_addr, C.int(min(errstr.len_used, errstr.len_alloc)))
	runtime.KeepAlive(conn) // ensure conn sticks around until we've finished copying data from it's C allocation
	return r
}

// lastError returns, given error code, the ydb error message stored by the previous YottaDB call as an error type or nil if there was no error.
// If you don't know the code, call lastCode()
func (conn *Conn) lastError(code C.int) error {
	if code == C.YDB_OK {
		return nil
	}
	// The next two cases duplicate code in recoverMessage but are performance-critical so are checked here:
	if code == YDB_TP_RESTART {
		return newError(int(code), "YDB_TP_RESTART")
	}
	if code == YDB_TP_ROLLBACK {
		return newError(int(code), "YDB_TP_ROLLBACK")
	}
	msg := conn.getErrorString()
	if msg == "" { // See if msg is still empty (we set it to empty before calling the API in conn.prepAPI()
		// This code gets run, for example, if ydb_exit() is called before a YDB function is invoked
		// causing the YDB function to exit without filling conn.cconn.errstr
		return newError(int(code), conn.recoverMessage(code))
	}

	pattern := ",(SimpleThreadAPI),"
	index := strings.Index(msg, pattern)
	if index == -1 {
		// If msg is improperly formatted, return it verbatim as an error with code YDBMessageInvalid (this should never happen with messages from YottaDB)
		return errorf(ydberr.YDBMessageInvalid, "could not parse YottaDB error message: %s", msg)
	}
	text := msg[index+len(pattern):]
	return newError(int(code), text)
}

// lastCode extracts the ydb error status code from the message stored by the previous YottaDB call.
func (conn *Conn) lastCode() C.int {
	msg := conn.getErrorString()
	if msg == "" {
		// if msg is empty there was no error because we set it to empty before calling the API in conn.prepAPI()
		return C.int(YDB_OK)
	}

	// Extract the error code from msg
	index := strings.Index(msg, ",")
	if index == -1 {
		// If msg is improperly formatted, panic it verbatim with an YDBMessageInvalid code (this should never happen with messages from YottaDB)
		panic(errorf(ydberr.YDBMessageInvalid, "could not parse YottaDB error message: %s", msg))
	}
	code, err := strconv.ParseInt(msg[:index], 10, 64)
	if err != nil {
		// If msg has no number, panic it verbatim with an YDBMessageInvalid code (this should never happen with messages from YottaDB)
		panic(errorf(ydberr.YDBMessageInvalid, "could not recover error code from YottaDB error message: %s", msg))
	}
	return C.int(-code)
}

// recoverMessage tries to get the error message (albeit without argument substitution) from the supplied status code in cases where the message has been lost.
// Note: lastCode() may not be called after recoverMessage() because recoverMessage clobbers cconn.errstr when it calls ydb_message_t
func (conn *Conn) recoverMessage(status C.int) string {
	cconn := conn.cconn
	// Check special cases first.
	switch status {
	// Identify certain return codes that are not identified by ydb_message_t().
	// I have only observed YDB_TP_RESTART being returned, but include the others just in case.
	case YDB_TP_RESTART:
		return "YDB_TP_RESTART"
	case YDB_TP_ROLLBACK:
		return "YDB_TP_ROLLBACK"
	case YDB_NOTOK:
		return "YDB_NOTOK"
	case YDB_LOCK_TIMEOUT:
		return "YDB_LOCK_TIMEOUT"
	case YDB_DEFER_HANDLER:
		return "YDB_DEFER_HANDLER"
	case ydberr.THREADEDAPINOTALLOWED:
		// This error will prevent ydb_message_t() from working below, so instead return a hard-coded error message.
		return "%YDB-E-THREADEDAPINOTALLOWED, Process cannot switch to using threaded Simple API while already using Simple API"
	case ydberr.CALLINAFTERXIT:
		// The engine is shut down so calling ydb_message_t will fail if we attempt it so just hard-code this error return value.
		return "%YDB-E-CALLINAFTERXIT, After a ydb_exit(), a process cannot create a valid YottaDB context"
	}
	// note: ydb_message_t() only looks at the absolute value of status so no need to negate it
	conn.prepAPI()
	rc := C.ydb_message_t(C.uint64_t(conn.tptoken.Load()), nil, status, &cconn.errstr)
	if rc != YDB_OK {
		if rc == ydberr.UNKNOWNSYSERR {
			panic(errorf(int(rc), "%%YDB-E-UNKNOWNSYSERR, [%d (%#x) returned by ydb_* C API] does not correspond to a known YottaDB error code", status, status))
		}
		if conn.getErrorString() == "" {
			// Do not call lastError if there is no message because it will infinitely recurse back to here to get the message
			// Pretty hard to work out how to coverage-test this error
			panic(errorf(int(rc), "ydb_message_t() returned YottaDB error code %d (%#x) when trying to get the message for error %d (%#x)", rc, rc, status, status))
		}
		err := conn.lastError(rc)
		panic(newError(ydberr.YDBMessageRecoveryFailure, fmt.Sprintf("error %d when trying to recover error message for error %d using ydb_message_t()", int(rc), int(-status)), err))
	}
	return conn.getErrorString()
}
