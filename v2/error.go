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

package yottadb

import (
	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// ---- Error type to contain/manage wrapper errors

// Error is a structure that defines the error message format which includes both the formated $ZSTATUS
// type message and the numeric error value.
type Error struct {
	code    int    // The error value (e.g. ydberr.DBFILERR, etc)
	message string // The error string - generally from $ZSTATUS when available
}

// Error is a type method of yottadb.Error to return the error message string.
func (err *Error) Error() string {
	return err.message
}

// Code is a type method of yottadb.Error to return the error status code.
// If err was supplied as a type error, then you'll need to cast it to type *Error as follows
//
//	status := err.(*Error).Code()
func (err *Error) Code() int {
	return err.code
}

// newError returns error code and message as a yottadb.Error error type.
func newError(code int, message string) error {
	return &Error{code, message}
}

// ---- Simulate YDB error messages for certain Go-specific error conditions

// ydbGoErrors is a map of error messages for the Go-specific set of errors.
// These are sent to syslog, so are formatted in the same way as other YDB messages to syslog.
var ydbGoErrors = map[int]string{
	-ydberr.DBRNDWNBYPASS:   "%YDB-W-DBRNDWNBYPASS, YottaDB database rundown may have been bypassed due to timeout - run MUPIP JOURNAL ROLLBACK BACKWARD / MUPIP JOURNAL RECOVER BACKWARD / MUPIP RUNDOWN",
	-ydberr.SIGACKTIMEOUT:   "%YDB-E-SIGACKTIMEOUT, Signal completion acknowledgement timeout: !AD",
	-ydberr.SIGGORTNTIMEOUT: "%YDB-W-ERR_SIGGORTNTIMEOUT, Shutdown of signal goroutines timed out",
}

// getWrapperErrorMsg returns a Go-specific YottaDB-formatted error message given its error number.
// If the error is not found in local list map of Go-specific errors, an empty string is returned.
func getWrapperErrorMsg(errNum int) string {
	if 0 > errNum {
		errNum = -errNum
	}
	return ydbGoErrors[errNum]
}
