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

import "fmt"

// ---- YDBError type to contain/manage wrapper errors

// YDBError is a structure that defines the error message format which includes both the formated $ZSTATUS
// type message and the numeric error value.
type YDBError struct {
	Code    int    // The error value (e.g. YDB_ERR_DBFILERR, etc)
	Message string // The error string - generally from $ZSTATUS when available
}

// Error is a method to return the expected error message string.
func (err *YDBError) Error() string {
	return fmt.Sprintf("YDB: %s", err.Message)
}

// NewError returns error code and message as a YDBError error type.
func NewError(code int, message string) error {
	return &YDBError{code, message}
}

// ---- Simulate YDB error messages for certain Go-specific error conditions

// Global constants containing the Go-specific error ids.
const (
	YDB_ERR_STRUCTUNALLOCD  = -151552010
	YDB_ERR_INVLKNMPAIRLIST = -151552018
	YDB_ERR_DBRNDWNBYPASS   = -151552026
	YDB_ERR_SIGACKTIMEOUT   = -151552034
	YDB_ERR_SIGGORTNTIMEOUT = -151552040
)

// ydbGoErrors is a map of error messages for the Go-specific set of errors.
// These are sent to syslog, so are formatted in the same way as other YDB messages to syslog.
var ydbGoErrors = map[int]string{
	-YDB_ERR_STRUCTUNALLOCD:  "%YDB-E-STRUCTNUNALLOCD, Structure not previously called with Alloc() method",
	-YDB_ERR_INVLKNMPAIRLIST: "%YDB-E-INVLKNMPAIRLIST, Invalid lockname/subscript pair list (uneven number of lockname/subscript parameters)",
	-YDB_ERR_DBRNDWNBYPASS:   "%YDB-E-DBRNDWNBYPASS, YDB-W-DBRNDWNBYPASS YottaDB database rundown may have been bypassed due to timeout - run MUPIP JOURNAL ROLLBACK BACKWARD / MUPIP JOURNAL RECOVER BACKWARD / MUPIP RUNDOWN",
	-YDB_ERR_SIGACKTIMEOUT:   "%YDB-E-SIGACKTIMEOUT, Signal completion acknowledgement timeout: !AD",
	-YDB_ERR_SIGGORTNTIMEOUT: "%YDB-W-ERR_SIGGORTNTIMEOUT, Shutdown of signal goroutines timed out",
}

// getWrapperErrorMsg returns a Go-specific YottaDB-formatted error message given its error number.
// If the error is not found in local list map of Go-specific errors, an empty string is returned.
func getWrapperErrorMsg(errNum int) string {
	if 0 > errNum {
		errNum = -errNum
	}
	return ydbGoErrors[errNum]
}
