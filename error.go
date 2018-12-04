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

package yottadb

import (
	"unsafe"
)

// #include <stdlib.h>
// #include "libyottadb.h"
// #include "libydberrors.h"
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Define error related structure/method
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// Define our error message format that includes both the formated $ZSTATUS type message plus the error value.
type YDBError struct {
	errcode int    // The error value (e.g. C.YDB_OK, C.YDB_ERR_DBFILERR, etc)
	errmsg  string // The error string - generally from $ZSTATUS if errcode is not C.YDB_OK - else nil
}

// Error() is a method to return the string out of our error like is normally expected.
func (err *YDBError) Error() string {
	return err.errmsg
}

// ErrorCode() is a function we use to find the error return code.
func ErrorCode(err error) int {
	yerr, ok := err.(*YDBError)
	if ok {
		rc := yerr.errcode
		return rc
	}
	return -1
}

// NewError() is a functione to create a new YDBError and return it. Note we use ydb_zstatus() instead of
// using (for example) GetE() to fetch $ZSTATUS because ydb_zstatus does not require a tptoken so means
// that we don't need to pass tptoken to all the data access methods (GetValStr() for example).
//
func NewError(errnum int) error {
	var msgptr *C.char

	if (int)(C.YDB_ERR_TPRESTART) == errnum {
		// Shortcut for this performance sensitive error - not a user error
		return &YDBError{errnum, "TPRESTART"}
	}
	// If we are seeing C.YDB_ERR_SIMPLEAPINEST sequentially, the likelihood is we are in an error
	// loop which we need to break out of with a panic.
	if ((int)(C.YDB_ERR_SIMPLEAPINEST) == errnum) && ((int)(C.YDB_ERR_SIMPLEAPINEST) == lastErrorRaised) {
		panic("YDB: Detected a SIMPLEAPINEST error loop")
	}
	lastErrorRaised = errnum
	// Fetch $ZSTATUS to return as the error string
	msgptr = (*C.char)(C.malloc(C.size_t(C.YDB_MAX_ERRORMSG)))
	C.ydb_zstatus(msgptr, C.int(C.YDB_MAX_ERRORMSG))
	errmsg := C.GoString((*C.char)(msgptr))
	C.free(unsafe.Pointer(msgptr))
	return &YDBError{errnum, errmsg}
}
