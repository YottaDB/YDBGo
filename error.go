//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2020 YottaDB LLC and/or its subsidiaries.	//
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
	"fmt"
	"unsafe"
)

// #include <stdlib.h>
// #include "libyottadb.h"
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Define error related structure/method
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// YDBError is a structure that defines the error message format which includes both the formated $ZSTATUS
// type message and the numeric error value.
type YDBError struct {
	errcode int    // The error value (e.g. YDB_ERR_DBFILERR, etc)
	errmsg  string // The error string - generally from $ZSTATUS when available
}

// Error is a method to return the expected error message string.
func (err *YDBError) Error() string {
	return err.errmsg
}

// ErrorCode is a function used to find the error return code.
func ErrorCode(err error) int {
	yerr, ok := err.(*YDBError)
	if ok {
		rc := yerr.errcode
		return rc
	}
	return -1
}

// NewError is a function to create a new YDBError and return it. Note that we use ydb_zstatus() instead of
// using (for example) GetE() to fetch $ZSTATUS because ydb_zstatus does not require a tptoken. This means
// that we don't need to pass tptoken to all the data access methods (For example, ValStr()).
//
func NewError(tptoken uint64, errstr *BufferT, errnum int) error {
	var msgptr *C.char
	var errmsg string
	var err error

	if YDB_TP_RESTART == errnum {
		// Shortcut for this performance sensitive error - not a user error
		return &YDBError{errnum, "TPRESTART"}
	}
	if YDB_ERR_NODEEND == errnum {
		// Another common "error" that needs no message
		return &YDBError{errnum, "NODEEND"}
	}
	if YDB_TP_ROLLBACK == errnum {
		// Our 3rd and final quickie-check that needs no message
		return &YDBError{errnum, "ROLLBACK"}
	}
	if (nil != errstr) && (nil != errstr.getCPtr()) {
		errmsg = C.GoString((*C.char)(errstr.getCPtr().buf_addr))
	}
	if 0 == len(errmsg) {
		// No message was supplied for the error - see if we can find one via MessageT()
		errmsg, err = MessageT(tptoken, errstr, errnum)
		if nil != err {
			if YDB_ERR_CALLINAFTERXIT != ErrorCode(err) {
				panic(fmt.Sprintf("YDB: Unable to find error text for error code %d with error %s", errnum, err))
			}
			// We had a CALLINAFTERXIT error from MessageT() - treat it as a nil message
			errmsg = ""
		}
		if 0 == len(errmsg) {
			// MessageT() did not return anything interesting so Fetch $ZSTATUS to return as the error string.
			// This has the advantage that CALLINAFTERXIT cannot happen but we are pulling from $ZSTATUS which is
			// a global value in the YottaDB engine that can be changed by any thread so this is last choice. The
			// advantage is that its message would be fully populated with parameter substituions of error codes,
			// filenames, etc.
			//
			// Note that MessageT() would have taken care of making sure the engine is initialized before this call.
			msgptr = (*C.char)(allocMem(C.size_t(YDB_MAX_ERRORMSG)))
			C.ydb_zstatus(msgptr, C.int(YDB_MAX_ERRORMSG))
			errmsg = C.GoString((*C.char)(msgptr))
			freeMem(unsafe.Pointer(msgptr), C.size_t(YDB_MAX_ERRORMSG))
			if 0 == len(errmsg) {
				// We couldn't find text to do with this message so at least make sure we know what the error
				// code is.
				errmsg = fmt.Sprintf("YDB-E-UNKNOWNMSG Unknown error message code: %d", errnum)
			}
		}
	}
	return &YDBError{errnum, errmsg}
}
