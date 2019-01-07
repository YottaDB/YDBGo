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
	"fmt"
)

// #include "libyottadb.h"
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Utility routines
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// MessageT is a STAPI utility function to return the error message (sans argument substitution) of a given error number.
func MessageT(tptoken uint64, errstr *BufferT, status int) (string, error) {
	var msgval BufferT

	printEntry("MessageT()")
	defer msgval.Free()
	msgval.Alloc(uint32(C.YDB_MAX_ERRORMSG))
	var cbuft *C.ydb_buffer_t
	if errstr != nil {
		cbuft = errstr.cbuft
	}
	rc := C.ydb_message_t(C.uint64_t(tptoken), cbuft, C.int(status), msgval.cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc), errstr)
		return "", err
	}
	// Returned string should be snug in the retval buffer. Pick it out so can return it as a string
	msgptr, err := msgval.ValStr(tptoken)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with GetValStr(): %s", err))
	}
	return *msgptr, err
}
