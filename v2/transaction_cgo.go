//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

// This file is a CGo workaround: if tpCallbackWrapper is included in conn.go we run into a known Go issue where the
// compiler gives duplicate entry point (or multiple definition) errors. So this routine is placed in its own module.

// #include "libyottadb.h"
import "C"
import (
	"runtime/cgo"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// transactionCallbackWrapper lets Transaction() invoke a Go callback closure from C.
//
//export tpCallbackWrapper
func tpCallbackWrapper(tptoken C.uint64_t, errstr *C.ydb_buffer_t, handle unsafe.Pointer) (retval C.int) {
	h := *(*cgo.Handle)(handle)
	info := h.Value().(*tpInfo) // type assertion never panics because tpCallbackWrapper is only called by Transaction which sets this to tpInfo
	conn := info.conn
	cconn := conn.cconn

	// Defer captures panics. Restart() and Rollback() panics are returned to the YDB transaction processor as the appropriate constants.
	// Other panic error values are stored in info.err to cross the CGo boundary and are re-paniced later back in Conn.Transaction().
	defer func() {
		recovered := recover()
		info.err = recovered
		if recovered == nil {
			retval = YDB_OK
			return
		}
		err, isYDBError := recovered.(*Error)
		if !isYDBError {
			// non-YottaDB errors cause rollback and also return the error in info.err for re-panic
			retval = YDB_TP_ROLLBACK
			return
		}
		code := err.Code
		if code == ydberr.TPTIMEOUT {
			code = conn.timeoutAction
		}
		if code == YDB_TP_ROLLBACK || code == YDB_TP_RESTART || code == YDB_OK {
			info.err = nil
		}
		// Handle any other error including YDB_TP_RESTART and YDB_TP_ROLLBACK
		retval = C.int(code)
	}()

	saveToken := conn.tptoken.Swap(uint64(tptoken))
	defer conn.tptoken.Store(saveToken)

	if errstr != &cconn.errstr {
		// This should not happen, so there's no way to coverage-test it.
		panic(errorf(ydberr.CallbackWrongGoroutine, "YDBGo design fault: transaction callback from a different connection than the one that initiated the transaction; contact YottaDB support."))
	}
	info.callback()
	return YDB_OK // retval may be changed by deferred func
}
