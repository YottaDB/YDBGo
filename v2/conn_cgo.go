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
func tpCallbackWrapper(tptoken C.uint64_t, errstr *C.ydb_buffer_t, handle unsafe.Pointer) C.int {
	h := *(*cgo.Handle)(handle)
	info := h.Value().(tpInfo)
	cconn := info.conn.cconn
	saveToken := cconn.tptoken
	cconn.tptoken = tptoken
	if errstr != &cconn.errstr {
		panic(newError(ydberr.CallbackWrongGoroutine, "YDBGo design fault: callback invoked from a different groutine than the one used by the connection"))
	}
	retval := info.callback()
	cconn.tptoken = saveToken
	h.Delete()
	return C.int(retval)
}
