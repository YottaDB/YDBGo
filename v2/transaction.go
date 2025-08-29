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

// Define Node type for access YottaDB database

package yottadb

import (
	"runtime"
	"runtime/cgo"
	"unsafe"
)

/* #include "libyottadb.h"
extern int tpCallbackWrapper(uint64_t tptoken, ydb_buffer_t *errstr, void *callback);
int tp_callback_wrapper(uint64_t tptoken, ydb_buffer_t *errstr, void *callback) {
  return tpCallbackWrapper(tptoken, errstr, callback);
}
*/
import "C"

// tpInfo struct stores callback function and connection used by Transaction() to run transaction logic.
type tpInfo struct {
	conn     *Conn
	callback func() int
}

// Transaction processes database logic inside a database transaction.
//   - `callback` must be a function that implements the required database logic.
//   - `transId` has its first 8 bytes recorded in the commit record of journal files for database regions participating in the transaction.
//     Note that a transId of case-insensitive "BATCH" or "BA" are special: see [Conn.TransactionFast]()
//   - `localsToRestore` are names of local M variables to be restored to their original values when a transaction is restarted.
//     If localsToRestore[0] equals "*" then all local M database variables are restored on restart. Note that since Go has its own local
//     variables it is unlikely that you will need this feature in Go.
//   - Returns true to indicate that the transaction logic was successful and has been committed to the database, or false if a rollback was necessary.
//   - Panics on errors because they are are all panic-worthy (e.g. invalid variable names). See [yottadb.Error] for rationale.
//
// The callback function should:
//   - Implement the required database logic taking into account key considerations for [Transaction Processing] code.
//   - If there are database collisions, `callback` will be called repeatedly, rolling back the database before each call. On the
//     fourth try, YottaDB will resort to calling it with other processes locked out to ensure its success.
//   - Call [Conn.Restart] if it needs to rollback and immediately restart the transaction function
//   - Call [Conn.Rollback] if it needs to rollback and immediately exit the transaction function
//
// Transaction nesting level may be determined within the callback function by reading the special variable [$tlevel], and the number of restart
// repetitions by [$trestart]. These things are documented in more detail in [Transaction Processing].
//
// [Transaction Processing]: https://docs.yottadb.com/ProgrammersGuide/langfeat.html#transaction-processing
// [$trestart]: https://docs.yottadb.com/ProgrammersGuide/isv.html#trestart
// [$tlevel]: https://docs.yottadb.com/ProgrammersGuide/isv.html#tlevel
func (conn *Conn) Transaction(transID string, localsToRestore []string, callback func()) bool {
	recoveredCallback := func() (retval int) {
		// defer a function that recovers from RESTART and ROLLBACK panics and returns them to YDB transaction processor instead
		defer func() {
			if err := recover(); err != nil {
				err, ok := err.(*Error)
				if !ok {
					panic(err)
				}
				code := err.Code
				if code == YDB_TP_RESTART || code == YDB_TP_ROLLBACK {
					retval = code
					return
				}
				panic(err)
			}
		}()
		callback()
		return YDB_OK // no rollback or restart
	}

	cconn := conn.cconn
	info := tpInfo{conn, recoveredCallback}
	handle := cgo.NewHandle(info)
	defer handle.Delete()

	names := stringArrayToAnyArray(localsToRestore)
	var status C.int
	if len(names) == 0 {
		conn.prepAPI()
		status = C.ydb_tp_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, C.ydb_tpfnptr_t(C.tp_callback_wrapper), unsafe.Pointer(&handle),
			(*C.char)(unsafe.Pointer(unsafe.StringData(transID))), 0, nil)
	} else {
		// use a Node type just as a handy way to vars to restore as a ydb_buffer_t array
		namelist := conn._Node(names[0], names[1:])
		conn.prepAPI()
		status = C.ydb_tp_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, C.ydb_tpfnptr_t(C.tp_callback_wrapper), unsafe.Pointer(&handle),
			(*C.char)(unsafe.Pointer(unsafe.StringData(transID))), C.int(len(names)), namelist.cnode.buffers)
		runtime.KeepAlive(namelist) // ensure namelist sticks around until we've finished copying data from it's C allocation
	}
	if status == YDB_TP_ROLLBACK {
		return false
	}
	if status != YDB_OK {
		panic(conn.lastError(status))
	}
	return true
}

// TransactionFast is a faster version of Transaction that does not ensure durability,
// for applications that do not require durability or have alternate durability mechanisms (such as checkpoints).
// It is implemented by setting the transID to the special name "BATCH" as discussed in [Transaction Processing].
//   - Panics on errors because they are are all panic-worthy (e.g. invalid variable names). See [yottadb.Error] for rationale.
//
// [Transaction Processing]: https://docs.yottadb.com/ProgrammersGuide/langfeat.html#transaction-processing
func (conn *Conn) TransactionFast(localsToRestore []string, callback func()) bool {
	return conn.Transaction("BATCH", localsToRestore, callback)
}

// Rollback and exit a transaction immediately.
func (conn *Conn) Rollback() {
	// This panic is caught by [Conn.Transaction] to make it do a rollback and exit
	panic(newError(YDB_TP_ROLLBACK, ""))
}

// Restart a transaction immediately (after first rolling back).
func (conn *Conn) Restart() {
	// This panic is caught by [Conn.Transaction] to make it do a restart
	panic(newError(YDB_TP_RESTART, ""))
}

// TransactionToken sets the transaction-level token being using by the given connection conn.
// This is for use only in the unusual situation of mixing YDBGo v1 and v2 code and you have a v2 transaction
// that needs to call a v1 function (which must therefore be passed the v2 Conn's tptoken).
// It would be tidier, however, to avoid mixing versions within a transaction, therefore this function is deprecated
// from its inception and will be removed in a future version once there has been plenty of time to migrate all code to v2.
// See [Conn.TransactionTokenSet]
func (conn *Conn) TransactionToken() (tptoken uint64) {
	return conn.tptoken.Load()
}

// TransactionTokenSet sets the transaction-level token being using by the given connection conn.
// This is for use only in the unusual situation of mixing YDBGo v1 and v2 code and you have a v1 transaction
// that needs to call a v2 function (which must therefore be run on a Conn with the v1 tptoken).
// It would be tidier, however, to avoid mixing versions within a transaction, therefore this function is deprecated
// from its inception and will be removed in a future version once there has been plenty of time to migrate all code to v2.
// See [Conn.TransactionToken]
func (conn *Conn) TransactionTokenSet(tptoken uint64) {
	conn.tptoken.Store(tptoken)
}

// Clone returns a new connection that operates with the same transaction-level token as the original connection conn.
// This may be used if you absolutely must have activity within one transaction spread across multiple goroutines, in which case
// each new goroutine will need a new connection that has the same transaction token as the original connection.
// However, be aware that spreading transaction activity across multiple goroutines is not a recommended pattern.
// Before doing so the programmer should first read and understand [Threads and Transaction Processing].
//
// [Threads and Transaction Processing]: https://docs.yottadb.com/MultiLangProgGuide/programmingnotes.html#threads-and-transaction-processing
func (conn *Conn) CloneConn() *Conn {
	new := NewConn()
	new.tptoken = conn.tptoken // point to the original conn's tptoken
	return new
}
