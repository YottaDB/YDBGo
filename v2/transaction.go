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

// Allow Go to perform YottaDB transactions

package yottadb

import (
	"fmt"
	"runtime"
	"runtime/cgo"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
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
	callback func()
	err      any // variable to store transaction panic errors during transition back through the CGo boundary
}

// Transaction processes database logic inside a database transaction.
//   - `callback` must be a function that implements the required database logic.
//   - `transId` has its first 8 bytes recorded in the commit record of journal files for database regions participating in the transaction.
//     Note that a transId of case-insensitive "BATCH" or "BA" are special: see [Conn.TransactionFast]()
//   - `localsToRestore` are names of local M variables to be restored to their original values when a transaction is restarted.
//     If localsToRestore[0] equals "*" then all local M locals are restored on restart. Note that since Go has its own local
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
//   - Finish quickly because database activity in other goroutines will be blocked until it is complete.
//   - Not create goroutines within the transaction unless absolutely necessary, in which case see [Conn.CloneConn].
//
// Transaction nesting level may be determined within the callback function by reading the special variable [$tlevel], and the number of restart
// repetitions by [$trestart]. These things are documented in more detail in [Transaction Processing].
//
// [Transaction Processing]: https://docs.yottadb.com/ProgrammersGuide/langfeat.html#transaction-processing
// [$trestart]: https://docs.yottadb.com/ProgrammersGuide/isv.html#trestart
// [$tlevel]: https://docs.yottadb.com/ProgrammersGuide/isv.html#tlevel
func (conn *Conn) Transaction(transID string, localsToRestore []string, callback func()) bool {
	cconn := conn.cconn
	info := tpInfo{conn, callback, nil}
	handle := cgo.NewHandle(&info)
	defer handle.Delete()

	names := stringArrayToAnyArray(localsToRestore)
	var status C.int
	transID += "\x00" // NUL-terminate transID because it's required by ydb_tp_st()
	if len(names) == 0 {
		conn.prepAPI()
		status = C.ydb_tp_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, C.ydb_tpfnptr_t(C.tp_callback_wrapper), unsafe.Pointer(&handle),
			(*C.char)(unsafe.Pointer(unsafe.StringData(transID))), 0, nil)
	} else {
		// use a Node type just as a ydb_buffer_t array (i.e. not pointing to a node) as a handy way to list varnames to restore
		namelist := conn._Node(names[0], names[1:])
		conn.prepAPI()
		status = C.ydb_tp_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, C.ydb_tpfnptr_t(C.tp_callback_wrapper), unsafe.Pointer(&handle),
			(*C.char)(unsafe.Pointer(unsafe.StringData(transID))), C.int(len(names)), namelist.cnode.buffers)
		runtime.KeepAlive(namelist) // ensure namelist sticks around until we've finished copying data from it's C allocation
	}
	runtime.KeepAlive(transID) // ensure batch id doesn't disappear until transaction call returns and has finished using it
	// Propagate any panics that occurred during the transaction function and
	// sent to me by tpCallbackWrapper in info.err to avoid panics crossing the CGo boundary.
	err := info.err
	if err != nil {
		// Re-panic the err, including any traceback (if it's a YDB error; we don't know how to get stacktraces from other Go errors)
		yerr, ok := err.(*Error)
		if ok {
			err = newError(yerr.Code, fmt.Sprintf("%s\n\n%s\nWas re-paniced as: %s", err, yerr.stack, err), yerr.chain...)
		}
		panic(err)
	}
	if status == YDB_TP_ROLLBACK {
		return false
	}
	if status != YDB_OK {
		// This line is not tested in coverage tests as I do not know how to make ydb_tp_st return an error that is not already
		// handled or already returned by a ydb function inside the transaction and handled there.
		// Nevertheless, this will handle it if it occurs.
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

// Constants used for TimeoutAction.
// These just happen to all map to YDB error codes that do something but the new names have a more consistent naming scheme
// and are more Goish.
const (
	TransactionCommit   = YDB_OK
	TransactionRollback = YDB_TP_ROLLBACK
	TransactionTimeout  = ydberr.TPTIMEOUT
)

// TimeoutAction sets the action that is performed when [conn.Transaction] times out.
// The following actions are valid and performed the named function:
//   - TransactionTimeout (default): rollback and panic with Error.Code = ydberr.TPTIMEOUT
//   - TransactionRollback: rollback the transaction and return false from the transaction function
//   - TransactionCommit: commit database activity that was done before the timeout
//
// Notes:
//   - Timeout only occurs if YottaDB special variable $ZMAXTPTIME is set.
//   - Although TimeoutAction is specific to the specified conn, $ZMAXTPTIME is global.
//   - If TimeoutAction has not been called on this Conn, the default action is TransactionTimeout.
func (conn *Conn) TimeoutAction(action int) {
	if action != TransactionTimeout && action != TransactionRollback && action != TransactionCommit {
		panic(errorf(ydberr.InvalidTimeoutAction, "invalid action constant %d passed to TimeoutAction()", action))
	}
	conn.timeoutAction = action
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

// Clone returns a new connection that initially begins with the same transaction token as the original connection conn.
// This may be used if you absolutely must have activity within one transaction spread across multiple goroutines, in which case
// each new goroutine will need a new connection that has the same transaction token as the original connection.
// However, be aware that spreading transaction activity across multiple goroutines is not a recommended pattern.
// Before doing so the programmer should first read and understand [Threads and Transaction Processing].
//
// [Threads and Transaction Processing]: https://docs.yottadb.com/MultiLangProgGuide/programmingnotes.html#threads-and-transaction-processing
func (conn *Conn) CloneConn() *Conn {
	new := NewConn()
	new.tptoken.Store(conn.tptoken.Load()) // initially inherit the original conn's tptoken
	new.timeoutAction = TransactionTimeout
	return new
}
