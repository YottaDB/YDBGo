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
	"errors"
	"fmt"
	"runtime"
	"runtime/cgo"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

/* #include "libyottadb.h"
#include "yottadb.h"

// Work around Go compiler issue to be fixed in Go 1.25: https://go-review.googlesource.com/c/go/+/642235
extern size_t _GoStringLen(_GoString_ s);
extern const char *_GoStringPtr(_GoString_ s);

// Fill ydb_buffer_t with a Go string.
// Returns 1 on success or 0 if the string had to be truncated
int fill_buffer(ydb_buffer_t *buf, _GoString_ val) {
	unsigned int len = _GoStringLen(val);
	buf->len_used = len <= buf->len_alloc? len: buf->len_alloc;
	memcpy(buf->buf_addr, _GoStringPtr(val), buf->len_used);
	return len <= buf->len_alloc;
}

// C routine to get address of ydb_lock_st() since CGo doesn't let you take the address of a variadic parameter-list function.
void *getfunc_ydb_lock_st(void) {
        return (void *)&ydb_lock_st;
}

extern int tpCallbackWrapper(uint64_t tptoken, ydb_buffer_t *errstr, void *callback);
int tp_callback_wrapper(uint64_t tptoken, ydb_buffer_t *errstr, void *callback) {
  return tpCallbackWrapper(tptoken, errstr, callback);
}
*/
import "C"

// ---- Conn connection object

// Amount to overallocate string spaces for potential future use with larger strings.
// This could be set to C.YDB_MAX_STR to avoid any reallocation, but that would make all new connections large.
const overalloc = 1024

// Conn represents a goroutine-specific 'connection' object for calling the YottaDB API.
// You must use a different connection for each goroutine.
//
// Conn type wraps C.conn in a Go struct so Go can add methods to it.
type Conn struct {
	// Pointer to C.conn rather than the item itself so we can malloc it and point to it from C without Go moving it.
	cconn *C.conn
}

// NewConn creates a new database connection.
// Each goroutine must have its own connection.
func NewConn() *Conn {
	initCheck()

	var conn Conn
	conn.cconn = (*C.conn)(calloc(C.sizeof_conn)) // must use our calloc, not malloc: see calloc doc
	conn.cconn.tptoken = C.YDB_NOTTP
	// Create space for err
	conn.cconn.errstr.buf_addr = (*C.char)(C.malloc(C.YDB_MAX_ERRORMSG))
	conn.cconn.errstr.len_alloc = C.YDB_MAX_ERRORMSG
	conn.cconn.errstr.len_used = 0
	// Create initial space for value used by various API call/return
	conn.cconn.value.buf_addr = (*C.char)(C.malloc(overalloc))
	conn.cconn.value.len_alloc = C.uint(overalloc)
	conn.cconn.value.len_used = 0

	runtime.AddCleanup(&conn, func(cn *C.conn) {
		C.free(unsafe.Pointer(cn.value.buf_addr))
		C.free(unsafe.Pointer(cn.errstr.buf_addr))
		C.free(unsafe.Pointer(cn.vplist))
		C.free(unsafe.Pointer(cn.paramBlock))
		C.free(unsafe.Pointer(cn))
	}, conn.cconn)
	return &conn
}

// prepAPI initializes anything necessary before C API calls.
// This sets the error_output string to the empty string in case the API call fails -- at least then we don't get an obsolete string reported.
func (conn *Conn) prepAPI() {
	conn.cconn.errstr.len_used = 0 // ensure error string is empty before API call
}

// ensureValueSize reallocates value.buf_addr if necessary to fit a string of size.
func (conn *Conn) ensureValueSize(cap int) {
	if cap > C.YDB_MAX_STR {
		panic(newYDBError(ydberr.InvalidStringLength, fmt.Sprintf("Invalid string length %d: max %d", cap, C.YDB_MAX_STR)))
	}
	value := &conn.cconn.value
	if cap > int(value.len_alloc) {
		cap += overalloc // allocate some extra for potential future use
		addr := (*C.char)(C.realloc(unsafe.Pointer(value.buf_addr), C.size_t(cap)))
		if addr == nil {
			panic(newYDBError(ydberr.OutOfMemory, fmt.Sprintf("out of memory when allocating %d bytes for string data transfer to YottaDB", cap)))
		}
		value.buf_addr = addr
		value.len_alloc = C.uint(cap)
	}
}

// setValue stores val into the ydb_buffer of Conn.cconn.value.
func (conn *Conn) setValue(val string) *C.ydb_buffer_t {
	cconn := conn.cconn
	conn.ensureValueSize(len(val))
	C.fill_buffer(&cconn.value, val)
	return &cconn.value
}

func (conn *Conn) getValue() string {
	cconn := conn.cconn
	return C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
}

// getErrorString returns a copy of conn.cconn.errstr as a Go string.
func (conn *Conn) getErrorString() string {
	// len_used should never be greater than len_alloc since all errors should fit into errstr, but just in case, take the min
	errstr := conn.cconn.errstr
	return C.GoStringN(errstr.buf_addr, C.int(min(errstr.len_used, errstr.len_alloc)))
}

// lastError returns, given error code, the ydb error message stored by the previous YottaDB call as an error type or nil if there was no error.
func (conn *Conn) lastError(code C.int) error {
	if code == C.YDB_OK {
		return nil
	}
	// Take a copy of errstr as a Go String
	// (len_used should never be greater than len_alloc since all errors should fit into errstr, but just in case, take the min)
	msg := conn.getErrorString()
	if msg == "" { // See if msg is still empty (we set it to empty before calling the API in conn.prepAPI()
		// This code gets run, for example, if ydb_exit() is called before a YDB function is invoked.
		return newYDBError(int(code), conn.recoverMessage(code))
	}

	pattern := ",(SimpleThreadAPI),"
	index := strings.Index(msg, pattern)
	if index == -1 {
		// If msg is improperly formatted, return it verbatim as an error with code YDBMessageInvalid (this should never happen with messages from YottaDB)
		return newYDBError(ydberr.YDBMessageInvalid, fmt.Sprintf("could not parse YottaDB error message: %s", msg))
	}
	text := msg[index+len(pattern):]
	return newYDBError(int(code), text)
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
		panic(newYDBError(ydberr.YDBMessageInvalid, fmt.Sprintf("could not parse YottaDB error message: %s", msg)))
	}
	code, err := strconv.ParseInt(msg[:index], 10, 64)
	if err != nil {
		// If msg is improperly formatted, panic it verbatim with an YDBMessageInvalid code (this should never happen with messages from YottaDB)
		panic(newYDBError(ydberr.YDBMessageInvalid, fmt.Sprintf("%s: %s", err, msg), err))
	}
	return C.int(-code)
}

// recoverMessage tries to get the error message (albeit without argument substitution) from the supplied status code in cases where the message has been lost.
func (conn *Conn) recoverMessage(status C.int) string {
	cconn := conn.cconn
	// Check for a couple of special cases first.
	switch status {
	case ydberr.THREADEDAPINOTALLOWED:
		// This error will prevent ydb_message_t() from working below, so instead return a hard-coded error message.
		return "%YDB-E-THREADEDAPINOTALLOWED, Process cannot switch to using threaded Simple API while already using Simple API"
	case ydberr.CALLINAFTERXIT:
		// The engine is shut down so calling ydb_message_t will fail if we attempt it so just hard-code this error return value.
		return "%YDB-E-CALLINAFTERXIT, After a ydb_exit(), a process cannot create a valid YottaDB context"
	}
	rc := C.ydb_message_t(cconn.tptoken, nil, -status, &cconn.errstr)
	if rc != YDB_OK {
		err := conn.lastError(rc)
		panic(newYDBError(ydberr.YDBMessageRecoveryFailure, fmt.Sprintf("error %d when trying to recover error message for error %d using ydb_message_t(): %s", int(rc), int(-status), err), err))
	}
	return conn.getErrorString()
}

// Zwr2Str takes the given ZWRITE-formatted string and converts it to return as a normal ASCII string.
//   - If zstr is not in valid zwrite format, return the empty string. Otherwise, return the unencoded string.
//   - Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
func (conn *Conn) Zwr2Str(zstr string) (string, error) {
	cconn := conn.cconn
	cbuf := conn.setValue(zstr)
	conn.prepAPI()
	status := C.ydb_zwr2str_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	if status == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		conn.ensureValueSize(int(cconn.value.len_used))
		conn.prepAPI()
		status = C.ydb_zwr2str_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	}
	if status != YDB_OK {
		return "", conn.lastError(status)
	}
	val := conn.getValue()
	if val == "" && zstr != "" {
		return "", errors.New("string has invalid ZWRITE-format")
	}
	return val, nil
}

// Str2Zwr takes the given Go string and converts it to return a ZWRITE-formatted string
//   - If the returned zwrite-formatted string does not fit within the maximum YottaDB string size, return a *Error with code=ydberr.INVSTRLEN.
//     Otherwise, return the ZWRITE-formatted string.
//   - Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
func (conn *Conn) Str2Zwr(str string) (string, error) {
	cconn := conn.cconn
	cbuf := conn.setValue(str)
	conn.prepAPI()
	status := C.ydb_str2zwr_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	if status == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		conn.ensureValueSize(int(cconn.value.len_used))
		cbuf := conn.setValue(str)
		conn.prepAPI()
		status = C.ydb_str2zwr_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	}
	if status != YDB_OK {
		return "", conn.lastError(status)
	}
	return conn.getValue(), nil
}

// KillLocalsExcept kills all M 'locals' except for the ones listed by name in exclusions.
//   - To kill a specific variable use [Node.Kill]()
func (conn *Conn) KillLocalsExcept(exclusions ...string) {
	var status C.int
	cconn := conn.cconn
	if len(exclusions) == 0 {
		conn.prepAPI()
		status = C.ydb_delete_excl_st(cconn.tptoken, &cconn.errstr, 0, nil)
	} else {
		// use a Node type just as a handy way to store exclusions strings as a ydb_buffer_t array
		namelist := conn.Node(exclusions[0], exclusions[1:]...)
		conn.prepAPI()
		status = C.ydb_delete_excl_st(cconn.tptoken, &cconn.errstr, C.int(len(exclusions)), &namelist.cnode.buffers)
	}
	if status != YDB_OK {
		panic(conn.lastError(status))
	}
}

// KillAllLocals kills all M 'locals'.
// It is for clearer source code as it simply calls KillLocalsExcept() without listing any exceptions.
//   - To kill a specific variable use [Node.Kill]()
func (conn *Conn) KillAllLocals() {
	conn.KillLocalsExcept()
}

// Lock releases all existing locks and attempt to acquire locks matching all supplied nodes, waiting up to timeout for availability.
//   - Equivalent to the M `LOCK` command. See [Node.Grab]() and [Node.Release]() methods for single-lock usage.
//   - A timeout of zero means try only once.
//   - Return true if lock was acquired; otherwise false.
//   - Panics with error TIME2LONG if the timeout exceeds YDB_MAX_TIME_NSEC or on other panic-worthy errors (e.g. invalid variable names).
func (conn *Conn) Lock(timeout time.Duration, nodes ...*Node) bool {
	timeoutNsec := C.ulonglong(timeout.Nanoseconds())
	cconn := conn.cconn

	// Add each parameter to the vararg list
	conn.vpStart() // restart parameter list
	conn.vpAddParam64(uint64(cconn.tptoken))
	conn.vpAddParam(uintptr(unsafe.Pointer(&cconn.errstr)))
	conn.vpAddParam64(uint64(timeoutNsec))
	conn.vpAddParam(uintptr(len(nodes)))
	for _, node := range nodes {
		cnode := node.cnode
		conn.vpAddParam(uintptr(unsafe.Pointer(&cnode.buffers)))
		conn.vpAddParam(uintptr(cnode.len - 1))
		conn.vpAddParam(uintptr(unsafe.Pointer(bufferIndex(&cnode.buffers, 1))))
	}

	// vplist now contains the parameter list we want to send to ydb_lock_st(). But CGo doesn't permit us
	// to call or even create a function pointer to ydb_lock_st(). So get it with getfunc_ydb_lock_st().
	status := conn.vpCall(C.getfunc_ydb_lock_st()) // call ydb_lock_st()
	if status != YDB_OK && status != C.YDB_LOCK_TIMEOUT {
		panic(conn.lastError(status))
	}
	return status == YDB_OK
}

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
//   - Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
//
// The callback function should:
//   - Implement the required database logic taking into account key considerations for [Transaction Processing] code.
//   - If there are database collisions, `callback` will be called repeatedly, rolling back the database before each call. On the
//     fourth try, YottaDB will resort to calling it with other processes locked out to ensure its success.
//   - Return YDB_OK on success
//   - Return YDB_TP_RESTART to force a rollback plus restart
//   - Return YDB_TP_ROLLBACK to roll back the database and return false to the caller of Transaction().
//
// Transaction nesting level may be determined within the callback function by reading the special variable [$tlevel], and the number of restart
// repetitions by [$trestart]. These things are documented in more detail in [Transaction Processing].
//
// [Transaction Processing]: https://docs.yottadb.com/ProgrammersGuide/langfeat.html#transaction-processing
// [$trestart]: https://docs.yottadb.com/ProgrammersGuide/isv.html#trestart
// [$tlevel]: https://docs.yottadb.com/ProgrammersGuide/isv.html#tlevel
func (conn *Conn) Transaction(transID string, localsToRestore []string, callback func() int) bool {
	cconn := conn.cconn
	info := tpInfo{conn, callback}
	handle := C.uintptr_t(cgo.NewHandle(info))
	var status C.int
	if len(localsToRestore) == 0 {
		conn.prepAPI()
		status = C.ydb_tp_st(cconn.tptoken, &cconn.errstr, C.ydb_tpfnptr_t(C.tp_callback_wrapper), unsafe.Pointer(&handle),
			(*C.char)(unsafe.Pointer(unsafe.StringData(transID))), 0, nil)
	} else {
		// use a Node type just as a handy way to store exclusions strings as a ydb_buffer_t array
		namelist := conn.Node(localsToRestore[0], localsToRestore[1:]...)
		conn.prepAPI()
		status = C.ydb_tp_st(cconn.tptoken, &cconn.errstr, C.ydb_tpfnptr_t(C.tp_callback_wrapper), unsafe.Pointer(&handle),
			(*C.char)(unsafe.Pointer(unsafe.StringData(transID))), C.int(len(localsToRestore)), &namelist.cnode.buffers)
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
//   - Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
//
// [Transaction Processing]: https://docs.yottadb.com/ProgrammersGuide/langfeat.html#transaction-processing
func (conn *Conn) TransactionFast(localsToRestore []string, callback func() int) bool {
	return conn.Transaction("BATCH", localsToRestore, callback)
}
