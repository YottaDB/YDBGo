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
	"reflect"
	"runtime"
	"strconv"
	"sync/atomic"
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

// Fill ydb_buffer_t from a Go []byte slice.
// Returns 1 on success or 0 if the string had to be truncated
int fill_buffer_bytes(ydb_buffer_t *buf, char *data, int len) {
	buf->len_used = len <= buf->len_alloc? len: buf->len_alloc;
	memcpy(buf->buf_addr, data, buf->len_used);
	return len <= buf->len_alloc;
}

// C routine to get address of ydb_lock_st() since CGo doesn't let you take the address of a variadic parameter-list function.
void *getfunc_ydb_lock_st(void) {
        return (void *)&ydb_lock_st;
}
*/
import "C"

// ---- Conn connection object

// Amount to overallocate string spaces for potential future use with larger strings.
// This could be set to C.YDB_MAX_STR to avoid any reallocation, but that would make all new connections large.
// This must be at least large enough to store a number converted to a string (see setAnyValue)
const overalloc = 1024 // Initial size (may be enlarged).

// Conn represents a goroutine-specific 'connection' object for calling the YottaDB API.
// You must use a different connection for each goroutine.
type Conn struct {
	// Conn type wraps C.conn in a Go struct so Go can add methods to it.

	// Pointer to C.conn rather than the item itself so we can malloc it and point to it from C without Go moving it.
	cconn *C.conn
	// tptoken is a place to store tptoken for thread-safe ydb_*_st() function calls
	// It is a pointer and atomic so that Conn.Clone works.
	// Note that on a 64-bit machine using atomic is not supposed to have any overhead.
	tptoken *atomic.Uint64
	// timeoutAction is the action to take on transaction timeout. See [conn.TimeoutAction]()
	timeoutAction int
}

// _newConn is a subset of NewConn without initCheck and value space -- used by signals.init()
func _newConn() *Conn {
	var conn Conn
	conn.cconn = (*C.conn)(calloc(C.sizeof_conn)) // must use our calloc, not malloc: see calloc doc
	conn.tptoken = &atomic.Uint64{}
	conn.tptoken.Store(C.YDB_NOTTP)
	conn.timeoutAction = TransactionTimeout
	// Create space for err
	conn.cconn.errstr.buf_addr = (*C.char)(C.malloc(C.YDB_MAX_ERRORMSG))
	conn.cconn.errstr.len_alloc = C.YDB_MAX_ERRORMSG
	conn.cconn.errstr.len_used = 0

	runtime.AddCleanup(&conn, func(cn *C.conn) {
		C.free(unsafe.Pointer(cn.value.buf_addr))
		C.free(unsafe.Pointer(cn.errstr.buf_addr))
		C.free(unsafe.Pointer(cn.vplist))
		C.free(unsafe.Pointer(cn.paramBlock))
		C.free(unsafe.Pointer(cn))
	}, conn.cconn)
	return &conn
}

// NewConn creates a new database connection.
// Each goroutine must have its own connection.
func NewConn() *Conn {
	initCheck()
	conn := _newConn()
	// Create initial space for value used by various API call/return
	conn.cconn.value.buf_addr = (*C.char)(C.malloc(overalloc))
	conn.cconn.value.len_alloc = C.uint(overalloc)
	conn.cconn.value.len_used = 0
	return conn
}

// prepAPI initializes anything necessary before C API calls.
// This sets the error_output string to the empty string in case the API call fails -- at least then we don't get an obsolete string reported.
func (conn *Conn) prepAPI() {
	conn.cconn.errstr.len_used = 0 // ensure error string is empty before API call
}

// ensureValueSize reallocates value.buf_addr if necessary to fit a string of size.
func (conn *Conn) ensureValueSize(cap int) {
	if cap > C.YDB_MAX_STR {
		panic(errorf(ydberr.InvalidStringLength, "Invalid string length %d: max %d", cap, C.YDB_MAX_STR))
	}
	value := &conn.cconn.value
	if cap > int(value.len_alloc) {
		cap += overalloc // allocate some extra for potential future use
		addr := (*C.char)(C.realloc(unsafe.Pointer(value.buf_addr), C.size_t(cap)))
		if addr == nil {
			panic(errorf(ydberr.OutOfMemory, "out of memory when allocating %d bytes for string data transfer to YottaDB", cap))
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

// setValueBytes stores val into the ydb_buffer of Conn.cconn.value.
func (conn *Conn) setValueBytes(val []byte) *C.ydb_buffer_t {
	cconn := conn.cconn
	conn.ensureValueSize(len(val))
	C.fill_buffer_bytes(&cconn.value, (*C.char)(unsafe.Pointer(unsafe.SliceData(val))), C.int(len(val)))
	return &cconn.value
}

// anyToString converts a number, []byte slices, or string to a string, like Sprint(val) but faster.
//   - val may be a string, []byte slice, integer type, or float; numeric types are converted to a string using the appropriate strconv function.
func anyToString(val any) string {
	switch n := val.(type) {
	// Go evaluates these cases in order, so put common ones first
	case string:
		return n
	case int:
		return strconv.FormatInt(int64(n), 10)
	case float64:
		return strconv.FormatFloat(n, 'G', -1, 64)
	case int64:
		return strconv.FormatInt(n, 10)
	case int32:
		return strconv.FormatInt(int64(n), 10)
	case uint:
		return strconv.FormatUint(uint64(n), 10)
	case uint32:
		return strconv.FormatUint(uint64(n), 10)
	case uint64:
		return strconv.FormatUint(n, 10)
	case float32:
		return strconv.FormatFloat(float64(n), 'G', -1, 32)
	case []byte:
		return string(n)
	default:
		panic(errorf(ydberr.InvalidValueType, "subscript (%v) must be a string, number, or []byte slice but is %s", val, reflect.TypeOf(val)))
	}
}

// setAnyValue is the same as setValue but accepts any type.
// It is akin to setValue(Sprint(val)) but faster.
//   - val may be a string, []byte slice, integer type, or float; numeric types are converted to a string using the appropriate strconv function.
//
// This function could use [anyToString] but it is faster when it doesn't because it can store []byte arrays directly into YDB buffer without conversion.
func (conn *Conn) setAnyValue(val any) {
	var str string
	switch n := val.(type) {
	// Go evaluates these cases in order, so put common ones first
	case string:
		// ensure enough space is allocated (not needed for number cases
		conn.setValue(n)
		return
	case int:
		str = strconv.FormatInt(int64(n), 10)
	case float64:
		str = strconv.FormatFloat(n, 'G', -1, 64)
	case int64:
		str = strconv.FormatInt(n, 10)
	case int32:
		str = strconv.FormatInt(int64(n), 10)
	case uint:
		str = strconv.FormatUint(uint64(n), 10)
	case uint32:
		str = strconv.FormatUint(uint64(n), 10)
	case uint64:
		str = strconv.FormatUint(n, 10)
	case float32:
		str = strconv.FormatFloat(float64(n), 'G', -1, 32)
	case []byte:
		conn.setValueBytes(n)
		return
	default:
		panic(errorf(ydberr.InvalidValueType, "value (%v) must be a string, number, or []byte slice but is %s", val, reflect.TypeOf(val)))
	}
	// The following is equivalent to setValue() but without the size check which is unnecessary since NewConn allocates at least overalloc size
	C.fill_buffer(&conn.cconn.value, str)
	runtime.KeepAlive(conn) // ensure conn sticks around until we've finished copying data into it's C allocation
}

func (conn *Conn) getValue() string {
	cconn := conn.cconn
	r := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	runtime.KeepAlive(conn) // ensure conn sticks around until we've finished copying data from it's C allocation
	return r
}

// Zwr2Str takes the given ZWRITE-formatted string and converts it to return as a normal ASCII string.
//   - If the input string does not fit within the maximum YottaDB string size, return a *Error with Code=ydberr.InvalidStringLength
//   - If zstr is not in valid zwrite format, return the empty string and a *Error with Code=ydberr.InvalidZwriteFormat.
//   - Otherwise, return the decoded string.
//   - Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
//
// Panics on other errors because they are are all panic-worthy (e.g. invalid variable names).
func (conn *Conn) Zwr2Str(zstr string) (string, error) {
	cconn := conn.cconn
	// Don't rely on setValue (below) to check length because it panics, whereas this function is supposed to return errors
	if len(zstr) > C.YDB_MAX_STR {
		return "", errorf(ydberr.InvalidStringLength, "Invalid string length %d: max %d", len(zstr), C.YDB_MAX_STR)
	}
	cbuf := conn.setValue(zstr)
	conn.prepAPI()
	status := C.ydb_zwr2str_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cbuf, cbuf)
	if status == ydberr.INVSTRLEN {
		// NOTE: this code will never run in the current design because setValue() above always allocates enough space
		// Allocate more space and retry the call
		conn.ensureValueSize(int(cconn.value.len_used))
		conn.prepAPI()
		status = C.ydb_zwr2str_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cbuf, cbuf)
	}
	if status != YDB_OK {
		// This error shouldn't happen so hard to test code coverage
		return "", conn.lastError(status)
	}
	val := conn.getValue()
	if val == "" && zstr != "" {
		s := zstr
		if len(s) > 80 {
			s = s[:80] + "..."
		}
		return "", errorf(ydberr.InvalidZwriteFormat, "string has invalid ZWRITE-format: %s", s)
	}
	return val, nil
}

// Str2Zwr takes the given Go string and converts it to return a ZWRITE-formatted string
//   - If the input string does not fit within the maximum YottaDB string size, return a *Error with Code=ydberr.InvalidStringLength
//   - If the output string does not fit within the maximum YottaDB string size, return a *Error with Code=ydberr.INVSTRLEN
//   - Otherwise, return the ZWRITE-formatted string.
//   - Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
//
// Panics on other errors because they are are all panic-worthy (e.g. invalid variable names).
func (conn *Conn) Str2Zwr(str string) (string, error) {
	cconn := conn.cconn
	// Don't rely on setValue (below) to check length because it panics, whereas this function is supposed to return errors
	if len(str) > C.YDB_MAX_STR {
		return "", errorf(ydberr.InvalidStringLength, "Invalid string length %d: max %d", len(str), C.YDB_MAX_STR)
	}
	cbuf := conn.setValue(str)
	conn.prepAPI()
	status := C.ydb_str2zwr_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cbuf, cbuf)
	if status == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		conn.ensureValueSize(int(cconn.value.len_used))
		cbuf := conn.setValue(str)
		conn.prepAPI()
		status = C.ydb_str2zwr_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cbuf, cbuf)
	}
	if status != YDB_OK {
		return "", conn.lastError(status)
	}
	return conn.getValue(), nil
}

// Check whether an entire string is printable ASCII to avoid unnecessarily calling YDB Str2Zwr().
func printableASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] > '~' {
			return false
		}
	}
	return true
}

// Quote adds quotes around strings but not around numbers (just as YottaDB would display them).
//   - The input value is treated as a string if it cannot be converted, unchanged, to and from float64
//     using [strconv.ParseFloat](value, 64) and [strconv.FormatFloat](number, 'f', -1, 64)
//   - If the string contains unprintable ASCII characters it is converted to YottaDB ZWRITE format using [Conn.Str2Zwr].
//   - This is exported so that the user can validate against the same conversion that is used by YDBGo.
func (conn *Conn) Quote(value string) string {
	num, err := strconv.ParseFloat(value, 64)
	// Treat as number only if it can be converted back to the same number -- in which case M would treat it as a number
	if err == nil && value == strconv.FormatFloat(num, 'f', -1, 64) {
		return value
	}
	if printableASCII(value) {
		return "\"" + value + "\""
	} else {
		zwr, err := conn.Str2Zwr(value)
		if err != nil {
			panic(err)
		}
		return zwr
	}
}

// KillLocalsExcept kills all M 'locals' except for the ones listed by name in exclusions.
//   - To kill a specific variable use [Node.Kill]()
func (conn *Conn) KillLocalsExcept(exclusions ...string) {
	var status C.int
	cconn := conn.cconn

	names := stringArrayToAnyArray(exclusions)
	if len(names) == 0 {
		conn.prepAPI()
		status = C.ydb_delete_excl_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, 0, nil)
	} else {
		// use a Node type just as a handy way to store exclusions strings as a ydb_buffer_t array
		namelist := conn._Node(names[0], names[1:])
		conn.prepAPI()
		status = C.ydb_delete_excl_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, C.int(len(names)), namelist.cnode.buffers)
		runtime.KeepAlive(namelist) // ensure namelist sticks around until we've finished copying data from it's C allocation
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
//   - Equivalent to the M `LOCK` command. See [Node.Lock]() and [Node.Unlock]() methods for single-lock usage.
//   - A timeout of zero means try only once.
//   - Return true if lock was acquired; otherwise false.
//   - Panics with error TIME2LONG if the timeout exceeds YDB_MAX_TIME_NSEC or on other panic-worthy errors (e.g. invalid variable names).
func (conn *Conn) Lock(timeout time.Duration, nodes ...*Node) bool {
	timeoutNsec := C.ulonglong(timeout.Nanoseconds())
	cconn := conn.cconn

	// Add each parameter to the vararg list
	conn.vpStart() // restart parameter list
	conn.vpAddParam64(conn.tptoken.Load())
	conn.vpAddParam(uintptr(unsafe.Pointer(&cconn.errstr)))
	conn.vpAddParam64(uint64(timeoutNsec))
	conn.vpAddParam(uintptr(len(nodes)))
	for _, node := range nodes {
		cnode := node.cnode
		conn.vpAddParam(uintptr(unsafe.Pointer(cnode.buffers)))
		conn.vpAddParam(uintptr(cnode.len - 1))
		conn.vpAddParam(uintptr(unsafe.Pointer(bufferIndex(cnode.buffers, 1))))
	}

	// vplist now contains the parameter list we want to send to ydb_lock_st(). But CGo doesn't permit us
	// to call or even create a function pointer to ydb_lock_st(). So get it with getfunc_ydb_lock_st().
	status := conn.vpCall(C.getfunc_ydb_lock_st()) // call ydb_lock_st()
	runtime.KeepAlive(nodes)                       // ensure nodes sticks around until we've finished copying data from their C allocations
	if status != YDB_OK && status != C.YDB_LOCK_TIMEOUT {
		panic(conn.lastError(status))
	}
	return status == YDB_OK
}
