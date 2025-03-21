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
	"bytes"
	"runtime"
	"strings"
	"unsafe"
)

/* #include "libyottadb.h"
// Create a thread-specific 'connection' object for calling the YottaDB API.
typedef struct conn {
	uint64_t tptoken;	// place to store tptoken for thread-safe ydb_*_st() function calls
	ydb_buffer_t errstr;	// space for YottaDB to return an error string
	ydb_buffer_t value;	// temporary space to store in or out value for get/set
} conn;

// Create a representation of a database node, including a cache of its subscript strings for fast calls to the YottaDB API.
typedef struct node {
	conn *conn;
	int len;		// number of buffers[] allocated to store subscripts/strings
	int datasize;		// length of string `data` field (all strings and subscripts concatenated)
	int mutable;		// whether the node is mutable (these are only emitted by node iterators)
	ydb_buffer_t buffers[1];	// first of an array of buffers (typically varname)
	ydb_buffer_t buffersn[];	// rest of array
	// char *data;		// stored after `buffers` (however large they are), which point into this data
} node;

// Fill ydb_buffer_t with a Go string.
// Returns 1 on success or 0 if the string had to be truncated
int fill_buffer(ydb_buffer_t *buf, _GoString_ val) {
	unsigned int len = _GoStringLen(val);
	buf->len_used = len <= buf->len_alloc? len: buf->len_alloc;
	memcpy(buf->buf_addr, _GoStringPtr(val), buf->len_used);
	return len <= buf->len_alloc;
}
*/
import "C"

// ---- Conn connection object

// Conn creates a thread-specific 'connection' object for calling the YottaDB API.
// You must use a different connection for each thread.
// Wrap C.conn in a Go struct so we can add methods to it.
type Conn struct {
	// Pointer to C.conn rather than the item itself so we can malloc it and point to it from C without Go moving it.
	cconn *C.conn
}

// NewConn creates a new connection for the current thread.
func NewConn() *Conn {
	initCheck()

	// TODO: This is set to YDB_MAX_STR (1MB) for the initial version only. Later we can reduce its initial value and create logic to reallocate it when necessary,
	//       e.g. in n.Set()
	const initialSpace = C.YDB_MAX_STR
	var conn Conn
	// This initial call must be to calloc() to get initialized (cleared) storage. We cannot allocate it and then
	// do another call to initialize it as that means uninitialized memory is traversing the cgo boundary which
	// is what triggers the cgo bug mentioned in the cgo docs (https://golang.org/cmd/cgo/#hdr-Passing_pointers).
	// TODO: but if we retain calloc, we need to check for memory error because Go doesn't create a wrapper for C.calloc like it does for C.malloc (cf. https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers:~:text=C.malloc%20cannot%20fail)
	// Alternatively, we could call malloc and then memset to clear just the ydb_buffer_t parts, but test which is faster.
	conn.cconn = (*C.conn)(C.calloc(1, C.sizeof_conn))
	conn.cconn.tptoken = C.YDB_NOTTP
	// Create space for err
	conn.cconn.errstr.buf_addr = (*C.char)(C.malloc(C.YDB_MAX_ERRORMSG))
	conn.cconn.errstr.len_alloc = C.YDB_MAX_ERRORMSG
	conn.cconn.errstr.len_used = 0
	// Create initial space for value used by various API call/return
	conn.cconn.value.buf_addr = (*C.char)(C.malloc(initialSpace))
	conn.cconn.value.len_alloc = C.uint(initialSpace)
	conn.cconn.value.len_used = 0

	runtime.AddCleanup(&conn, func(cn *C.conn) {
		C.free(unsafe.Pointer(cn.value.buf_addr))
		C.free(unsafe.Pointer(cn.errstr.buf_addr))
		C.free(unsafe.Pointer(cn))
	}, conn.cconn)
	return &conn
}

func (conn *Conn) setValue(val string) *C.ydb_buffer_t {
	cconn := conn.cconn
	if C.fill_buffer(&cconn.value, val) == 0 {
		// TODO: fix the following to realloc
		panic("YDB: have not yet implemented reallocating cconn.value to fit a large returned string")
	}
	return &cconn.value
}

func (conn *Conn) getValue() string {
	cconn := conn.cconn
	return C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
}

// GetError returns, given error code, the ydb error message stored by the previous YottaDB call as an error type or nil if there was no error.
func (conn *Conn) GetError(code C.int) error {
	if code == C.YDB_OK {
		return nil
	}
	// Take a copy of errstr as a Go String
	// (len_used should never be greater than len_alloc since all errors should fit into errstr, but just in case, take the min)
	cconn := conn.cconn
	msg := C.GoStringN(cconn.errstr.buf_addr, C.int(min(cconn.errstr.len_used, cconn.errstr.len_alloc)))
	if msg == "" { // this should never happen
		msg = "(unknown error)"
	}
	return NewError(int(code), msg)
}

// Zwr2Str takes the given ZWRITE-formatted string and converts it to return as a normal ASCII string.
// If zstr is not in valid zwrite format, return the empty string. Otherwise, return the unencoded string.
// Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
func (conn *Conn) Zwr2Str(zstr string) (string, error) {
	cconn := conn.cconn
	cbuf := conn.setValue(zstr)
	ret := C.ydb_zwr2str_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	if ret == C.YDB_ERR_INVSTRLEN {
		// TODO: fix the following to realloc
		panic("YDB: have not yet implemented reallocating cconn.value to fit a large returned string")
	}
	if ret != YDB_OK {
		return "", conn.GetError(ret)
	}
	return conn.getValue(), nil
}

// Str2Zwr takes the given Go string and converts it to return a ZWRITE-formatted string
// If the returned zwrite-formatted string does not fit within the maximum YottaDB string size, return error code YDB_ERR_INVSTRLEN.
// Otherwise, return the ZWRITE-formatted string.
// Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
func (conn *Conn) Str2Zwr(str string) (string, error) {
	cconn := conn.cconn
	cbuf := conn.setValue(str)
	ret := C.ydb_str2zwr_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	if ret == C.YDB_ERR_INVSTRLEN {
		// TODO: fix the following to realloc
		panic("YDB: have not yet implemented reallocating cconn.value to fit a large returned string")
	}
	if ret != YDB_OK {
		return "", conn.GetError(ret)
	}
	return conn.getValue(), nil
}

// ---- Node object

// Node is an object containing strings that represents a YottaDB node, supporting fast calls to the YottaDB C API.
// Stores all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
// structs that point to each successive string, to provide fast access to YottaDB API functions.
// Thread Safety: Regular Nodes are immutable, so are thread-safe (one thread cannot change a Node used by
// another thread). There is a mutable version of Node emitted by Node iterators (FOR loops over Node), which
// may not be shared with other threads except by first taking an immutable Node.Copy() of it.
// Wraps C.node in a Go struct so we can add methods to it.
type Node struct {
	// Pointer to C.node rather than the item itself so we can point to it from C without Go moving it.
	n    *C.node
	conn *Conn // Node.conn points to the Go conn; Node.n.conn will point directly to the C.conn
}

// Node creates a `Node` type instance that represents a database node with class methods for fast calls to YottaDB.
// The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
func (conn *Conn) Node(varname string, subscripts ...string) (n *Node) {
	// Concatenate strings the fastest Go way.
	// This involves creating an extra copy of subscripts but is probably faster than one C.memcpy call per subscript
	var joiner bytes.Buffer
	joiner.WriteString(varname)
	for _, s := range subscripts {
		joiner.WriteString(s)
	}

	size := C.sizeof_node + C.sizeof_ydb_buffer_t*len(subscripts) + joiner.Len()
	// This initial call must be to calloc() to get initialized (cleared) storage. We cannot allocate it and then
	// do another call to initialize it as that means uninitialized memory is traversing the cgo boundary which
	// is what triggers the cgo bug mentioned in the cgo docs (https://golang.org/cmd/cgo/#hdr-Passing_pointers).
	// TODO: but if we retain calloc, we need to check for memory error because Go doesn't create a wrapper for C.calloc like it does for C.malloc (cf. https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers:~:text=C.malloc%20cannot%20fail)
	// Alternatively, we could call malloc and then memset to clear just the ydb_buffer_t parts, but test which is faster.
	var goNode Node
	n = &goNode
	n.n = (*C.node)(C.calloc(1, C.size_t(size)))
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(cnode *C.node) {
		C.free(unsafe.Pointer(cnode))
	}, n.n)

	n.conn = conn // point to the Go conn
	cnode := n.n
	cnode.conn = (*C.conn)(unsafe.Pointer(conn.cconn)) // point to the C version of the conn
	cnode.len = C.int(len(subscripts) + 1)
	cnode.mutable = 0 // i.e. false

	dataptr := unsafe.Add(unsafe.Pointer(&cnode.buffers[0]), C.sizeof_ydb_buffer_t*(len(subscripts)+1))
	C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))

	// Now fill in ydb_buffer_t pointers
	s := varname
	buf := (*C.ydb_buffer_t)(unsafe.Pointer(&cnode.buffers[0]))
	buf.buf_addr = (*C.char)(dataptr)
	buf.len_used, buf.len_alloc = C.uint(len(s)), C.uint(len(s))
	dataptr = unsafe.Add(dataptr, len(s))
	for i, s := range subscripts {
		buf := (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&cnode.buffers[0]), C.sizeof_ydb_buffer_t*(i+1)))
		buf.buf_addr = (*C.char)(dataptr)
		buf.len_used, buf.len_alloc = C.uint(len(s)), C.uint(len(s))
		dataptr = unsafe.Add(dataptr, len(s))
	}
	return n
}

// Return string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
func (n *Node) String() string {
	var bld strings.Builder
	cnode := n.n // access C.node from Go node
	for i := range cnode.len {
		buf := (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&cnode.buffers[0]), C.sizeof_ydb_buffer_t*i))
		s := C.GoStringN(buf.buf_addr, C.int(buf.len_used))
		if i > 0 {
			bld.WriteString("(\"")
		}
		bld.WriteString(s)
		if i > 0 {
			bld.WriteString("\")")
		}
	}
	return bld.String()
}

// Set the value of a database node.
func (n *Node) Set(val string) error {
	// Create a ydb_buffer_t pointing to go string
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	if len(val) > int(cconn.value.len_alloc) {
		panic("YDB: tried to set database value to a string longer than the maximum database value length")
	}
	n.conn.setValue(val)
	ret := C.ydb_set_st(cconn.tptoken, &cconn.errstr, &cnode.buffers[0], cnode.len-1, (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&cnode.buffers[0]), C.sizeof_ydb_buffer_t)), &cconn.value)

	return n.conn.GetError(ret)
}

// Get the value of a database node.
// On error return value "" and error
// If deflt is supplied return string deflt[0] instead of GVUNDEF or LVUNDEF errors.
func (n *Node) Get(deflt ...string) (string, error) {
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	err := C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers[0], cnode.len-1, (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(&cnode.buffers[0]), C.sizeof_ydb_buffer_t)), &cconn.value)
	if err == C.YDB_ERR_INVSTRLEN {
		// TODO: fix the following to realloc
		panic("YDB: have not yet implemented reallocating cconn.value to fit a large returned string")
	}
	if len(deflt) > 0 && (err == C.YDB_ERR_GVUNDEF || err == C.YDB_ERR_LVUNDEF) {
		return deflt[0], nil
	}
	if err != C.YDB_OK {
		return "", n.conn.GetError(err)
	}
	// take a copy of the string so that we can release `space`
	value := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	return value, nil
}
