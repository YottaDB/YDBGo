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

	"lang.yottadb.com/go/yottadb/v2/ydberr"
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
	ydb_buffer_t buffers;	// first of an array of buffers (typically varname)
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

// indexBuf finds the address of index i within a ydb_buffer_t array.
// This function is necessary because CGo discards buffersn[] since it has no size.
func bufferIndex(buf *C.ydb_buffer_t, i int) *C.ydb_buffer_t {
	return (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(buf), C.sizeof_ydb_buffer_t*i))
}

// ---- Conn connection object

// Amount to overallocate string spaces for potential future use with larger strings.
// This could be set to C.YDB_MAX_STR to avoid any reallocation, but that would make all new connections large.
const overalloc = 1024

// Conn creates a thread-specific 'connection' object for calling the YottaDB API.
// You must use a different connection for each thread.
//
// This struct wraps C.conn in a Go struct so Go can add methods to it.
type Conn struct {
	// Pointer to C.conn rather than the item itself so we can malloc it and point to it from C without Go moving it.
	cconn *C.conn
}

// NewConn creates a new database connection for the current goroutine.
// Each goroutine must have its own connection.
func NewConn() *Conn {
	initCheck()

	var conn Conn
	// This initial call must be to calloc() to get initialized (cleared) storage: due to a documented cgo bug
	// we must not let Go store pointer values in uninitialized C-allocated memory or errors may result.
	// See the cgo bug mentioned at https://golang.org/cmd/cgo/#hdr-Passing_pointers.
	conn.cconn = (*C.conn)(C.calloc(1, C.sizeof_conn))
	if conn.cconn == nil {
		panic("YDB: out of memory when allocating new database connection")
	}
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
		C.free(unsafe.Pointer(cn))
	}, conn.cconn)
	return &conn
}

// ensureValueSize reallocates value.buf_addr if necessary to fit a string of size.
func (conn *Conn) ensureValueSize(cap int) {
	if cap > C.YDB_MAX_STR {
		panic("YDB: tried to set database value to a string longer than the maximum length YDB_MAX_STR")
	}
	value := &conn.cconn.value
	if cap > int(value.len_alloc) {
		cap += overalloc // allocate some extra for potential future use
		addr := (*C.char)(C.realloc(unsafe.Pointer(value.buf_addr), C.size_t(cap)))
		if addr == nil {
			panic("YDB: out of memory when allocating more space for string data transfer to YottaDB")
		}
		value.buf_addr = addr
		value.len_alloc = C.uint(cap)
	}
}

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
	err := C.ydb_zwr2str_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	if err == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		conn.ensureValueSize(int(cconn.value.len_used))
		err = C.ydb_zwr2str_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	}
	if err != YDB_OK {
		return "", conn.GetError(err)
	}
	val := conn.getValue()
	if val == "" && zstr != "" {
		return "", NewError(0, "string has invalid ZWRITE-format")
	}
	return val, nil
}

// Str2Zwr takes the given Go string and converts it to return a ZWRITE-formatted string
// If the returned zwrite-formatted string does not fit within the maximum YottaDB string size, return error code ydberr.INVSTRLEN.
// Otherwise, return the ZWRITE-formatted string.
// Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
func (conn *Conn) Str2Zwr(str string) (string, error) {
	cconn := conn.cconn
	cbuf := conn.setValue(str)
	ret := C.ydb_str2zwr_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	if ret == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		conn.ensureValueSize(int(cconn.value.len_used))
		cbuf := conn.setValue(str)
		ret = C.ydb_str2zwr_st(cconn.tptoken, &cconn.errstr, cbuf, cbuf)
	}
	if ret != YDB_OK {
		return "", conn.GetError(ret)
	}
	return conn.getValue(), nil
}

// ---- Node object

// Node is an object containing strings that represents a YottaDB node.
// Stores all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
// structs that point to each successive string, to provide fast access to YottaDB API functions.
// Regular Nodes are immutable. There is a mutable version of Node emitted by Node iterators, which
// will change each loop. If you need to take a snapshot of a mutable node this may be done with node.Clone().
//
// Thread Safety: Do not run database actions on node objects created in another thread. If you want to
// act on a node object passed in from another thread, first call node.Clone(conn) to make a copy of the
// other thread's node object using the current thread's connection `conn`. Then perform methods on that.

// This struct wraps a C.node struct in a Go struct so Go can add methods to it.
type Node struct {
	// Pointer to C.node rather than the item itself so we can point to it from C without Go moving it.
	n    *C.node
	conn *Conn // Node.conn points to the Go conn; Node.n.conn will point directly to the C.conn
}

// Creates a `Node` type instance that represents a database node with methods that access YottaDB.
// The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
// varname may be a string or, if it is another node, that node's path strings will be prepended to `subscripts`.
func (conn *Conn) _Node(varname interface{}, subscripts ...string) (n *Node) {
	// Note: benchmarking shows that the use of interface{} slows down node creation almost immeasurably (< 0.1%)
	// Concatenate strings the fastest Go way.
	// This involves creating an extra copy of subscripts but is probably faster than one C.memcpy call per subscript
	var joiner bytes.Buffer
	var first string // first string stored in joiner
	var firstLen int // number of subscripts in first string
	var node1 *Node  // if varname is a node, store it in here
	switch val := varname.(type) {
	case *Node:
		node1 = val
		first = C.GoStringN(node1.n.buffers.buf_addr, node1.n.datasize)
		firstLen = int(node1.n.len)
	default:
		first = val.(string)
		firstLen = 1
	}
	joiner.WriteString(first)
	for _, s := range subscripts {
		joiner.WriteString(s)
	}

	size := C.sizeof_node + C.sizeof_ydb_buffer_t*(firstLen-1+len(subscripts)) + joiner.Len()
	var goNode Node
	n = &goNode
	// This initial call must be to calloc() to get initialized (cleared) storage: due to a documented cgo bug
	// we must not let Go store pointer values in uninitialized C-allocated memory or errors may result.
	// See the cgo bug mentioned at https://golang.org/cmd/cgo/#hdr-Passing_pointers.
	n.n = (*C.node)(C.calloc(1, C.size_t(size)))
	if n.n == nil {
		panic("YDB: out of memory when allocating new reference to database node")
	}
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(cnode *C.node) {
		C.free(unsafe.Pointer(cnode))
	}, n.n)

	n.conn = conn // point to the Go conn
	cnode := n.n
	cnode.conn = conn.cconn // point to the C version of the conn
	cnode.len = C.int(len(subscripts) + firstLen)
	cnode.mutable = 0 // i.e. false

	dataptr := unsafe.Pointer(bufferIndex(&cnode.buffers, len(subscripts)+firstLen))
	// Note: have tried to replace the following with copy() to avoid a CGo invocation, but it's slower
	C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))

	// Now fill in ydb_buffer_t pointers
	if node1 != nil {
		// Copy node1.buffers to node.buffers
		C.memcpy(unsafe.Pointer(&cnode.buffers), unsafe.Pointer(&node1.n.buffers), C.size_t(node1.n.len)*C.sizeof_ydb_buffer_t)
		dataptr = unsafe.Add(dataptr, len(first))
	} else {
		s := first
		buf := bufferIndex(&cnode.buffers, 0)
		buf.buf_addr = (*C.char)(dataptr)
		buf.len_used, buf.len_alloc = C.uint(len(s)), C.uint(len(s))
		dataptr = unsafe.Add(dataptr, len(s))
	}
	for i, s := range subscripts {
		buf := bufferIndex(&cnode.buffers, i+firstLen)
		buf.buf_addr = (*C.char)(dataptr)
		buf.len_used, buf.len_alloc = C.uint(len(s)), C.uint(len(s))
		dataptr = unsafe.Add(dataptr, len(s))
	}
	return n
}

// Creates a `Node` type instance that represents a database node with methods that access YottaDB.
// The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
func (conn *Conn) Node(varname string, subscripts ...string) (n *Node) {
	return conn._Node(varname, subscripts...)
}

// Creates a child node of parent that represents parent with subscripts appended.
func (parent *Node) Child(subscripts ...string) (n *Node) {
	return parent.conn._Node(parent, subscripts...)
}

// Creates a clone of node, n, optionally for use with a different connection, conn[0], in a different goroutine.
// Nodes may be passed to another goroutine but not used to access the database unless first cloned to another connection.
func (parent *Node) Clone(conn ...(*Conn)) (clone *Node) {
	clone = parent.conn._Node(parent)
	if len(conn) > 0 {
		clone.conn = conn[0]
		cnode := clone.n
		cnode.conn = clone.conn.cconn
	}
	return clone
}

// Return string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
func (n *Node) String() string {
	var bld strings.Builder
	cnode := n.n // access C.node from Go node
	for i := range cnode.len {
		buf := bufferIndex(&cnode.buffers, int(i))
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

// Set the value of a database node to val.
func (n *Node) Set(val string) error {
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	n.conn.setValue(val)
	ret := C.ydb_set_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)

	return n.conn.GetError(ret)
}

// Get the value of a database node.
// On error return the empty string and the error.
// If deflt is supplied return string deflt[0] instead of GVUNDEF or LVUNDEF errors.
func (n *Node) Get(deflt ...string) (string, error) {
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	err := C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	if err == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		n.conn.ensureValueSize(int(cconn.value.len_used))
		err = C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	}
	if len(deflt) > 0 && (err == ydberr.GVUNDEF || err == ydberr.LVUNDEF) {
		return deflt[0], nil
	}
	if err != C.YDB_OK {
		return "", n.conn.GetError(err)
	}
	// take a copy of the string so that we can release `space`
	value := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	return value, nil
}

// Data returns whether the database node has a value or subnodes as follows:
//   - 0: node has neither a value nor a subtree, i.e., it is undefined.
//   - 1: node has a value, but no subtree
//   - 10: node has no value, but does have a subtree
//   - 11: node has both value and subtree
func (n *Node) Data() int {
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	var val C.uint
	err := C.ydb_data_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &val)
	if err != YDB_OK {
		panic(n.conn.GetError(err))
	}
	return int(val)
}

// Returns whether the database node has a value.
// Use this in preference to node.Data()
func (n *Node) HasValue() bool {
	return (n.Data() & 1) == 1
}

// Returns whether the database node has a tree of subscripts containing data.
// Use this in preference to node.Data()
func (n *Node) HasTree() bool {
	return (n.Data() & 10) == 10
}

// Returns whether the database node has both tree and value.
// Use this in preference to node.Data()
func (n *Node) HasTreeAndValue() bool {
	return (n.Data() & 11) == 11
}

// Returns whether the database node has neither tree nor value.
// Use this in preference to node.Data()
func (n *Node) HasNone() bool {
	return (n.Data() & 11) == 0
}

// Kill deletes a database node including its value and any subtree.
// To delete only the value of a node use node.Clear()
func (n *Node) Kill() error {
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	ret := C.ydb_delete_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), C.YDB_DEL_TREE)
	return n.conn.GetError(ret)
}

// Delete the node value, not its child subscripts.
// Equivalent to YottaDB M command ZKILL
func (n *Node) Clear() error {
	cnode := n.n // access C.node from Go node
	cconn := cnode.conn
	ret := C.ydb_delete_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), C.YDB_DEL_NODE)
	return n.conn.GetError(ret)
}
