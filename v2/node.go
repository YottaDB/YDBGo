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
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

/* #include "libyottadb.h"
#include "yottadb.h"
*/
import "C"

// indexBuf finds the address of index i within a ydb_buffer_t array.
// This function is necessary because CGo discards buffersn[] since it has no size.
func bufferIndex(buf *C.ydb_buffer_t, i int) *C.ydb_buffer_t {
	return (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(buf), C.sizeof_ydb_buffer_t*i))
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
	cnode *C.node
	conn  *Conn // Node.conn points to the Go conn; Node.cnode.conn will point directly to the C.conn
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
		first = C.GoStringN(node1.cnode.buffers.buf_addr, node1.cnode.datasize)
		firstLen = int(node1.cnode.len)
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
	n.cnode = (*C.node)(C.calloc(1, C.size_t(size)))
	if n.cnode == nil {
		panic("YDB: out of memory when allocating new reference to database node")
	}
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(cnode *C.node) {
		C.free(unsafe.Pointer(cnode))
	}, n.cnode)

	n.conn = conn
	cnode := n.cnode
	cnode.conn = conn.cconn // point to the C version of the conn
	cnode.len = C.int(len(subscripts) + firstLen)
	cnode.mutable = 0 // i.e. false

	dataptr := unsafe.Pointer(bufferIndex(&cnode.buffers, len(subscripts)+firstLen))
	// Note: have tried to replace the following with copy() to avoid a CGo invocation, but it's slower
	C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))

	// Now fill in ydb_buffer_t pointers
	if node1 != nil {
		// Copy node1.buffers to node.buffers
		C.memcpy(unsafe.Pointer(&cnode.buffers), unsafe.Pointer(&node1.cnode.buffers), C.size_t(node1.cnode.len)*C.sizeof_ydb_buffer_t)
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
		clone.cnode.conn = clone.conn.cconn
	}
	return clone
}

// Return string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
func (n *Node) String() string {
	var bld strings.Builder
	cnode := n.cnode // access C.node from Go node
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

// Set the value of a database node to val and return val.
// The val may be a string, integer, or float because it is converted to a string using fmt.Sprint().
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) Set(val string) string {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	n.conn.setValue(fmt.Sprint(val))
	status := C.ydb_set_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	if status != YDB_OK {
		panic(n.conn.GetError(status))
	}
	return val
}

// Get and return the value of a database node or deflt if it does not exist.
// Since a default is supplied, the only possible errors are panic-worthy, so this calls panic on them.
func (n *Node) Get(deflt ...string) string {
	val, err := n.GetIf()
	if err == nil {
		return val
	}
	status := err.(*YDBError).Code
	if status == ydberr.GVUNDEF || status == ydberr.LVUNDEF || status == ydberr.INVSVN {
		if len(deflt) == 0 {
			return ""
		}
		return deflt[0]
	}
	panic(err)
}

// Return the value of a database node if possible, otherwise return an error.
// The errors returned are GVUNDEF, LVUNDEF, INVSVN, and also other panic-worthy errors (e.g. invalid variable names),
// so you may safely use Get() instead.
func (n *Node) GetIf() (string, error) {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	status := C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	if status == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		n.conn.ensureValueSize(int(cconn.value.len_used))
		status = C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	}
	if status != YDB_OK {
		return "", n.conn.GetError(status)
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
//
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) Data() int {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	var val C.uint
	status := C.ydb_data_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &val)
	if status != YDB_OK {
		panic(n.conn.GetError(status))
	}
	return int(val)
}

// Returns whether the database node has a value.
// Use this in preference to node.Data()
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) HasValue() bool {
	return (n.Data() & 1) == 1
}

// Returns whether the database node has a tree of subscripts containing data.
// Use this in preference to node.Data()
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) HasTree() bool {
	return (n.Data() & 10) == 10
}

// Returns whether the database node has both tree and value.
// Use this in preference to node.Data()
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) HasTreeAndValue() bool {
	return (n.Data() & 11) == 11
}

// Returns whether the database node has neither tree nor value.
// Use this in preference to node.Data()
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) HasNone() bool {
	return (n.Data() & 11) == 0
}

// Kill deletes a database node including its value and any subtree.
// To delete only the value of a node use node.Clear()
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) Kill() {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	status := C.ydb_delete_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), C.YDB_DEL_TREE)
	if status != YDB_OK {
		panic(n.conn.GetError(status))
	}
}

// Delete the node value, not its child subscripts.
// Equivalent to YottaDB M command ZKILL
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) Clear() error {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	status := C.ydb_delete_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), C.YDB_DEL_NODE)
	return n.conn.GetError(status)
}

// Atomically increment the value of database node by amount.
// The amount may be an integer, float or string representation of the same, and defaults to 1 if not supplied.
// Convert the value of the node to a number first by discarding any trailing non-digits and returning zero if it is still not a number.
// Return the new value of the node.
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) Incr(amount ...interface{}) float64 {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	var numberString string
	if len(amount) == 0 {
		numberString = "1"
	} else {
		numberString = fmt.Sprint(amount[0])
	}
	n.conn.setValue(numberString)

	status := C.ydb_incr_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value, &cconn.value)
	if status != YDB_OK {
		panic(n.conn.GetError(status))
	}

	valuestring := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	value, err := strconv.ParseFloat(valuestring, 64)
	if err != nil {
		panic(err)
	}
	return value
}

// Grab attempts to acquire or increment the count a lock matching this node, waiting up to timeout for availability.
// Equivalent to the M `LOCK +` command.
// The timeout is supplied in timeout[0] in seconds. If no timeout is supplied, wait forever. A timeout of zero means try only once.
// Return true if lock was acquired; otherwise false.
// Panics with TIME2LONG if the timeout exceeds YDB_MAX_TIME_NSEC or on other panic-worthy errors (e.g. invalid variable names).
func (n *Node) Grab(timeout ...float64) bool {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	forever := len(timeout) == 0
	var timeoutNsec C.ulonglong
	if forever {
		timeoutNsec = YDB_MAX_TIME_NSEC
	} else {
		// Prevent overflow in timeout conversion to ulonglong and ensure the proper error is created
		timeoutNsec = C.ulonglong(timeout[0] * 1000000000)
		if timeout[0] > YDB_MAX_TIME_NSEC {
			timeoutNsec = YDB_MAX_TIME_NSEC + 1
		}
	}

	for {
		status := C.ydb_lock_incr_st(cconn.tptoken, &cconn.errstr, timeoutNsec, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1))
		if status == C.YDB_LOCK_TIMEOUT && !forever {
			return false
		} else if status != YDB_OK {
			panic(n.conn.GetError(status))
		}
	}
}

// Release decrements the count of a lock matching this node, releasing it if zero.
// Equivalent to the M `LOCK -` command.
// Returns nothing since releasing a lock cannot fail.
// Panics on errors because they are are all panic-worthy (e.g. invalid variable names).
func (n *Node) Release() {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	status := C.ydb_lock_decr_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1))
	if status != YDB_OK {
		panic(n.conn.GetError(status))
	}
}
