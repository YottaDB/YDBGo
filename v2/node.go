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
	"iter"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

/* #include "libyottadb.h"
#include "yottadb.h"

// It's tempting to apply `#cgo nocallback` directives to all ydb_* C functions (1% call speed increase - tested),
// but this is wrong because ydb_* function can call the Go function signalExitCallback() when a signal occurs.
*/
import "C"

// indexBuf finds the address of index i within a ydb_buffer_t array.
//   - This function is necessary because CGo discards Node.cnode.buffersn[] since it has no size.
func bufferIndex(buf *C.ydb_buffer_t, i int) *C.ydb_buffer_t {
	return (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(buf), C.sizeof_ydb_buffer_t*i))
}

// ---- Node object

// Node is an object containing strings that represents a YottaDB node.
//   - Stores all the supplied strings (varname and subscripts) in the Node object along with array of C.ydb_buffer_t
//     structs that point to each successive string, to provide fast access to YottaDB API functions.
//   - Regular Nodes are immutable. There is a mutable version of Node emitted by [Node.Next]() and Node iterators, which
//     will change each loop. If you need to take an immutable snapshot of a mutable node this may be done with [Node.Clone]().
//   - Concurrency: Do not run database actions on node objects created in another goroutine. If you want to
//     act on a node object passed in from another goroutine, first call [Node.Clone](conn) to make a copy of the
//     other goroutine's node object using the current goroutine's connection `conn`. Then perform methods on that.
//
// Node methods panic on errors because they are are all panic-worthy (e.g. invalid variable names).
// See [yottadb.Error] for error strategy and rationale.
type Node struct {
	// Node type wraps a C.node struct in a Go struct so Go can add methods to it.
	// Pointer to C.node rather than the item itself so we can point to it from C without Go moving it.
	cnode   *C.node
	Conn    *Conn // Node.Conn points to the Go conn; Node.cnode.conn will point directly to the C.conn
	mutable bool  // Whether the node may be altered for fast iteration
}

// Convert []string to []any.
// Used to pass string arrays to Node
func stringArrayToAnyArray(strings []string) []any {
	array := make([]any, len(strings))
	for i, s := range strings {
		array[i] = s
	}
	return array
}

// _Node creates a `Node` type instance that represents a database node with methods that access YottaDB.
//   - The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
//   - The varname and subscripts may be of type string, []byte slice, or an integer or float type; numeric types are converted to a string using the appropriate strconv function.
//   - The varname may also be another node, in which case that node's subscript strings will be prepended to `subscripts` to build the new node.
func (conn *Conn) _Node(varname any, subscripts []any) (n *Node) {
	// Note: benchmarking shows that the use of any slows down node creation almost immeasurably (< 0.1%)
	// Concatenate strings the fastest Go way.
	// This involves creating an extra copy of subscripts but is probably faster than one C.memcpy call per subscript
	var joiner bytes.Buffer
	var first string // first string stored in joiner
	var firstLen int // number of subscripts in first string
	var node1 *Node  // if varname is a node, store it in here
	subs := make([]string, len(subscripts))
	switch val := varname.(type) {
	case *Node:
		node1 = val
		cnode := node1.cnode
		firstLen = int(cnode.len)
		firstStringAddr := cnode.buffers.buf_addr
		lastbuf := bufferIndex(&cnode.buffers, firstLen-1)
		// Calculate data size of all strings in node to copy = address of last string - address of first string + length of last string
		datasize := C.uint(uintptr(unsafe.Pointer(lastbuf.buf_addr))) - C.uint(uintptr(unsafe.Pointer(firstStringAddr)))
		datasize += lastbuf.len_used
		first = C.GoStringN(firstStringAddr, C.int(datasize))
	default:
		first = anyToString(val)
		firstLen = 1
	}
	joiner.WriteString(first)
	for i, s := range subscripts {
		subs[i] = anyToString(s)
		joiner.WriteString(subs[i])
	}

	size := C.sizeof_node + C.sizeof_ydb_buffer_t*(firstLen-1+len(subscripts)) + joiner.Len()
	n = &Node{}
	n.cnode = (*C.node)(calloc(C.size_t(size))) // must use our calloc, not malloc: see calloc doc
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(cnode *C.node) {
		C.free(unsafe.Pointer(cnode))
	}, n.cnode)

	n.Conn = conn
	cnode := n.cnode
	cnode.conn = conn.cconn // point to the C version of the conn
	cnode.len = C.int(len(subscripts) + firstLen)

	dataptr := unsafe.Pointer(bufferIndex(&cnode.buffers, len(subscripts)+firstLen))
	if joiner.Len() > 0 {
		// Note: have tried to replace the following with copy() to avoid a CGo invocation, but it's slower
		C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))
	}

	// Function to each buffer to dataptr++ as I loop through strings
	setbuf := func(buf *C.ydb_buffer_t, length C.uint) {
		buf.buf_addr = (*C.char)(dataptr)
		buf.len_used, buf.len_alloc = length, length
		dataptr = unsafe.Add(dataptr, length)
	}

	// Now fill in ydb_buffer_t pointers
	if node1 != nil {
		// First set buffers for all strings copied from parent node
		for i := range firstLen {
			buf := bufferIndex(&cnode.buffers, i)
			setbuf(buf, bufferIndex(&node1.cnode.buffers, i).len_used)
		}
		runtime.KeepAlive(node1) // ensure node1 sticks around until we've finished copying data from it's C allocation
	} else {
		buf := bufferIndex(&cnode.buffers, 0)
		setbuf(buf, C.uint(len(first)))
	}
	for i, s := range subs {
		buf := bufferIndex(&cnode.buffers, i+firstLen)
		setbuf(buf, C.uint(len(s)))
	}
	return n
}

// Node method creates a `Node` type instance that represents a database node with methods that access YottaDB.
//   - The strings and array are stored in C-allocated space to give Node methods fast access to YottaDB API functions.
//   - The varname and subscripts may be of type string, []byte slice, or an integer or float type; numeric types are converted to a string using the appropriate strconv function.
func (conn *Conn) Node(varname string, subscripts ...any) (n *Node) {
	return conn._Node(varname, subscripts)
}

// CloneNode creates a copy of node associated with conn (in case node was created using a different conn).
// A node associated with a conn used by another goroutine must not be used by the current goroutine except as
// a parameter to CloneNode(). If this rule is not obeyed, then the two goroutines could have their transaction depth
// and error messages mixed up. It is the programmer's responsibility to ensure this does not happen by using CloneNode.
// This does the same as n.Clone() except that it can switch to new conn.
// Only immutable nodes are returned.
func (conn *Conn) CloneNode(n *Node) *Node {
	return conn._Node(n, nil)
}

// Child creates a child node of parent that represents parent with subscripts appended.
//   - [Node.Clone]() without parameters is equivalent to [Node.Child]() without parameters.
func (n *Node) Child(subscripts ...any) (child *Node) {
	return n.Conn._Node(n, subscripts)
}

// Clone creates an immutable copy of node.
//   - [Node.Clone]() is equivalent to calling [Node.Child]() without parameters.
//
// See [Node.IsMutable]() for notes on mutability.
func (n *Node) Clone() (clone *Node) {
	return n.Conn._Node(n, nil)
}

// Subscripts returns a string that holds the specified varname or subscript of the given node.
// An index of zero returns the varname; higher numbers return the respective subscript.
// A negative index returns a subscript counted from the end (the last is -1).
// An out-of-range subscript panics.
func (n *Node) Subscript(index int) string {
	cnode := n.cnode // access C.node from Go node
	if index < 0 {
		index = int(cnode.len) + index
	}
	if index < 0 || index >= int(cnode.len) {
		panic(errorf(ydberr.InvalidSubscriptIndex, "subscript %d out of bounds (0-%d)", index, cnode.len))
	}
	buf := bufferIndex(&cnode.buffers, index)
	r := C.GoStringN(buf.buf_addr, C.int(buf.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r
}

// Subscripts returns a slice of strings that represent the varname and subscript names of the given node.
func (n *Node) Subscripts() []string {
	cnode := n.cnode // access C.node from Go node
	strings := make([]string, cnode.len)
	for i := range cnode.len {
		buf := bufferIndex(&cnode.buffers, int(i))
		s := C.GoStringN(buf.buf_addr, C.int(buf.len_used))
		strings[i] = s
	}
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return strings
}

// String returns a string representation of this database node in typical YottaDB format: `varname("sub1")("sub2")`.
//   - Output subscripts as unquoted numbers if they convert to float64 and back without change (using [Node.Quote]).
//   - Output strings in YottaDB ZWRITE format
func (n *Node) String() string {
	var bld strings.Builder
	subs := n.Subscripts()
	for i, s := range subs {
		if i == 0 {
			bld.WriteString(s)
			continue
		}
		if i == 1 {
			bld.WriteString("(")
		}
		bld.WriteString(n.Conn.Quote(s))
		if i == len(subs)-1 {
			bld.WriteString(")")
		} else {
			bld.WriteString(",")
		}
	}
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return bld.String()
}

// GoString makes print("%#v") dump the node and its contents with [Node.Dump](30, 80).
// For example the output format of Node("person",42).GoString() might look like this:
//
//	person(42)=1234
//	person(42)("age")="49"
//	person(42)("height")("centimeters")=190.5
//	person(42)("height")("inches")=1234
//	person(42)("name")="Joe Bloggs"
func (n *Node) GoString() string {
	return n.Dump(30, 80)
}

// Dump returns a string representation of this database node and subtrees with their contents in YottaDB ZWRITE format.
// Output subscripts and values as unquoted numbers if they convert to float64 and back without change (using [Node.Quote]).
// Output strings in YottaDB ZWRITE format.
// See [Node.GoString] for an example of the output format.
// Two optional integers may be supplied to specify maximums where both default to -1:
//   - first specifies the maximum number of lines to output, where -1 means infinite. If lines are truncated, the output ends with "\n...\n".
//   - second specifies the maximum number of characters at which to truncate values prior to output where -1 means infinite.
//     Truncated values are output with suffix "..." after any ending quotes. Note that conversion to ZWRITE format may expand this.
func (n *Node) Dump(args ...int) string {
	var bld strings.Builder
	if len(args) > 2 {
		panic(errorf(ydberr.TooManyParameters, "%d parameters supplied to Dump() which only takes 2", len(args)))
	}
	args = append(args, -1, -1) // defaults
	maxLines, maxString := args[0], args[1]
	i := 0
	for node := range n.Tree() {
		i++
		if maxLines != -1 && i > maxLines {
			bld.WriteString("...\n")
			break
		}
		bld.WriteString(node.String())
		bld.WriteString("=")
		val := node.Get("<Variable '" + node.Subscript(0) + "' deleted while iterating it>")
		if maxString != -1 && len(val) > maxString {
			bld.WriteString(node.Conn.Quote(val[:maxString]))
			bld.WriteString("...")
		} else {
			bld.WriteString(node.Conn.Quote(val))
		}
		bld.WriteString("\n")
	}
	return bld.String()
}

// Set applies val to the value of a database node.
//   - The val may be a string, []byte slice, or an integer or float type; numeric types are converted to a string using the appropriate strconv function.
func (n *Node) Set(val any) {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	n.Conn.setAnyValue(val)
	n.Conn.prepAPI()
	status := C.ydb_set_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
}

// Get fetches and returns the value of a database node or defaultValue[0] if the node's variable name does not exist.
//   - Note that the default only works on undefined variables, not on empty *subscripted* variables.
//     If the node's variable name (M local or M global) exists but the subscripted node has no value, Get() will return the empty string.
//     If you need to distinguish between an empty string and a value-less node you must use [Node.HasValue]()
//   - Since a default is supplied, the only possible errors are panic-worthy, so this calls panic on them.
func (n *Node) Get(defaultValue ...string) string {
	ok := n._Lookup()
	if !ok {
		if len(defaultValue) == 0 {
			return ""
		}
		return defaultValue[0]
	}
	cconn := n.cnode.conn
	// copy cconn.value into a Go type so that cconn.value can be re-used for another ydb call
	r := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r
}

// GetInt fetches and returns the value of a database node as an integer.
// Return zero if the node's value does not exist or is not convertable to an integer.
func (n *Node) GetInt() int {
	val := n.Get()
	num, err := strconv.ParseInt(val, 10, 0)
	if err != nil {
		if _, ok := err.(*strconv.NumError); !ok {
			panic(err) // unknown error unrelated to conversion
		}
		return 0
	}
	return int(num)
}

// GetFloat fetches and returns the value of a database node as a float64.
// Return zero if the node's value does not exist or is not convertable to float64.
func (n *Node) GetFloat() float64 {
	val := n.Get()
	num, err := strconv.ParseFloat(val, 64)
	if err != nil {
		if _, ok := err.(*strconv.NumError); !ok {
			panic(err) // unknown error unrelated to conversion
		}
		return 0
	}
	return num
}

// GetBytes is the same as [Node.Get] except that it accepts and returns []byte slices rather than strings.
func (n *Node) GetBytes(defaultValue ...[]byte) []byte {
	ok := n._Lookup()
	if !ok {
		if len(defaultValue) == 0 {
			return []byte{}
		}
		return defaultValue[0]
	}
	cconn := n.cnode.conn
	// copy cconn.value into a Go type so that cconn.value can be re-used for another ydb call
	r := C.GoBytes(unsafe.Pointer(cconn.value.buf_addr), C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r
}

// Lookup returns the value of a database node and true, or if the variable name could not be found, returns the empty string and false.
//   - If the node's variable name (M local or M global) exists but the subscripted node has no value, Lookup() will return the empty string and true.
//     If you need to distinguish between an empty string and a value-less node you must use [Node.HasValue]()
//   - bool false is returned on errors GVUNDEF (undefined M global), LVUNDEF (undefined M local), or INVSVN (invalid Special Variable Name).
//   - You may use [Node.Get]() to return a default value when an undefined variable is accessed.
func (n *Node) Lookup() (string, bool) {
	ok := n._Lookup()
	if !ok {
		return "", false
	}
	cconn := n.cnode.conn
	// copy cconn.value into a Go type so that cconn.value can be re-used for another ydb call
	r := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r, true
}

// LookupBytes is the same as [Node.Lookup] except that it returns the value as a []byte slice rather than a string.
func (n *Node) LookupBytes() ([]byte, bool) {
	ok := n._Lookup()
	if !ok {
		return []byte{}, false
	}
	cconn := n.cnode.conn
	// copy cconn.value into a Go type so that cconn.value can be re-used for another ydb call
	r := C.GoBytes(unsafe.Pointer(cconn.value.buf_addr), C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r, true
}

// _Lookup returns the value of a database node in n.cconn.value and returns whether the variable name could be found.
func (n *Node) _Lookup() bool {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	n.Conn.prepAPI()
	status := C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	if status == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		n.Conn.ensureValueSize(int(cconn.value.len_used))
		n.Conn.prepAPI()
		status = C.ydb_get_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
	}
	if status == ydberr.GVUNDEF || status == ydberr.LVUNDEF || status == ydberr.INVSVN {
		return false
	}
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
	return true
}

// data returns whether the database node has a value or subnodes as follows:
//   - 0: node has neither a value nor a subtree, i.e., it is undefined.
//   - 1: node has a value, but no subtree
//   - 10: node has no value, but does have a subtree
//   - 11: node has both value and subtree
//
// It is private because it really isn't a nice name.
func (n *Node) data() int {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	var val C.uint
	n.Conn.prepAPI()
	status := C.ydb_data_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &val)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
	return int(val)
}

// HasValue returns whether the database node has a value.
func (n *Node) HasValue() bool {
	return (n.data() & 1) == 1
}

// HasValueOnly returns whether the database node has a value but no tree.
func (n *Node) HasValueOnly() bool {
	return n.data() == 1
}

// HasTree returns whether the database node has a tree of subscripts containing data.
func (n *Node) HasTree() bool {
	return (n.data() & 10) == 10
}

// HasTreeOnly returns whether the database node has no value but does have a tree of subscripts that contain data
func (n *Node) HasTreeOnly() bool {
	return n.data() == 10
}

// HasBoth returns whether the database node has both tree and value.
func (n *Node) HasBoth() bool {
	return (n.data() & 11) == 11
}

// HasNone returns whether the database node has neither tree nor value.
func (n *Node) HasNone() bool {
	return (n.data() & 11) == 0
}

// Kill deletes a database node including its value and any subtree.
//   - To delete only the value of a node use [Node.Clear]()
func (n *Node) Kill() {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	n.Conn.prepAPI()
	status := C.ydb_delete_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), C.YDB_DEL_TREE)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
}

// Clear deletes the node value, not its child subscripts.
//   - Equivalent to YottaDB M command ZKILL
func (n *Node) Clear() {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn
	n.Conn.prepAPI()
	status := C.ydb_delete_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), C.YDB_DEL_NODE)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
}

// Incr atomically increments the value of database node by amount.
//   - The amount may be an integer, float or string representation of the same.
//   - YottaDB first converts the value of the node to a number by discarding any trailing non-digits and returning zero if it is still not a number.
//     Then it adds amount to the node, all atomically.
//   - Return the new value of the node as a string
func (n *Node) Incr(amount any) string {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	n.Conn.setAnyValue(amount)
	if cconn.value.len_used == 0 {
		panic(errorf(ydberr.IncrementEmpty, `cannot increment by the empty string ""`))
	}

	n.Conn.prepAPI()
	status := C.ydb_incr_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value, &cconn.value)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}

	valuestring := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return valuestring
}

// Lock attempts to acquire or increment the count a lock matching this node, waiting up to timeout for availability.
// Equivalent to the M `LOCK +lockpath` command.
//   - If no timeout is supplied, wait forever. A timeout of zero means try only once.
//   - Return true if lock was acquired; otherwise false.
//   - Panics with TIME2LONG if the timeout exceeds YDB_MAX_TIME_NSEC or on other panic-worthy errors (e.g. invalid variable names).
func (n *Node) Lock(timeout ...time.Duration) bool {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	forever := len(timeout) == 0
	var timeoutNsec C.ulonglong
	if forever {
		timeoutNsec = YDB_MAX_TIME_NSEC
	} else {
		timeoutNsec = C.ulonglong(timeout[0].Nanoseconds())
	}

	for {
		n.Conn.prepAPI()
		status := C.ydb_lock_incr_st(cconn.tptoken, &cconn.errstr, timeoutNsec, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1))
		if status == YDB_OK {
			return true
		}
		if status == C.YDB_LOCK_TIMEOUT && !forever {
			return false
		}
		if status != YDB_OK {
			panic(n.Conn.lastError(status))
		}
	}
}

// Unlock decrements the count of a lock matching this node, releasing it if zero.
// Equivalent to the M `LOCK -lockpath` command.
//   - Returns nothing since releasing a lock cannot fail.
func (n *Node) Unlock() {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	n.Conn.prepAPI()
	status := C.ydb_lock_decr_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1))
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
}

// Mutate returns a mutable version of node n with the final subscript changed to the given value.
//   - Only creates a clone of node if the supplied node is not already mutable or doesn't have enough space for the changed subscript.
//
// Since creating new nodes is expensive, this method may be used for a fast iterator. For example, to kill nodes var(0..1000000):
//
//	n := conn.Node("var", "").Mutate("")
//	for i := range 1000000 {
//	  n.Mutate(strconv.Itoa(i)).kill()
//	}
func (n *Node) Mutate(val any) *Node {
	value := anyToString(val)
	cnode := n.cnode // access C equivalents of Go types
	retNode := n
	lastbuf := bufferIndex(&cnode.buffers, int(cnode.len-1))
	if !n.IsMutable() || int(lastbuf.len_alloc) < len(value) {
		strs := stringArrayToAnyArray(n.Subscripts())
		// Overallocate by appending preallocSubscript to value
		strs[len(strs)-1] = value + preallocSubscript
		retNode = n.Conn._Node(strs[0], strs[1:])
		retNode.mutable = true
		lastbuf := bufferIndex(&retNode.cnode.buffers, len(strs)-1)
		lastbuf.len_used = C.uint(len(value))
	} else {
		C.memcpy(unsafe.Pointer(lastbuf.buf_addr), unsafe.Pointer(unsafe.StringData(value)), C.size_t(len(value)))
		lastbuf.len_used = C.uint(len(value))
	}
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data into it's C allocation
	return retNode
}

// IsMutable returns whether given node is mutable.
//   - Mutable nodes are returned only by [Node.Next]() and generated by [Node.Iterate](). Mutable nodes will change their final
//     subscript each iteration or each call to Node.Next().
//   - If you need to take an immutable snapshot of a mutable node this may be done with [Node.Clone]().
func (n *Node) IsMutable() bool {
	return n.mutable
}

// This subscript name is used to preallocate subscript space in mutable nodes that may have their final subscript name changed.
// When it is exceeded by subsequent Node.Next() iterations, the entire node must be cloned to achieve reallocation
var preallocSubscript string = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// _next returns the name of the next subscript at the same depth level as the given node.
// This implements the logic for both Next() and Prev().
//   - If the parameter reverse is true, fetch the next node in reverse order, i.e. Prev().
//   - bool returns with false only if there are no more subscripts
//
// See further documentation at Next().
func (n *Node) _next(reverse bool) (string, bool) {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	var status C.int
	for range 2 {
		n.Conn.prepAPI()
		if reverse {
			status = C.ydb_subscript_previous_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
		} else {
			status = C.ydb_subscript_next_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &cconn.value)
		}
		if status == ydberr.INVSTRLEN {
			// Allocate more space and retry the call
			n.Conn.ensureValueSize(int(cconn.value.len_used))
			continue
		}
		break
	}

	if status == ydberr.NODEEND {
		return "", false
	}
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
	r := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r, true
}

// Next returns a Node instance pointing to the next subscript at the same depth level.
// Return a mutable node pointing to the database node with the next subscript after the given node, at the same depth level.
// Unless you want to start half way through a sequence of subscripts, it's usually tidier to use Node.Iterate() instead.
//   - Equivalent to the M function [$ORDER()] and has the same treatment of 'null subscripts' (i.e. empty strings).
//   - The order of returned nodes matches the collation order of the M database.
//   - The node path supplied does not need to exist in the database to find the next match.
//   - If the supplied node n contains only a variable name without subscripts, the next variable (GLVN) name is returned instead of the next subscript.
//
// Returns nil when there are no more subscripts at the level of the supplied node path, or a mutable node as follows:
//   - if the supplied node is immutable, a mutable clone of n with its final subscript changed to the next node.
//   - if the supplied node is mutable, the same node n with its final subscript changed to the next node.
//
// If you need to take an immutable snapshot of the returned mutable node this may be done with [Node.Clone]()
//
// See:
//   - Compare [Node.Prev]()
//   - [Node.Iterate]() for an iterator version and [Node.TreeNext]() for traversal of nodes in a way that descends into the entire tree.
//
// [$ORDER()]: https://docs.yottadb.com/ProgrammersGuide/functions.html#order
func (n *Node) Next() *Node {
	next, ok := n._next(false)
	if !ok {
		return nil
	}
	return n.Mutate(next)
}

// Prev is the same as Next but operates in the reverse order.
// See [Node.Next]()
func (n *Node) Prev() *Node {
	next, ok := n._next(true)
	if !ok {
		return nil
	}
	return n.Mutate(next)
}

// _children returns an interator that can FOR-loop through all a node's single-depth child subscripts.
// This implements the logic for both Children() and ChildrenBackward().
//   - If the parameter reverse is true, iterate nodes in reverse order.
//
// See further documentation at Iterate().
func (n *Node) _children(reverse bool) iter.Seq2[*Node, string] {
	// The next 3 lines are functionally n := n.Child("") but result in faster subsequent code because they create a mutable
	// node with enough spare subscript space to avoid Node.Next() having to immediately create a mutable node.
	n = n.Child(preallocSubscript)
	n.mutable = true
	n = n.Mutate("")

	return func(yield func(*Node, string) bool) {
		for {
			next, ok := n._next(reverse)
			if !ok {
				return
			}
			n = n.Mutate(next)
			if !yield(n, next) {
				return
			}
		}
	}
}

// Children returns an interator over immediate child nodes for use in a FOR-loop.
// This iterator is a wrapper for [Node.Next](). It yields two values:
//   - a mutable node instance with final subscripts changed to successive subscript names
//   - the name of the child subscript (optionally assigned with a range statement)
//
// Notes:
//   - Treats 'null subscripts' (i.e. empty strings) in the same way as M function [$ORDER()].
//   - The order of returned nodes matches the collation order of the M database.
//   - This function never adjusts the supplied node even if it is mutable (it always creates its own mutable copy).
//   - If you need to take an immutable snapshot of the returned mutable node, use [Node.Clone]().
//
// See:
//   - [Node.ChildrenBackward]().
//   - [Node.Tree]() for traversal of nodes in a way that descends into the entire tree.
//
// [$ORDER()]: https://docs.yottadb.com/ProgrammersGuide/functions.html#order
func (n *Node) Children() iter.Seq2[*Node, string] {
	return n._children(false)
}

// ChildrenBackward is the same as Children but operates in reverse order.
// See [Node.Children]().
func (n *Node) ChildrenBackward() iter.Seq2[*Node, string] {
	return n._children(true)
}

// _treeNext returns the next node in the traversal of a database tree of a database variable.
// This implements the logic for both TreeNext() and TreePrev().
//   - If the parameter reverse is true, iterate the tree in reverse order.
//
// See further documentation at TreeNext().
func (n *Node) _treeNext(reverse bool) *Node {
	cnode := n.cnode // access C equivalents of Go types
	cconn := cnode.conn

	// Preallocate child subscripts of this size as a reasonable guess of space to fit most subscripts
	retNode := n.Child(preallocSubscript) // Create new node to store result with a single preallocated child
	var retSubs C.int
	var malloced bool // whether we had to malloc() and hence defer free()
	var status C.int
	for {
		retSubs = retNode.cnode.len - 1 // -1 because cnode counts the varname as a subscript and ydb_node_next_st() does not
		n.Conn.prepAPI()
		if reverse {
			status = C.ydb_node_previous_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &retSubs, bufferIndex(&retNode.cnode.buffers, 1))
		} else {
			status = C.ydb_node_next_st(cconn.tptoken, &cconn.errstr, &cnode.buffers, cnode.len-1, bufferIndex(&cnode.buffers, 1), &retSubs, bufferIndex(&retNode.cnode.buffers, 1))
		}
		if status == ydberr.INSUFFSUBS {
			if debugMode {
				fmt.Printf("INSUFFSUBS: %d (need %d)\n", retNode.cnode.len-1, retSubs)
			}
			extraStrings := make([]any, retSubs-(retNode.cnode.len-1))
			// Pre-fill node subscripts
			for i := range extraStrings {
				extraStrings[i] = preallocSubscript
			}
			retNode = retNode.Child(extraStrings...)
			continue
		}
		if status == ydberr.INVSTRLEN {
			if debugMode {
				fmt.Printf("INVSTRLEN subscript %d\n", retSubs)
			}
			buf := bufferIndex(&retNode.cnode.buffers, int(retSubs+1)) // +1 because cnode counts the varname as a subscript and ydb_node_next_st() does not
			len := buf.len_used
			newbuf := C.malloc(C.size_t(len))
			malloced = true // flag that we have to clone this node before freeing newbuf
			defer C.free(newbuf)
			buf.buf_addr = (*C.char)(newbuf)
			buf.len_alloc = len
			continue
		}
		break
	}
	if status == ydberr.NODEEND {
		return nil
	}
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
	retNode.cnode.len = C.int(retSubs + 1) // +1 because cnode counts the varname as a subscript and ydb_node_next_st() does not
	// if we malloced anything, make sure we take a copy of it before defer runs to free the mallocs on return
	if malloced {
		strings := stringArrayToAnyArray(retNode.Subscripts())
		retNode = n.Conn._Node(strings[0], strings[1:])
	}
	return retNode
}

// TreeNext returns the next node in the traversal of a database tree of a database variable.
// Equivalent to the M function [$QUERY()]. It yields immutable nodes instances.
//   - The next node is chosen in depth-first order (i.e by descending deeper into the subscript tree before moving to the next node at the same level).
//   - The order of returned nodes matches the collation order of the M database.
//   - The node path supplied does not need to exist in the database to find the next match.
//   - Returns nil when there are no more nodes after the given node path within the given database variable (GLVN).
//   - Nodes that have 'null subscripts' (i.e. empty string) are all returned in their place except for the top-level GLVN(""), which is never returned.
//
// See:
//   - [Node.TreePrev]().
//   - [Node.LevelNext]() for traversal of nodes at the same level or to move from one database variable (GLVN) to another.
//
// [$QUERY()]: https://docs.yottadb.com/ProgrammersGuide/functions.html#query
func (n *Node) TreeNext() *Node {
	return n._treeNext(false)
}

// TreePrev is the same as TreeNext but operates in reverse order.
// See [Node.TreeNext]().
func (n *Node) TreePrev() *Node {
	return n._treeNext(true)
}

// Tree returns an interator over all descendants of node for use in a FOR-loop.
// This iterator is a wrapper for [Node.TreeNext](). It yields immutable node instances.
//   - The next node is chosen in depth-first order (i.e by descending deeper into the subscript tree before moving to the next node at the same level).
//   - The order of returned nodes matches the collation order of the M database.
//   - Nodes that have 'null subscripts' (i.e. empty string) are all returned in their place.
//
// See:
//   - [Node.TreeNext](), [Node.TreePrev]()
//   - [Node.Children]() for traversal of only immediate children.
//
// [$ORDER()]: https://docs.yottadb.com/ProgrammersGuide/functions.html#query
func (n *Node) Tree() iter.Seq[*Node] {
	len1 := int(n.cnode.len)
	subs1 := n.Subscripts()

	return func(yield func(*Node) bool) {
		for {
			n = n.TreeNext()
			if n == nil || int(n.cnode.len) < len1 {
				return
			}
			// Ensure that returned node is still a descendent of first node; i.e. all initial subscripts match
			// Don't need to check varname (i=0) as TreeNext() doesn't search beyond varname
			for i := 1; i < len1; i++ {
				buf := bufferIndex(&n.cnode.buffers, int(i))
				// Access buf string using unsafe.String because it doesn't make a time-consuming copy of the string like GoStringN does.
				if unsafe.String((*byte)(unsafe.Pointer(buf.buf_addr)), buf.len_used) != subs1[i] {
					return
				}
			}
			if !yield(n) {
				return
			}
		}
	}
}
