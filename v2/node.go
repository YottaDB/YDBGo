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
	cnode    *C.node
	Conn     *Conn // Node.Conn points to the Go conn; Node.cnode.conn will point directly to the C.conn
	original *Node // if this node is mutable, points to the originating node
	cap      int   // capacity of buffers stored in mutation[0] node which may be >cnode.len; used only by Index() for mutation0
	// List of mutated children of this node: one for each subscript depth indexed, or nil if no mutations exist yet
	mutations [](*Node)
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
	var firstLen int   // length of concatenated subscripts in varname
	var firstCount int // number of subscripts in varname
	var node1 *Node    // if varname is a node, store it in here
	subs := make([]string, len(subscripts))
	switch val := varname.(type) {
	case *Node:
		node1 = val
		cnode := node1.cnode
		firstCount = int(cnode.len)
		if node1.IsMutable() {
			// if mutable, there may be gaps between strings due to overallocation, so remove gaps by concatenating each string individually
			for i := range int(cnode.len) {
				buffer := bufferIndex(cnode.buffers, i)
				// unsafe.String gives fast temporary access to C data as if it were a string without copying the data to a string first
				joiner.WriteString(unsafe.String((*byte)(unsafe.Pointer(buffer.buf_addr)), buffer.len_used))
			}
			firstLen = joiner.Len()
		} else {
			// if immutable, we can copy all subscripts in a single lump as there are no gaps between strings
			firstStringAddr := cnode.buffers.buf_addr
			lastbuf := bufferIndex(cnode.buffers, firstCount-1)
			// Calculate data size of all strings in node to copy: address of last string - address of first string + length of last string
			datasize := C.uint(uintptr(unsafe.Pointer(lastbuf.buf_addr))) - C.uint(uintptr(unsafe.Pointer(firstStringAddr)))
			datasize += lastbuf.len_used
			// unsafe.String gives fast temporary access to C data as if it were a string without copying the data to a string first
			joiner.WriteString(unsafe.String((*byte)(unsafe.Pointer(firstStringAddr)), datasize))
			firstLen = int(datasize)
		}
	default:
		first := anyToString(val)
		joiner.WriteString(first)
		firstLen = len(first)
		firstCount = 1
	}
	for i, s := range subscripts {
		subs[i] = anyToString(s)
		joiner.WriteString(subs[i])
	}

	size := C.sizeof_node + C.sizeof_ydb_buffer_t*(firstCount+len(subscripts)) + joiner.Len()
	n = &Node{}
	cnode := (*C.node)(calloc(C.size_t(size))) // must use our calloc, not malloc: see calloc doc
	n.cnode = cnode
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(cnode *C.node) {
		C.free(unsafe.Pointer(cnode))
	}, cnode)

	n.Conn = conn
	cnode.conn = conn.cconn // point to the C version of the conn
	cnode.len = C.int(len(subscripts) + firstCount)
	cnode.buffers = (*C.ydb_buffer_t)(unsafe.Add(unsafe.Pointer(cnode), C.sizeof_node))

	dataptr := unsafe.Pointer(bufferIndex(cnode.buffers, len(subscripts)+firstCount))
	if joiner.Len() > 0 {
		// Note: have tried to replace the following with copy() to avoid a CGo invocation, but it's slower
		C.memcpy(dataptr, unsafe.Pointer(&joiner.Bytes()[0]), C.size_t(joiner.Len()))
	}

	// Function to set each buffer to string[dataptr++] as I loop through strings
	setbuf := func(buf *C.ydb_buffer_t, length C.uint) {
		buf.buf_addr = (*C.char)(dataptr)
		buf.len_used, buf.len_alloc = length, length
		dataptr = unsafe.Add(dataptr, length)
	}

	// Now fill in ydb_buffer_t pointers
	if node1 != nil {
		// First set buffers for all strings copied from parent node
		for i := range firstCount {
			buf := bufferIndex(cnode.buffers, i)
			setbuf(buf, bufferIndex(node1.cnode.buffers, i).len_used)
		}
		runtime.KeepAlive(node1) // ensure node1 sticks around until we've finished copying data from it's C allocation
	} else {
		buf := bufferIndex(cnode.buffers, 0)
		setbuf(buf, C.uint(firstLen))
	}
	for i, s := range subs {
		buf := bufferIndex(cnode.buffers, i+firstCount)
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
	if n.IsMutable() {
		panic(errorf(ydberr.InvalidMutableOperation, "mutable Node (%s) must not be cloned", n))
	}
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

// _subscript is an implementation of Subscript() but without the bounds check or negative index access (for internal use by Index, to fetch cached depth strings)
func (n *Node) _subscript(index int) string {
	cnode := n.cnode // access C.node from Go node
	buf := bufferIndex(cnode.buffers, index)
	r := C.GoStringN(buf.buf_addr, C.int(buf.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return r
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
	return n._subscript(index)
}

// Subscripts returns a slice of strings that represent the varname and subscript names of the given node.
func (n *Node) Subscripts() []string {
	cnode := n.cnode // access C.node from Go node
	strings := make([]string, cnode.len)
	for i := range cnode.len {
		buf := bufferIndex(cnode.buffers, int(i))
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
// For example output, see [Node.GoString]. Output lines are formatted as follows:
//   - Output subscripts and values as unquoted numbers if they convert to float64 and back without change (using [Node.Quote]).
//   - Output strings in YottaDB ZWRITE format.
//   - Every line output ends with "\n", including the final line.
//   - If the receiver is nil, output is "<nil>\n"
//   - If node has no value and no children, outputs the empty string.
//
// See [Node.GoString] for an example of the output format.
// Two optional integers may be supplied to specify maximums where both default to -1:
//   - first parameter specifies the maximum number of lines to output (not including a file "...\n" line indicating truncation). A maximum of -1 means infinite.
//     If lines are truncated, an additional line "...\n" is added so that the output ends with "\n...\n". A maximum of 0 lines is treated as 1.
//   - second parameter specifies the maximum number of characters at which to truncate values prior to output where -1 means infinite.
//     Truncated values are output with suffix "..." after any ending quotes. Note that conversion to ZWRITE format may expand this.
func (n *Node) Dump(args ...int) string {
	if len(args) > 2 {
		panic(errorf(ydberr.TooManyParameters, "%d parameters supplied to Dump() which only takes 2", len(args)))
	}
	args = append(args, -1, -1) // defaults
	maxLines, maxString := args[0], args[1]
	if maxLines == 0 {
		maxLines = 1 // This ensures ending is always "\n...\n" whenever lines are truncated
	}
	if n == nil {
		return "<nil>\n"
	}

	var bld strings.Builder
	// local func to output one line of the tree
	dumpLine := func(node *Node, val string) {
		bld.WriteString(node.String())
		bld.WriteString("=")
		if maxString != -1 && len(val) > maxString {
			bld.WriteString(node.Conn.Quote(val[:maxString]))
			bld.WriteString("...")
		} else {
			bld.WriteString(node.Conn.Quote(val))
		}
		bld.WriteString("\n")
	}

	lines := 0
	val, ok := n.Lookup()
	if ok {
		lines++
		dumpLine(n, val)
	}
	for node := range n.Tree() {
		val, ok = node.Lookup()
		if !ok {
			// Node subscript was deleted while iterating it, so don't print that subscript
			continue
		}
		lines++
		if maxLines != -1 && lines > maxLines {
			bld.WriteString("...\n")
			break
		}
		dumpLine(node, val)
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
	status := C.ydb_set_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &cconn.value)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
}

// Get fetches and returns the value of a database node or defaultValue[0] if the database node is empty.
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
func (n *Node) GetInt(defaultValue ...int) int {
	val := n.Get()
	num, err := strconv.ParseInt(val, 10, 0)
	if err != nil {
		if _, ok := err.(*strconv.NumError); !ok {
			panic(err) // unknown error unrelated to conversion
		}
		if len(defaultValue) == 0 {
			return 0
		}
		return defaultValue[0]
	}
	return int(num)
}

// GetFloat fetches and returns the value of a database node as a float64.
// Return zero if the node's value does not exist or is not convertable to float64.
func (n *Node) GetFloat(defaultValue ...float64) float64 {
	val := n.Get()
	num, err := strconv.ParseFloat(val, 64)
	if err != nil {
		if _, ok := err.(*strconv.NumError); !ok {
			panic(err) // unknown error unrelated to conversion
		}
		if len(defaultValue) == 0 {
			return 0
		}
		return defaultValue[0]
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
//   - bool false is returned on errors GVUNDEF (undefined M global) or LVUNDEF (undefined M local). Other errors panic.
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
	status := C.ydb_get_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &cconn.value)
	if status == ydberr.INVSTRLEN {
		// Allocate more space and retry the call
		n.Conn.ensureValueSize(int(cconn.value.len_used))
		n.Conn.prepAPI()
		status = C.ydb_get_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &cconn.value)
	}
	if status == ydberr.GVUNDEF || status == ydberr.LVUNDEF {
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
	status := C.ydb_data_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &val)
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
	status := C.ydb_delete_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), C.YDB_DEL_TREE)
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
	status := C.ydb_delete_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), C.YDB_DEL_NODE)
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
	status := C.ydb_incr_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &cconn.value, &cconn.value)
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}

	valuestring := C.GoStringN(cconn.value.buf_addr, C.int(cconn.value.len_used))
	runtime.KeepAlive(n) // ensure n sticks around until we've finished copying data from it's C allocation
	return valuestring
}

// Lock attempts to acquire or increment the count of a lock matching this node, waiting up to timeout for availability.
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
		status := C.ydb_lock_incr_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, timeoutNsec, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1))
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
	status := C.ydb_lock_decr_st(C.uint64_t(n.Conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1))
	if status != YDB_OK {
		panic(n.Conn.lastError(status))
	}
}

// stubNode creates a minimal mutable node that points to the buffers of another node.
// Used by Index().
func (conn *Conn) stubNode(original *Node, buffers *C.ydb_buffer_t, len int) (n *Node) {
	size := C.sizeof_node
	cnode := (*C.node)(calloc(C.size_t(size))) // must use our calloc, not malloc: see calloc doc
	cnode.conn = conn.cconn
	cnode.len = C.int(len)
	cnode.buffers = buffers
	n = &Node{}
	n.cnode = cnode
	n.Conn = conn
	n.original = original
	// Queue the cleanup function to free it
	runtime.AddCleanup(n, func(cnode *C.node) {
		C.free(unsafe.Pointer(cnode))
	}, cnode)
	return n
}

// This subscript string is used to preallocate subscript string space in mutable nodes that may have their final subscript name changed.
// When it is exceeded by subsequent Node.Index() iterations, the entire node must be cloned to achieve reallocation.
var preallocSubscript string = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
var preallocMutationDepth int = 5 // May expand on-the-fly up to YDB_MAX_SUBS

// Index allows fast temporary access to subnodes referenced by the given subscripts.
// It indexes a node object with the given subscripts and returns a mutable node object that will change next time Index is invoked
// on the same parent node object (which is thread-safe because each Goroutine has separate node objects).
// Fast access is achieved by removing the overhead of creating a new node object every access, e.g. each time around a loop.
// For example, [Node.Children] yields mutable nodes created by Index().
// [Node.Child] should be used instead, for non-temporary access, for example, where nodes will be passed to subroutines.
//   - Returns a mutable child of the given node with the given subscripts appended (see [Node.IsMutable] for mutability details).
//
// Usage is similar to [Node.Child] except that it typically runs about 4 times as fast and returns mutable instead of immutable nodes.
func (n *Node) Index(subscripts ...any) *Node {
	// This works by allocating a new mutable node (stored in Node.mutations[0]) and several stub nodes that use its buffers.
	// Stub nodes are stored in Node.mutations[1..n] for each depth of indexed subscripts (cf. 1. below).
	// The first node in Node.mutations[0] is a full node. Subsequent mutations point back to its subscript strings (cf. 2. below).
	//
	// 1. The reason a mutation/stub is required for each depth is illustrated by the programmer doing:
	//   for person := range n.Children() {
	//     name := person.Index("name").Get()
	//     age := person.Index("age").GetInt()
	//   }
	// Note that person is already a mutable node yielded by Children().
	// Without separate nodes for separate depths, the first Index() will append "name" to the mutable node person.
	// The second Index() will thus produce person("name","age") instead of person("age").
	//
	// 2. The reason subs in Node.mutations[1..n] subscript strings point back to mutations[0] is to prevent a bug if they had their own strings.
	// Using the same example above, in the second iteration of the FOR loop person.Index("name") will use
	// the mutable node for depth 2 which will still have its person subscript set to its previous value because it
	// was only incremented in the depth 1 mutable node.

	// Speed up the common case where only one subscript is supplied.
	// (faster partly because there's no need to create newsubs array for coverted strings)
	if len(subscripts) == 1 {
		return n.index1(subscripts[0])
	}
	if len(subscripts) == 0 {
		panic(errorf(ydberr.SubscriptRequired, "Index() method requires at least one subscript as a parameter"))
	}

	// Cast new subscripts to strings
	newsubs := make([]string, len(subscripts))
	for i, sub := range subscripts {
		newsubs[i] = anyToString(sub)
	}

	// Calculate depth of index from original parent node
	original := n // originating parent node from which index was taken
	if n.IsMutable() {
		original = n.original
	}
	originalLen := int(original.cnode.len)
	var depth int // indexing depth: number of subscripts the mutation adds to the *original* node
	depth = int(n.cnode.len) - originalLen + len(newsubs)
	if originalLen-1+depth > YDB_MAX_SUBS { // -1 for varname
		panic(errorf(ydberr.InvalidSubscriptIndex, "attempt to Index() node %s exceeded YDB_MAX_SUBS", n))
	}

	// Check whether an existing mutation[0] node has space for these new subscripts -- or we need to reallocate
	nLen := int(n.cnode.len)
	if original.mutations == nil || original.mutations[0] == nil || original.mutations[0].cap < nLen+len(newsubs) {
		return n.reallocateMutation(newsubs)
	}
	buffers := original.mutations[0].cnode.buffers
	for i, sub := range newsubs {
		space := int(bufferIndex(buffers, nLen+i).len_alloc)
		if space < len(sub) {
			return n.reallocateMutation(newsubs)
		}
	}
	// No need to reallocate, so just store the new subscripts into the mutated node
	for i, sub := range newsubs {
		buffer := bufferIndex(buffers, nLen+i)
		C.memcpy(unsafe.Pointer(buffer.buf_addr), unsafe.Pointer(unsafe.StringData(sub)), C.size_t(len(sub)))
		buffer.len_used = C.uint(len(sub))
	}
	return original.mutations[depth-1]
}

// index1 is the same as Index except that it operates faster in the common case when only a single parameter is given.
// It is automatically invoked by Index() when that case is true.
func (n *Node) index1(subscript any) *Node {
	// Cast new subscript to string
	sub := anyToString(subscript)

	// Calculate depth of index from original parent node
	var depth int // indexing depth: number of subscripts the mutation adds to the *original* node
	original := n // originating parent node from which index was taken
	if n.IsMutable() {
		original = n.original
	}
	originalLen := int(original.cnode.len)
	depth = int(n.cnode.len) - originalLen + 1
	if originalLen-1+depth > YDB_MAX_SUBS { // -1 for varname
		panic(errorf(ydberr.InvalidSubscriptIndex, "attempt to Index() node %s exceeded YDB_MAX_SUBS", n))
	}

	// Check whether an existing mutation[0] node has space for this new subscript -- or we need to reallocate
	nLen := int(n.cnode.len)
	if original.mutations == nil || original.mutations[0] == nil || original.mutations[0].cap < nLen+1 {
		return n.reallocateMutation([]string{sub})
	}
	buffers := original.mutations[0].cnode.buffers
	space := int(bufferIndex(buffers, nLen).len_alloc)
	if space < len(sub) {
		return n.reallocateMutation([]string{sub})
	}
	// No need to reallocate, so just store the new subscript into the mutated node
	buffer := bufferIndex(buffers, nLen)
	C.memcpy(unsafe.Pointer(buffer.buf_addr), unsafe.Pointer(unsafe.StringData(sub)), C.size_t(len(sub)))
	buffer.len_used = C.uint(len(sub))
	return original.mutations[depth-1]
}

// reallocateMutation reallocates mutation0 when necessary for use by Index().
// This is only called after determining that reallocation really is necessary
func (n *Node) reallocateMutation(newsubs []string) *Node {
	// See explanation of how this works in the comment at the beginning of Index()
	original := n // originating parent node from which index was taken
	if n.IsMutable() {
		original = n.original
	}
	originalLen := int(original.cnode.len)
	var depth int // indexing depth: number of subscripts the mutation adds to the *original* node
	depth = int(n.cnode.len) - originalLen + len(newsubs)
	if original.mutations == nil {
		original.mutations = make([]*Node, 1, preallocMutationDepth)
	}
	mutation0 := original.mutations[0]
	// calculate max # subscripts = nLen + any more used previously on this mutable node
	maxDepth := depth
	if mutation0 != nil {
		maxDepth = max(maxDepth, mutation0.cap-originalLen)
	}
	subs := make([]any, 0, maxDepth)
	// Add indexes that n has before newsubs
	for i := originalLen; i < int(n.cnode.len); i++ {
		// append preallocSubscript to each allocated subscript so that we don't have to reallocate small increases
		subs = append(subs, n.Subscript(i)+preallocSubscript)
	}
	// Add new subscripts
	for _, sub := range newsubs {
		// append preallocSubscript to each allocated subscript so that we don't have to reallocate small increases
		subs = append(subs, sub+preallocSubscript)
	}
	// Add any surplus indexes above newsubs that were previously used on mutation0 even though they are not needed for this particular indexing operation
	// i.e. never shrink mutation0.cap
	for len(subs) < maxDepth {
		subs = append(subs, mutation0._subscript(originalLen+len(subs)))
	}
	mutation0 = n.Conn._Node(original, subs)
	mutation0.original = original                // indicate new node is mutable
	mutation0.cap = int(mutation0.cnode.len)     // actual buffer capacity of mutation0
	mutation0.cnode.len = C.int(originalLen + 1) // shrink mutation[0] node to present just one index subscript (its index depth=1)
	buffers := mutation0.cnode.buffers
	// remove preallocSubscript over-allocation from the end of each subscript
	for i := range depth {
		bufferIndex(buffers, originalLen+i).len_used -= C.uint(len(preallocSubscript))
	}
	original.mutations[0] = mutation0 // store mutation0
	// Re-point all stub buffers to the new mutation0 buffers
	for i := 1; i < len(original.mutations); i++ {
		original.mutations[i].cnode.buffers = buffers
	}
	// Create any stubs needed up to depth
	for i := len(original.mutations); i < depth; i++ {
		stub := n.Conn.stubNode(original, buffers, originalLen+i+1)
		original.mutations = append(original.mutations, stub)
	}
	return original.mutations[depth-1]
}

// IsMutable returns whether given node is mutable.
// Mutable nodes may have their subscripts changed each loop iteration or each call to Node.Index(). This means that
// the same mutable node object may reference different database nodes and will not always point to the same one.
//   - Mutable nodes are returned by [Node.Index], [Node.Next], [Node.Prev] and iterator [Node.Children].
//   - All standard node methods operate on a mutable node except conn.CloneNode().
//   - If an immutable copy of a mutable node is required, use [Node.Clone] or [Node.Child].
//   - If you need to take an immutable snapshot of a mutable node this may be done with [Node.Clone] or [Node.Child].
//
// A mutable node object is like a regular node object except that it will change to point to a different database
// node each time [Node.Index] is invoked on its originating node object n, so if you store a reference to it,
// that reference will no longer point to the same database node as it originally did. For example, the following
// code will print the most recent vehicle, not the heaviest vehicle as intended, because [Node.Children]() yields
// mutable nodes.
//
//	n := conn.Node("vehicles")
//	var heaviest *yottadb.Node
//	var maxWeight float64 = 0
//	for vehicle := range n.Children() {
//	  if vehicle.Index("weight").GetFloat() > maxWeight {
//	    heaviest = vehicle
//	    maxWeight = vehicle.Index("weight")
//	  }
//	}
//	fmt.Print(heaviest.Dump())
func (n *Node) IsMutable() bool {
	return n.original != nil
}

// _next returns the name of the next subscript at the same depth level as the given node.
// This implements the logic for both Next() and Prev().
//   - If the parameter reverse is true, fetch the next node in reverse order, i.e. Prev().
//   - bool returns with false only if there are no more subscripts
//
// See further documentation at Next().
func (n *Node) _next(reverse bool) (string, bool) {
	cnode := n.cnode // access C equivalents of Go types
	conn := n.Conn
	cconn := cnode.conn

	var status C.int
	for range 2 {
		n.Conn.prepAPI()
		if reverse {
			status = C.ydb_subscript_previous_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &cconn.value)
		} else {
			status = C.ydb_subscript_next_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &cconn.value)
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

// _Next implements both Next and Prev based on the reverse parameter.
// It returns a mutable Node object pointing to the next subscript (unlike _next() which returns the next subscript as a string).
func (n *Node) _Next(reverse bool) *Node {
	next, ok := n._next(reverse)
	if !ok {
		return nil
	}
	if n.cnode.len <= 1 {
		// Cannot index a node with no varname, so just return a new top-level immutable node with next as subscript
		return n.Conn.Node(next)
	}
	var parent *Node
	if n.IsMutable() {
		original := n.original // get the original immutable parent
		depth := int(n.cnode.len - original.cnode.len)
		// find parent node or parent index so I can index that
		if depth < 2 {
			parent = original
		} else {
			parent = original.mutations[(depth-1)-1]
		}
	} else {
		// If there is no mutable parent index, create a parent node with one less subscript than `n`,
		// then use that to index for fast iteration
		subs := n.Subscripts()
		parent = n.Conn._Node(subs[0], stringArrayToAnyArray(subs[1:max(1, len(subs)-1)]))
	}
	return parent.Index(next)
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
	return n._Next(false)
}

// Prev is the same as Next but operates in the reverse order.
// See [Node.Next]()
func (n *Node) Prev() *Node {
	return n._Next(true)
}

// _children returns an interator that can FOR-loop through all a node's single-depth child subscripts.
// This implements the logic for both Children() and ChildrenBackward().
//   - If the parameter reverse is true, iterate nodes in reverse order.
//
// See further documentation at Iterate().
func (n *Node) _children(reverse bool) iter.Seq2[*Node, string] {
	// Ensure our mutable access to this node doesn't clobber the caller's use of Index() on this node.
	// In theory the caller should know that it might since we've documented that Children() uses Index(),
	// but better to be safe even though it means creation of one extra node every FOR loop
	first := n.Clone()
	n = first.Index("")
	return func(yield func(*Node, string) bool) {
		for {
			next, ok := n._next(reverse)
			if !ok {
				return
			}
			n = first.Index(next)
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
	conn := n.Conn
	cconn := cnode.conn

	// Create new node to store result with a single preallocated child as an initial guess of space needed.
	retNode := n.Child(preallocSubscript)
	var retSubs C.int
	var malloced bool // whether we had to malloc() and hence defer free()
	var status C.int
	for {
		retSubs = retNode.cnode.len - 1 // -1 because cnode counts the varname as a subscript and ydb_node_next_st() does not
		n.Conn.prepAPI()
		if reverse {
			status = C.ydb_node_previous_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &retSubs, bufferIndex(retNode.cnode.buffers, 1))
		} else {
			status = C.ydb_node_next_st(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cnode.buffers, cnode.len-1, bufferIndex(cnode.buffers, 1), &retSubs, bufferIndex(retNode.cnode.buffers, 1))
		}
		if status == ydberr.INSUFFSUBS {
			extraStrings := make([]any, retSubs-(retNode.cnode.len-1))
			// Pre-fill node subscripts
			for i := range extraStrings {
				extraStrings[i] = preallocSubscript
			}
			retNode = retNode.Child(extraStrings...)
			continue
		}
		if status == ydberr.INVSTRLEN {
			buf := bufferIndex(retNode.cnode.buffers, int(retSubs+1)) // +1 because cnode counts the varname as a subscript and ydb_node_next_st() does not
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
// Equivalent to the M function [$QUERY()].
//   - It yields immutable nodes.
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
				buf := bufferIndex(n.cnode.buffers, int(i))
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
