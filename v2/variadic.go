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

// Allow Go to call a variadic C function
// This is used to call YottaDB C API's variadic function ydb_lock_st()

package yottadb

import (
	"fmt"
	"io"
	"runtime"
	"strconv"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// #include <stdlib.h> /* For uint64_t definition on Linux */
// #include "libyottadb.h"
import "C"

const maxARM32RegParms uint32 = 4 // Max number of parms passed in registers in ARM32 (affects passing of 64 bit parms)

// ---- Variadic parameter (vp) support for C (despite CGo not supporting it directly).

// vpCall method calls a variadic function with the parameters previously added.
// The function pointer `vpfunc` must point to the C variadic function to call.
// The instance vplist must have been previously initialized with any parameters using call(s) to vpAddParam*().
func (conn *Conn) vpCall(vpfunc unsafe.Pointer) C.int {
	cconn := conn.cconn
	conn.prepAPI()
	retval := C.ydb_call_variadic_plist_func((C.ydb_vplist_func)(vpfunc), cconn.vplist)
	cconn.vplist.n = 0
	return retval
}

// vpStart must be called before the first vpAddParam call to list parameters for a variadic call.
func (conn *Conn) vpStart() {
	vplist := conn.vpAlloc() // Lazily allocate vplist only if needed
	vplist.n = 0
}

// vpAlloc allocates and returns vplist if it hasn't already been allocated.
func (conn *Conn) vpAlloc() *C.gparam_list {
	cconn := conn.cconn
	// Lazily allocate vplist only if needed
	if cconn.vplist != nil {
		return cconn.vplist
	}
	cconn.vplist = (*C.gparam_list)(calloc(C.sizeof_gparam_list)) // must use our calloc, not malloc: see calloc doc
	// Note this gets freed by conn cleanup
	cconn.vplist.n = -1 // flags an error if the user forgets to call vpStart() before vpAddParam()
	return cconn.vplist
}

// vpAddParam adds another parameter to the variadic parameter list that will be used at the next invocation of conn.vpCall().
// Note that any supplied addresses must point to C allocated memory, not Go allocated memory, or CGo will panic.
func (conn *Conn) vpAddParam(value uintptr) {
	vplist := conn.vpAlloc() // Lazily allocate vplist only if needed
	n := vplist.n
	if n < 0 {
		panic(newError(ydberr.Variadic, "programmer forgot to call vpStart() before vpAddParam()"))
	}
	if n >= C.MAX_GPARAM_LIST_ARGS {
		panic(newError(ydberr.Variadic, fmt.Sprintf("variadic parameter item count %d exceeds maximum count of %d", n+1, C.MAX_GPARAM_LIST_ARGS)))
	}
	// Compute address of indexed element
	elemptr := (*uintptr)(unsafe.Pointer(&vplist.arg[n]))
	*elemptr = value
	vplist.n++
}

// vpAddParam64 adds a specifically 64-bit parameter to the variadic parameter list that will be used at the next invocation of conn.vpCall().
// On 32-bit platforms this will push the 64-bit value in two 32-bit slots in the correct endian order.
// Note that any supplied addresses must point to C allocated memory, not Go allocated memory, or CGo will panic.
func (conn *Conn) vpAddParam64(value uint64) {
	if strconv.IntSize == 64 { // if we're on a 64-bit machine
		conn.vpAddParam(uintptr(value))
		return
	}
	vplist := conn.vpAlloc() // Lazily allocate vplist only if needed
	if isLittleEndian() {
		if runtime.GOARCH == "arm" {
			// If this is 32 bit ARM, there is a rule about the first 4 parms that go into registers. If
			// there is a mix of 32 bit and 64 bit parameters, the 64 bit parm must always go into an
			// even/odd pair of registers. If the next index is odd (meaning we could be loading into an
			// odd/even pair, then that register is skipped and left unused. This only applies to parms
			// loaded into registers and not to parms pushed on the stack.
			const maxARM32RegParms = 4 // Max number of parms passed in registers in ARM32
			if vplist.n&1 == 1 && int(vplist.n) < maxARM32RegParms {
				conn.vpAddParam(0) // skip odd-indexed spots
			}
		}
		conn.vpAddParam(uintptr(value & 0xffffffff))
		conn.vpAddParam(uintptr(value >> 32))
	} else {
		conn.vpAddParam(uintptr(value >> 32))
		conn.vpAddParam(uintptr(value & 0xffffffff))
	}
}

// vpDump dumps a variadic parameter list for debugging/test purposes.
func (conn *Conn) vpDump(w io.Writer) {
	vplist := conn.cconn.vplist
	if vplist == nil {
		panic(newError(ydberr.Variadic, "could not dump nil vararg list"))
	}
	n := int(vplist.n)
	argbase := unsafe.Add(unsafe.Pointer(vplist), unsafe.Sizeof(vplist))
	fmt.Fprintf(w, "   Total of %d elements in this variadic plist\n", n)
	for i := range n {
		elemptr := unsafe.Add(argbase, i*int(unsafe.Sizeof(vplist)))
		fmt.Fprintf(w, "   Elem %d (%p) Value: %d (0x%x)\n", i, elemptr, *((*uintptr)(elemptr)), *((*uintptr)(elemptr)))
	}
}

// isLittleEndian is a function to determine endianness.
func isLittleEndian() bool {
	var bittest = 1
	return *(*byte)(unsafe.Pointer(&bittest)) == 1
}
