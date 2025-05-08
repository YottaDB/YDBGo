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
)

// #include <stdlib.h> /* For uint64_t definition on Linux */
// #include "libyottadb.h"
import "C"

const maxARM32RegParms uint32 = 4 // Max number of parms passed in registers in ARM32 (affects passing of 64 bit parms)

// ---- Variadic parameter (vp) support for C (despite CGo not supporting it directly).

// vpcall method calls a variadic function with the parameters previously added.
// The function pointer `vpfunc` must point to the C variadic function to call.
// The instance vplist must have been previously initialized with any parameters using call(s) to vpaddParam*().
func (conn *Conn) vpcall(vpfunc unsafe.Pointer) C.int {
	cconn := conn.cconn
	retval := C.ydb_call_variadic_plist_func((C.ydb_vplist_func)(vpfunc), cconn.vplist)
	cconn.vplist.n = 0
	return retval
}

// vpalloc allocates and returns vplist if it hasn't already been allocated.
func (conn *Conn) vpalloc() *C.gparam_list {
	cconn := conn.cconn
	// Lazily allocate vplist only if needed
	if cconn.vplist != nil {
		return cconn.vplist
	}
	// This initial call must be to calloc() to get initialized (cleared) storage: due to a documented cgo bug
	// we must not let Go store pointer values in uninitialized C-allocated memory or errors may result.
	// See the cgo bug mentioned at https://golang.org/cmd/cgo/#hdr-Passing_pointers.
	cconn.vplist = (*C.gparam_list)(C.calloc(1, C.size_t(C.sizeof_gparam_list)))
	if cconn.vplist == nil {
		panic("YDBGo: out of memory when allocating storage for variadac parameter list")
	}
	// Note this gets freed by conn cleanup
	cconn.vplist.n = 0 // initialize to 0 now and at the end of every vpcall()
	return cconn.vplist
}

// vpaddParam adds another parameter to the variadic parameter list that will be used at the next invocation of conn.vpcall().
// Note that any supplied addresses must point to C allocated memory, not Go allocated memory, or CGo will panic.
func (conn *Conn) vpaddParam(value uintptr) {
	vplist := conn.vpalloc() // Lazily allocate vplist only if needed
	n := vplist.n
	if n >= C.MAX_GPARAM_LIST_ARGS {
		panic(fmt.Sprintf("YDBGo: variadic parameter item count %d exceeds maximum count of %d", n+1, C.MAX_GPARAM_LIST_ARGS))
	}
	// Compute address of indexed element
	elemptr := (*uintptr)(unsafe.Pointer(&vplist.arg[n]))
	*elemptr = value
	vplist.n++
}

// vpaddParam64 adds a specifically 64-bit parameter to the variadic parameter list that will be used at the next invocation of conn.vpcall().
// On 32-bit platforms this will push the 64-bit value in two 32-bit slots in the correct endian order.
// Note that any supplied addresses must point to C allocated memory, not Go allocated memory, or CGo will panic.
func (conn *Conn) vpaddParam64(value uint64) {
	if strconv.IntSize == 64 { // if we're on a 64-bit machine
		conn.vpaddParam(uintptr(value))
		return
	}
	vplist := conn.vpalloc() // Lazily allocate vplist only if needed
	if isLittleEndian() {
		if runtime.GOARCH == "arm" {
			// If this is 32 bit ARM, there is a rule about the first 4 parms that go into registers. If
			// there is a mix of 32 bit and 64 bit parameters, the 64 bit parm must always go into an
			// even/odd pair of registers. If the next index is odd (meaning we could be loading into an
			// odd/even pair, then that register is skipped and left unused. This only applies to parms
			// loaded into registers and not to parms pushed on the stack.
			const maxARM32RegParms = 4 // Max number of parms passed in registers in ARM32
			if vplist.n&1 == 1 && int(vplist.n) < maxARM32RegParms {
				conn.vpaddParam(0) // skip odd-indexed spots
			}
		}
		conn.vpaddParam(uintptr(value & 0xffffffff))
		conn.vpaddParam(uintptr(value >> 32))
	} else {
		conn.vpaddParam(uintptr(value >> 32))
		conn.vpaddParam(uintptr(value & 0xffffffff))
	}
}

// vpdump dumps a variadic parameter list for debugging/test purposes.
func (conn *Conn) vpdump(w io.Writer) {
	vplist := conn.cconn.vplist
	if vplist == nil {
		panic("YDBGo: could not dump nil vararg list")
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
