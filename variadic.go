//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2019 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"fmt"
	"unsafe"
)

// #include <stdlib.h>
// #include "libyottadb.h"
// /* Equivalent of gparam_list in callg.h (not available to us) */
// #define MAXVPARMS 36
// typedef struct {
//         intptr_t  n;
//         uintptr_t arg[MAXVPARMS];
// } gparam_list;
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Variadic plist support for C (despite cgo not supporting it).
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// variadicPlist structure is used to anchor the C parameter list used to call callg_nc() via
// ydb_call_variadic_list_func_st(). Because this structure's contents contain pointers to C
// allocated storage, this structure is NOT safe for concurrent access unless those accesses are
// setting different array elements and not affecting the overall structure.
type variadicPlist struct { // Variadic plist support (not exported) needed by LockS() function
	cvplist *C.gparam_list
}

// alloc is a variadicPlist method to allocate the variable plist C structure anchored in variadicPlist.
func (vplist *variadicPlist) alloc() {
	printEntry("variadicPlist.alloc()")
	if nil == vplist {
		panic("*variadicPlist receiver of alloc() cannot be nil")
	}
	if nil != vplist.cvplist {
		// Already allocated
		return
	}
	vplist.cvplist = (*C.gparam_list)(C.malloc(C.size_t(C.sizeof_gparam_list)))
}

// callVariadicPlistFunc is a variadicPlist method to drive a variadic plist function with the given
// plist. The function pointer must be to a C routine as cgo does not allow golang function pointers to
// be passed to C. Note that cgo also prohibits passing pointers to variadic routines so some "stealth"
// to get around that is required - this is done by calling a C routine that creates the needed pointer
// and returns it as an unsafe pointer that can then be used as the parameter for this routine.
func (vplist *variadicPlist) callVariadicPlistFunc(vpfunc unsafe.Pointer) int {
	printEntry("variadicPlist.callVariadicPlistFunc()")
	if nil == vplist {
		panic("*variadicPlist receiver of callVariadicPlistFunc() cannot be nil")
	}
	return int(C.ydb_call_variadic_plist_func((C.ydb_vplist_func)(vpfunc),
		(C.uintptr_t)(uintptr(unsafe.Pointer(vplist.cvplist)))))
}

// free is a variadicPlist method to release the allocated C buffer in this structure.
func (vplist *variadicPlist) free() {
	printEntry("variadicPlist.free()")
	if (nil != vplist) && (nil != vplist.cvplist) {
		C.free(unsafe.Pointer(vplist.cvplist))
		vplist.cvplist = nil
	}
}

// dump is a variadicPlist method to dump a variadic plist block for debugging purposes.
func (vplist *variadicPlist) dump(tptoken uint64) {
	printEntry("variadicPlist.dump()")
	if nil == vplist {
		panic("*variadicPlist receiver of dump() cannot be nil")
	}
	cvplist := vplist.cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		fmt.Printf("YDB: Error fetching STRUCTNOTALLOCD: %s\n", errmsg)
		return
	}
	elemcnt := cvplist.n
	fmt.Printf("   Total of %d (0x%x) elements in this variadic plist\n", elemcnt, elemcnt)
	if C.MAXVPARMS < elemcnt {
		// Reset elemcnt to max we support. Value is probably trash but what the lower loop displays
		// might be interesting
		elemcnt = C.MAXVPARMS
		fmt.Println("     (Element count exceeds max - reset to ", elemcnt)
	}
	argbase := unsafe.Pointer(uintptr(unsafe.Pointer(cvplist)) + unsafe.Sizeof(cvplist))
	for i := 0; i < int(elemcnt); i++ {
		elemptr := (*uintptr)(unsafe.Pointer(uintptr(argbase) + (uintptr(i) * uintptr(unsafe.Sizeof(cvplist)))))
		elemu64 := *elemptr
		fmt.Printf("   Elem %d: %d / 0x%x\n", i, elemu64, elemu64)
	}
}

// setUsed is a variadicPlist method to set the number of used elements in the variadic plist array.
func (vplist *variadicPlist) setUsed(tptoken uint64, errstr *BufferT, newUsed uint32) error {
	printEntry("variadicPlist.setUsed")
	if nil == vplist {
		panic("*variadicPlist receiver of setUsed() cannot be nil")
	}
	cvplist := vplist.cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	(*cvplist).n = C.intptr_t(newUsed)
	return nil
}

// setVPlistParam is a variadicPlist method to set an entry to the variable plist - note any addresses being passed in
// here MUST point to C allocated memory and NOT Golang allocated memory or cgo will cause a panic.
func (vplist *variadicPlist) setVPlistParam(tptoken uint64, errstr *BufferT, paramindx int, paramaddr uintptr) error {
	printEntry("variadicPlist.setVPlistParm")
	if nil == vplist {
		panic("*variadicPlist receiver of setVPlistParam() cannot be nil")
	}
	cvplist := vplist.cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	// Compute address of indexed element
	elemptr := (*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(cvplist)) +
		((uintptr(paramindx) + 1) * uintptr(unsafe.Sizeof(paramaddr)))))
	*elemptr = paramaddr
	return nil
}
