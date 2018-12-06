//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.	//
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
// #include "libydberrors.h"
// /* Equivalent of gparam_list in callg.h (not available to us) */
// #define MAXVPARMS 36
// typedef struct {
//         intptr_t  n;
//         uintptr_t arg[MAXVPARMS];
// } gparam_list;
import "C"

// variadicPlist structure is used to anchor the C parameter list used to call callg_nc() via
// ydb_call_variadic_list_func_st().
type variadicPlist struct { // Variadic plist support (not exported) needed by LockS() function
	cvplist *C.gparam_list
}

// alloc is a variadicPlist method to allocate the variable plist C structure anchored in variadicPlist
func (vplist *variadicPlist) alloc() {
	printEntry("variadicPlist.alloc()")
	if nil != (*vplist).cvplist {
		// Already allocated
		return
	}
	(*vplist).cvplist = (*C.gparam_list)(C.malloc(C.size_t(C.sizeof_gparam_list)))
}

// callVariadicPlistFuncSt is a variadicPlist method to drive a variadic plist function with the given
// plist. The function pointer must be to a C routine as cgo does not allow golang function pointers to
// be passed to C.
func (vplist *variadicPlist) callVariadicPlistFuncST(tptoken uint64, vpfunc unsafe.Pointer) int {
	printEntry("variadicPlist.callVariadicPlistFuncST()")
	return int(C.ydb_call_variadic_plist_func_st(C.uint64_t(tptoken), (C.ydb_vplist_func)(vpfunc),
		(C.uintptr_t)(uintptr(unsafe.Pointer((*vplist).cvplist)))))
}

// free is a variadicPlist method to release the allocated C buffer in this structure.
func (vplist *variadicPlist) free() {
	printEntry("variadicPlist.free()")
	if nil != (*vplist).cvplist {
		C.free(unsafe.Pointer((*vplist).cvplist))
		(*vplist).cvplist = nil
	}
}

// dump is a variadicPlist method to dump a variadic plist block for debugging purposes
func (vplist *variadicPlist) dump(tptoken uint64) {
	printEntry("variadicPlist.dump()")
	cvplist := (*vplist).cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		fmt.Printf("YDB: Error fetching STRUCTNOTALLOCD: %s\n", errmsg)
		return
	}
	elemcnt := (*cvplist).n
	fmt.Printf("   Total of %d (%x) elements in this variadic plist\n", elemcnt, elemcnt)
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
		fmt.Printf("   Elem %d: %d / %x\n", i, elemu64, elemu64)
	}
}

// setUsed is a variadicPlist method to set the number of used elements in the variadic plist array
func (vplist *variadicPlist) setUsed(tptoken uint64, newUsed uint32) error {
	printEntry("variadicPlist.setUsed")
	cvplist := (*vplist).cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	(*cvplist).n = C.intptr_t(newUsed)
	return nil
}

// setVPlistParam is a varidicPlist method to set an entry to the variable plist - note the addresses we
// add here MUST point to C allocated memory and NOT Golang allocated memory or a crash will result.
func (vplist *variadicPlist) setVPlistParam(tptoken uint64, paramindx int, paramaddr uintptr) error {
	printEntry("variadicPlist.setVPlistParm")
	cvplist := (*vplist).cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
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