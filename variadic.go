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
	"runtime"
	"strconv"
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

const maxARM32RegParms uint32 = 4 // Max number of parms passed in registers in ARM32 (affects passing of 64 bit parms)

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
		errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		fmt.Printf("YDB: Error fetching STRUCTNOTALLOCD: %s\n", errmsg)
		return
	}
	elemcnt := cvplist.n
	fmt.Printf("   Total of %d (0x%x) elements in this variadic plist\n", elemcnt, elemcnt)
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
		errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	if C.MAXVPARMS <= newUsed {
		panic(fmt.Sprintf("YDB: setUsed item count %d exceeds maximum count of %d", newUsed, C.MAXVPARMS))
	}
	(*cvplist).n = C.intptr_t(newUsed)
	return nil
}

// setVPlistParam is a variadicPlist method to set an entry to the variable plist - note any addresses being passed in
// here MUST point to C allocated memory and NOT Golang allocated memory or cgo will cause a panic. Note parameter
// indexes are 0 based.
func (vplist *variadicPlist) setVPlistParam(tptoken uint64, errstr *BufferT, paramindx uint32, paramaddr uintptr) error {
	printEntry("variadicPlist.setVPlistParm")
	if nil == vplist {
		panic("*variadicPlist receiver of setVPlistParam() cannot be nil")
	}
	cvplist := vplist.cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	if C.MAXVPARMS <= paramindx {
		panic(fmt.Sprintf("YDB: setVPlistParam item count %d exceeds maximum count of %d", paramindx, C.MAXVPARMS))
	}
	// Compute address of indexed element
	elemptr := (*uintptr)(unsafe.Pointer(uintptr(unsafe.Pointer(&cvplist.arg[0])) +
		uintptr(paramindx*uint32(unsafe.Sizeof(paramaddr)))))
	*elemptr = paramaddr
	return nil
}

// setVPlistParam64Bit pushes the supplied 64 bit parameter onto the supplied variadicPlist at the supplied index. This is for
// parms that are ALWAYS 64 bits regardless of platform address mode. This is interesting only when using a 32 bit address
// mode which means the 64 bit parm must span 2 slots. In that case, the index is bumped by 2. Note that paramindx is incremented
// by this function since the caller does not know whether to bump it once or twice.
func (vplist *variadicPlist) setVPlistParam64Bit(tptoken uint64, errstr *BufferT, paramindx *uint32, parm64bit uint64) error {
	var err error

	printEntry("variadicPlist.setVPlistParm64Bit")
	if nil == vplist {
		panic("*variadicPlist receiver of setVPlistParam64Bit() cannot be nil")
	}
	cvplist := vplist.cvplist
	if nil == cvplist {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	if C.MAXVPARMS <= *paramindx {
		panic(fmt.Sprintf("YDB: setVPlistParam64Bit item count %d exceeds maximum count of %d", paramindx, C.MAXVPARMS))
	}
	// Compute address of indexed element(s)
	if 64 == strconv.IntSize {
		err = vplist.setVPlistParam(tptoken, errstr, *paramindx, uintptr(parm64bit))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
		}
	} else {
		// Have 32 bit addressing - 64 bit parm needs to take 2 slots (in the correct order).
		if IsLittleEndian() {
			if "arm" == runtime.GOARCH {
				// If this is 32 bit ARM, there is a rule about the first 4 parms that go into registers. If
				// there is a mix of 32 bit and 64 bit parameters, the 64 bit parm must always go into an
				// even/odd pair of registers. If the next index is odd (meaning we could be loading into an
				// odd/even pair, then that register is skipped and left unused. This only applies to parms
				// loaded into registers and not to parms pushed on the stack.
				if (1 == (*paramindx & 0x1)) && (maxARM32RegParms > *paramindx) {
					// Our index is odd so skip a spot leaving garbage contents in that slot rather than
					// take the time to clear them.
					(*paramindx)++
				}
				if C.MAXVPARMS <= *paramindx {
					panic(fmt.Sprintf("YDB: setVPlistParam64Bit item count %d exceeds maximum count of %d",
						paramindx, C.MAXVPARMS))
				}
			}
			err = vplist.setVPlistParam(tptoken, errstr, *paramindx, uintptr(parm64bit&0xffffffff))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			(*paramindx)++
			if C.MAXVPARMS <= *paramindx {
				panic(fmt.Sprintf("YDB: setVPlistParam64Bit item count %d exceeds maximum count of %d",
					paramindx, C.MAXVPARMS))
			}
			err = vplist.setVPlistParam(tptoken, errstr, *paramindx, uintptr(parm64bit>>32))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
		} else {
			err = vplist.setVPlistParam(tptoken, errstr, *paramindx, uintptr(parm64bit>>32))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			(*paramindx)++
			if C.MAXVPARMS <= *paramindx {
				panic(fmt.Sprintf("YDB: setVPlistParam64Bit item count %d exceeds maximum count of %d",
					paramindx, C.MAXVPARMS))
			}
			err = vplist.setVPlistParam(tptoken, errstr, *paramindx, uintptr(parm64bit&0xffffffff))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
		}
	}
	(*paramindx)++
	return nil
}
