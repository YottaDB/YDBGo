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

// #include "libyottadb.h"
// /* Equivalent of gparam_list in callg.h (not available to us) */
// #define MAXVPARMS 36
// typedef struct {
//         intptr_t  n;
//         uintptr_t arg[MAXVPARMS];
// } gparam_list;
// int YdB_vArIaDiC_pLiSt_TeSt();  /* Don't define parms here as variadic plist declarations gives cgo issues */
// /* Note these defines also appear in the YdB_vArIaDiC_pLiSt_TeSt.c test routine so if they change there, change them here */
// #define expectedbuf1	"Buffer one"
// #define expectedbuf2	"Buffer two"
import "C"

import (
	"fmt"
	"unsafe"
)

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Test routine(s)
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// TestVariadicPlistHelper routine to checkout variadic parameter list methods.
func TestVariadicPlistHelper(debugFlag bool, errors *int) error {
	var vplist variadicPlist
	var v1, v2 BufferT
	var expectedstr string

	defer vplist.free()
	defer v1.Free()
	defer v2.Free()
	if debugFlag {
		fmt.Println("VPLST: Variadic plist test starting")
	}
	// Build a variadic plist with 4 items - first create some stuff to send
	vplist.alloc()
	v1.Alloc(32)
	v2.Alloc(32)
	expectedstr = C.expectedbuf1
	v1.SetValStr(NOTTP, &expectedstr)
	expectedstr = C.expectedbuf2
	v2.SetValStr(NOTTP, &expectedstr)
	// Place items in variable length parm list non-serially
	vplist.setVPlistParam(NOTTP, 3, uintptr(unsafe.Pointer(v2.cbuft)))
	vplist.setVPlistParam(NOTTP, 2, uintptr(unsafe.Pointer(v1.cbuft)))
	vplist.setVPlistParam(NOTTP, 0, uintptr(3)) // The count of parms passed in
	vplist.setVPlistParam(NOTTP, 1, uintptr(42))
	vplist.setUsed(NOTTP, 4)
	// TODO: we should verify the return value of this function and return an error if not correct
	vplist.callVariadicPlistFuncST(NOTTP, C.YdB_vArIaDiC_pLiSt_TeSt)
	if debugFlag {
		fmt.Println("VPLST: Variadic plist test complete!")
	}
	return nil
}
