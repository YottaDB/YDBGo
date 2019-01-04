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
	"strings"
	"strconv"
	"unsafe"
)

// #include "libyottadb.h"
// /* Equivalent of gparam_list in callg.h (not available to us) */
// #define MAXVPARMS 36
// /* C routine to get around the cgo issue and its lack of support for variadic plist routines */
// int ydb_go_lock_st(uint64_t tptoken, ydb_buffer_t *errstr, uintptr_t cvplist);
// int ydb_go_lock_st(uint64_t tptoken, ydb_buffer_t *errstr, uintptr_t cvplist)
// {
// 	return ydb_call_variadic_plist_func_st(tptoken, errstr, (ydb_vplist_func)&ydb_lock_s, cvplist);
// }
import "C"

//
// This file contains the only Simple API routine that is not a method. The rest of the threaded SimpleAPI method functions
// are defined in buffer_t.go, bufer_t_array.go, and key_t.go with utilities being defined in util.go.
//

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Simple (Threaded) API function(s)
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// LockST is a STAPI function that releases all existing locks then locks the supplied variadic list of lock keys.
func LockST(tptoken uint64, errstr *BufferT, timeoutNsec uint64, lockname ...*KeyT) error {
	var vplist variadicPlist
	var lockcnt, namecnt int
	var parmindx int

	printEntry("KeyT.SubNextST()")
	defer vplist.free()
	vplist.alloc()
	// Before we put the timeout parameter into the plist, we need to check our architecture. If this is 64 bit,
	// we're fine and can proceed but if we are 32 bit, then the 64 bit timeoutNsec parameter needs to be split
	// in half across two parms which will be reassembled in ydb_lock_s().
	if 64 == strconv.IntSize {
		vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(timeoutNsec))
		parmindx++
	} else {
		if IsLittleEndian() {
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(timeoutNsec&0xffffffff))
			parmindx++
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(timeoutNsec>>32))
			parmindx++
		} else {
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(timeoutNsec>>32))
			parmindx++
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(timeoutNsec&0xffffffff))
			parmindx++
		}
	}
	lockcnt = len(lockname)
	namecnt = lockcnt
	vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(namecnt))
	parmindx++
	if 0 != lockcnt {
		parmsleft := C.MAXVPARMS - parmindx // We've already used two parms (timeout and namecount)
		parmsleftorig := parmsleft // Save for error below just-in-case
		lockindx := 0                // The next lockname index to be read
		// Load the lockname parameters into the plist
		for 0 < lockcnt {
			// Make sure enough room for another set of 3 parms
			if 3 > parmsleft {
				errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_NAMECOUNT2HI))
				if nil != err {
					panic(fmt.Sprintf("YDB: Error fetching NAMECOUNT2HI: %s", err))
				}
				// Do some error message substitution
				errmsg = strings.Replace(errmsg, "!AD", "LockST()", 1)
				errmsg = strings.Replace(errmsg, "!UL", fmt.Sprintf("%d", parmsleftorig / 3), 1)
				return &YDBError{(int)(C.YDB_ERR_NAMECOUNT2HI), errmsg}
			}
			// Set the 3 parameters for this lockname
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(unsafe.Pointer((*lockname[lockindx]).Varnm.cbuft)))
			parmindx++
			parmsleft--
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr((*lockname[lockindx]).Subary.elemsUsed))
			parmindx++
			parmsleft--
			subgobuftary := &((*lockname[lockindx]).Subary)
			subbuftary := unsafe.Pointer(subgobuftary.cbuftary)
			vplist.setVPlistParam(tptoken, errstr, parmindx, uintptr(subbuftary))
			parmindx++
			parmsleft--
			// Housekeeping
			lockindx++
			lockcnt--
		}
	}
	vplist.setUsed(tptoken, errstr, uint32(parmindx))
	// At this point, vplist now contains the plist we want to send to ydb_lock_s(). However, Golang/cgo does not permit
	// either the call or even creating a function pointer to ydb_lock_s(). So instead of driving vplist.callVariadicPlistFuncST()
	// which is what we would normally do here, we're going to call a C helper function (defined in the cgo preamble at the
	// top of this routine) to do the call that callVariadicPlistFuncST() would have done.
	var cbuft *C.ydb_buffer_t
	if errstr != nil {
		cbuft = errstr.cbuft
	}
	rc := C.ydb_go_lock_st(C.uint64_t(tptoken), cbuft, (C.uintptr_t)(uintptr(unsafe.Pointer(vplist.cvplist))))
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}
