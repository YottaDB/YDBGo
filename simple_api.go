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
	"strconv"
	"strings"
	"unsafe"
)

// #include "libyottadb.h"
// /* Equivalent of gparam_list in callg.h (not available to us) */
// #define MAXVPARMS 36
// /* C routine to get around the cgo issue and its lack of support for variadic plist routines */
// void *ydb_get_lockst_funcvp(void);
// void *ydb_get_lockst_funcvp(void)
// {
// 	return (void *)&ydb_lock_st;
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
	var parmIndx int
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.SubNextST()")
	defer vplist.free()
	vplist.alloc()
	// First two parms are the tptoken and the contents of the BufferT (not the BufferT itself).
	err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(tptoken))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
	}
	parmIndx++
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(cbuft)))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
	}
	parmIndx++
	// Before we put the timeout parameter into the plist, we need to check our architecture. If this is 64 bit,
	// we're fine and can proceed but if we are 32 bit, then the 64 bit timeoutNsec parameter needs to be split
	// in half across two parms which will be reassembled in ydb_lock_s().
	if 64 == strconv.IntSize {
		err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(timeoutNsec))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
		}
		parmIndx++
	} else {
		if IsLittleEndian() {
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(timeoutNsec&0xffffffff))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(timeoutNsec>>32))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
		} else {
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(timeoutNsec>>32))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(timeoutNsec&0xffffffff))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
		}
	}
	lockcnt = len(lockname)
	namecnt = lockcnt
	err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(namecnt))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
	}
	parmIndx++
	if 0 != lockcnt {
		parmsleft := C.MAXVPARMS - parmIndx // We've already used two parms (timeout and namecount)
		parmsleftorig := parmsleft          // Save for error below just-in-case
		lockindx := 0                       // The next lockname index to be read
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
				errmsg = strings.Replace(errmsg, "!UL", fmt.Sprintf("%d", namecnt), 1)
				errmsg = strings.Replace(errmsg, "!UL", fmt.Sprintf("%d", parmsleftorig/3), 1)
				return &YDBError{(int)(C.YDB_ERR_NAMECOUNT2HI), errmsg}
			}
			// Set the 3 parameters for this lockname
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer((*lockname[lockindx]).Varnm.getCPtr())))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
			parmsleft--
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr((*lockname[lockindx]).Subary.ElemUsed()))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
			parmsleft--
			subgobuftary := (*lockname[lockindx]).Subary
			subbuftary := unsafe.Pointer(subgobuftary.getCPtr())
			err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(subbuftary))
			if nil != err {
				panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
			}
			parmIndx++
			parmsleft--
			// Housekeeping
			lockindx++
			lockcnt--
		}
	}
	err = vplist.setUsed(tptoken, errstr, uint32(parmIndx))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setUsed(): %s", err))
	}
	// At this point, vplist now contains the plist we want to send to ydb_lock_s(). However, Golang/cgo does not permit
	// either the call or even creating a function pointer to ydb_lock_s(). So instead of driving vplist.CallVariadicPlistFuncST()
	// which is what we would normally do here, we're going to call a C helper function (defined in the cgo preamble at the
	// top of this routine) to do the call that callVariadicPlistFuncST() would have done.
	rc := vplist.callVariadicPlistFunc(C.ydb_get_lockst_funcvp()) // Drive ydb_lock_st()
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}
