//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC and/or its subsidiaries.	//
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
	"unsafe"
)

// #include "libyottadb.h"
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Miscellaneous functions
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// max is a function to provide max integer value between two given values.
func max(x int, y int) int {
	if x >= y {
		return x
	}
	return y
}

// printEntry is a function to print the entry point of the function, when entered, if the debug flag is enabled.
func printEntry(funcName string) {
	if debugFlag {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			fmt.Println("Entered ", funcName, " from ", file, " at line ", line)
		} else {
			fmt.Println("Entered ", funcName)
		}
	}
}

// initkey is a function to initialize a provided key with the provided varname and subscript array in string form.
func initkey(tptoken uint64, errstr *BufferT, dbkey *KeyT, varname *string, subary *[]string) {
	var maxsublen, sublen, i uint32
	var err error

	subcnt := uint32(len(*subary))
	maxsublen = 0
	for i = 0; i < subcnt; i++ {
		// Find maximum length of subscript so know how much to allocate
		sublen = uint32(len((*subary)[i]))
		if sublen > maxsublen {
			maxsublen = sublen
		}
	}
	dbkey.Alloc(uint32(len(*varname)), subcnt, maxsublen)
	dbkey.Varnm.SetValStr(tptoken, errstr, varname)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	// Load subscripts into KeyT (if any)
	for i = 0; i < subcnt; i++ {
		err = dbkey.Subary.SetValStr(tptoken, errstr, i, &((*subary)[i]))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	err = dbkey.Subary.SetElemUsed(tptoken, errstr, subcnt)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetUsed(): %s", err))
	}
}

// IsLittleEndian is a function to determine endianness. Exposed in case anyone else wants to know.
func IsLittleEndian() bool {
	var bittest = 0x01

	if 0x01 == *(*byte)(unsafe.Pointer(&bittest)) {
		return true
	}
	return false
}

// Exit is a function to drive YDB's exit handler in case of panic or other non-normal shutdown that bypasses
// atexit() that would normally drive the exit handler.
func Exit() {
	C.ydb_exit()
}
