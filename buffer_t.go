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
// #include <string.h>
// #include "libyottadb.h"
// #include "libydberrors.h"
import "C"

type BufferT struct { // Contains a single ydb_buffer_t struct
	cbuft *C.ydb_buffer_t // C flavor of the ydb_buffer_t struct
}

// Alloc() is a method to allocate the ydb_buffer_t C storage and allocate or re-allocate the buffer pointed
// to by that struct.
func (buft *BufferT) Alloc(bufSiz uint32) {
	var cbuftptr *C.ydb_buffer_t

	printEntry("BufferT.Alloc()")
	if nil != (*buft).cbuft {
		// We already have a ydb_buffer_t, just get rid of current buffer for re-allocate
		cbuftptr = (*buft).cbuft
		cbufptr := (*cbuftptr).buf_addr
		C.free(unsafe.Pointer(cbufptr))
		(*cbuftptr).buf_addr = nil
	} else {
		// Allocate a C flavor ydb_buffer_t struct to pass to simpleAPI
		(*buft).cbuft = (*C.ydb_buffer_t)(C.malloc(C.size_t(C.sizeof_ydb_buffer_t)))
		cbuftptr = (*buft).cbuft
		(*cbuftptr).len_alloc = 0 // Setting these now incase have failure or interrupt before we finish
		(*cbuftptr).buf_addr = nil
	}
	(*cbuftptr).len_used = 0
	// Allocate a new buffer of the given size
	(*cbuftptr).buf_addr = (*C.char)(C.malloc(C.size_t(bufSiz)))
	(*cbuftptr).len_alloc = C.uint(bufSiz)
}

// Dump() is a method to dump the contents of a BufferT block for debugging purposes.
func (buft *BufferT) Dump() {
	printEntry("BufferT.Dump()")
	cbuftptr := (*buft).cbuft
	fmt.Printf("BufferT.Dump(): cbuftptr: %p", cbuftptr)
	if nil != cbuftptr {
		fmt.Printf(", buf_addr: %v, len_alloc: %v, len_used: %v", (*cbuftptr).buf_addr,
			(*cbuftptr).len_alloc, (*cbuftptr).len_used)
		if 0 < (*cbuftptr).len_used {
			strval := C.GoStringN((*cbuftptr).buf_addr, C.int((*cbuftptr).len_used))
			fmt.Printf(", value: %s", strval)
		}
	}
	fmt.Printf("\n")
}

// Free() is a method to release both the buffer and ydb_buffer_t block associate with the BufferT block.
func (buft *BufferT) Free() {
	printEntry("BufferT.Free()")
	cbuftptr := (*buft).cbuft
	if nil != cbuftptr {
		// ydb_buffer_t block exists - free its buffer first if it exists
		if nil != (*cbuftptr).buf_addr {
			C.free(unsafe.Pointer((*cbuftptr).buf_addr))
		}
		C.free(unsafe.Pointer(cbuftptr))
		(*buft).cbuft = nil
	}
}

// LenAlloc() is a method to fetch the ydb_buffer_t.len_alloc field containing the allocated length of the buffer.
func (buft *BufferT) LenAlloc(tptoken uint64) (uint32, error) {
	printEntry("BufferT.LenAlloc()")
	cbuftptr := (*buft).cbuft
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	return (uint32)((*cbuftptr).len_alloc), nil
}

// LenUsed() is a method to fetch the ydb_buffer_t.len_used field containing the used length of the buffer. Note
// that if len_used > than len_alloc thus indicating a previous issue, an INVSTRLEN error is raised.
func (buft *BufferT) LenUsed(tptoken uint64) (uint32, error) {
	printEntry("BufferT.LenUsed()")
	cbuftptr := (*buft).cbuft
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0xffffffff, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := (*cbuftptr).len_alloc
	lenused := (*cbuftptr).len_used
	if lenused > lenalloc {
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return 0xffffffff, &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	return uint32(lenused), nil
}

// ValBAry() is a method to fetch the buffer contents as a byte array (returned as *[]byte to limit copies made).
func (buft *BufferT) ValBAry(tptoken uint64) (*[]byte, error) {
	var bary []byte

	printEntry("BufferT.ValBAry()")
	cbuftptr := (*buft).cbuft
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := (*cbuftptr).len_alloc
	lenused := (*cbuftptr).len_used
	cbufptr := (*cbuftptr).buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		bary = C.GoBytes(unsafe.Pointer(cbufptr), C.int(lenalloc)) // Return what we can (alloc size)
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &bary, &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	// The entire buffer is there so return that
	bary = C.GoBytes(unsafe.Pointer(cbufptr), C.int(lenused))
	return &bary, nil
}

// ValStr() is a method to fetch the buffer contents as a string (returned as *string to limit copies made).
func (buft *BufferT) ValStr(tptoken uint64) (*string, error) {
	var str string

	printEntry("BufferT.ValStr()")
	cbuftptr := (*buft).cbuft
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := (*cbuftptr).len_alloc
	lenused := (*cbuftptr).len_used
	cbufptr := (*cbuftptr).buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		str = C.GoStringN(cbufptr, C.int(lenalloc)) // Return what we can (alloc size)
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &str, &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	// The entire buffer is there so return that
	str = C.GoStringN(cbufptr, C.int(lenused))
	return &str, nil
}

// SetLenUsed() is a method to set the used length of buffer in the ydb_buffer_t block (must be <= alloclen).
func (buft *BufferT) SetLenUsed(tptoken uint64, newLen uint32) error {
	printEntry("BufferT.SetLenUsed()")
	cbuftptr := (*buft).cbuft
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := (*cbuftptr).len_alloc
	if newLen > uint32(lenalloc) {
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	(*cbuftptr).len_used = C.uint(newLen)
	return nil
}

// SetValBAry() is a method to set a []byte array into the given buffer.
func (buft *BufferT) SetValBAry(tptoken uint64, value *[]byte) error {
	printEntry("BufferT.SetValBAry()")
	cbuftptr := (*buft).cbuft
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	vallen := C.uint(len(*value))
	if vallen > (*cbuftptr).len_alloc {
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		(*cbuftptr).len_used = vallen // Set so caller knows what alloc length SHOULD have been (minimum)
		return &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	// Copy the Golang buffer to the C buffer
	if 0 < vallen {
		C.memcpy(unsafe.Pointer((*cbuftptr).buf_addr),
			unsafe.Pointer(&((*value)[0])),
			C.size_t(vallen))
	}
	(*cbuftptr).len_used = vallen
	return nil
}

// SetValStr() is a method to set a string into the given buffer.
func (buft *BufferT) SetValStr(tptoken uint64, value *string) error {
	printEntry("BufferT.SetValStr()")
	valuebary := []byte(*value)
	return buft.SetValBAry(tptoken, &valuebary)
}

// SetValStrLit() is a method to set a literal string into the given buffer.
func (buft *BufferT) SetValStrLit(tptoken uint64, value string) error {
	printEntry("BufferT.SetValStrLit()")
	valuebary := []byte(value)
	return buft.SetValBAry(tptoken, &valuebary)
}

// Str2ZwrST is a STAPI method to take the given string and return it in ZWRITE format.
func (buft *BufferT) Str2ZwrST(tptoken uint64, zwr *BufferT) error {
	printEntry("BufferT.Str2ZwrST()")
	rc := C.ydb_str2zwr_st(C.uint64_t(tptoken), (*buft).cbuft, (*zwr).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	// Returned string should be snug in the zwr buffer
	return nil
}

// Zwr2StrST() is a STAPI method to take the given ZWRITE format string and return it as a normal ASCII string.
func (buft *BufferT) Zwr2StrST(tptoken uint64, str *BufferT) error {
	printEntry("BufferT.Zwr2StrST()")
	rc := C.ydb_zwr2str_st(C.uint64_t(tptoken), (*buft).cbuft, (*str).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	// Returned string should be snug in the str buffer
	return nil
}
