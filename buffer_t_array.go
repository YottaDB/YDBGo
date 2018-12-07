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
	"io"
	"os"
)

// #include <stdlib.h>
// #include <string.h>
// #include "libyottadb.h"
// #include "libydberrors.h"
import "C"

// BufferTArray is an array of ydb_buffer_t structures. The reason this is not an array of BufferT structures is because
// we can't pass a pointer to those Golang structures to a C routine (cgo restriction) so we have to have this separate
// array of the C structures instead. Also, cgo doesn't support indexing of C structures so we have to do that ourselves
// as well.
type BufferTArray struct {
	elemsAlloc uint32            // Number of elements in array
	elemsUsed  uint32            // Number of elements used
	cbuftary   *[]C.ydb_buffer_t // C flavor of ydb_buffer_t array
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Data manipulation methods for BufferTArray
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// Alloc is a method to allocate an array of 'numBufs' ydb_buffer_t structures anchored in this BufferTArray and also
// for each of those buffers, allocate 'bufSiz' byte buffers anchoring them in the ydb_buffer_t structure.
func (buftary *BufferTArray) Alloc(numBufs, bufSiz uint32) {
	var i uint32

	printEntry("BufferTArray.Alloc()")
	if nil == buftary {
		panic("*BufferTArray receiver of Alloc() cannot be nil")
	}
	if nil != (*buftary).cbuftary {
		buftary.Free() // Get rid of previous allocations and re-allocate
	}
	// Allocate new ydb_buffer_t array and initialize
	len := C.size_t(uint32(C.sizeof_ydb_buffer_t) * numBufs)
	cbuftary := (*[]C.ydb_buffer_t)(C.malloc(len))
	(*buftary).cbuftary = cbuftary
	(*buftary).elemsAlloc = numBufs
	(*buftary).elemsUsed = 0
	// Allocate a buffer for each ydb_buffer_t structure of bufSiz bytes
	for i = 0; numBufs > i; i++ {
		elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) +
			uintptr(C.sizeof_ydb_buffer_t*i)))
		(*elemptr).buf_addr = (*C.char)(C.malloc(C.size_t(bufSiz)))
		(*elemptr).len_alloc = C.uint(bufSiz)
		(*elemptr).len_used = 0
	}
}

// Dump is a STAPI method to dump (print) the contents of this BufferTArray block
func (buftary *BufferTArray) Dump() {
	buftary.DumpToWriter(os.Stdout)
}

//DumpToWriter is a writer that allows the tests or user code to dump to other than stdout
func (buftary *BufferTArray) DumpToWriter(w io.Writer) {
	printEntry("BufferTArray.Dump()")
	if nil == buftary {
		panic("*BufferTArray receiver of Dump() cannot be nil")
	}
	cbuftary := (*buftary).cbuftary
	if nil != cbuftary {
		fmt.Fprintf(w, "BufferTArray.Dump(): cbuftary: %p, elemsAlloc: %d, elemsUsed: %d\n", cbuftary,
			(*buftary).elemsAlloc, (*buftary).elemsUsed)
		for i := 0; int((*buftary).elemsUsed) > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer((uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i))))
			valstr := C.GoStringN((*elemptr).buf_addr, C.int((*elemptr).len_used))
			fmt.Fprintf(w, "  %d: %s\n", i, valstr)
		}
	}
}

// Free is a method to release all allocated C storage in a BufferTArray.
func (buftary *BufferTArray) Free() {
	printEntry("BufferTArray.Free()")
	if nil != buftary {
		// Deallocate the buffers in each ydb_buffer_t
		cbuftary := (*buftary).cbuftary
		if nil == cbuftary {
			return  // Nothing to do
		}
		for i := 0; int((*buftary).elemsAlloc) > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i)))
			if 0 != (*elemptr).len_alloc {
				C.free(unsafe.Pointer((*elemptr).buf_addr))
			}
		}
		// Array buffers are freed, now free the array of ydb_buffer_t structs if it exists
		C.free(unsafe.Pointer(cbuftary))
		(*buftary).cbuftary = nil
		(*buftary).elemsAlloc = 0
		(*buftary).elemsUsed = 0
	}
}

// ElemAlloc is a method to return elemsAlloc from a BufferTArray.
func (buftary *BufferTArray) ElemAlloc() uint32 {
	printEntry("BufferTArray.ElemAlloc()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemAlloc() cannot be nil")
	}
	return (*buftary).elemsAlloc
}

// ElemLenAlloc is a method to retrieve the buffer allocation length associated with our BufferTArray.
// Since all buffers are the same size in this array, just return the value from the first array entry.
func (buftary *BufferTArray) ElemLenAlloc(tptoken uint64) (uint32, error) {
	printEntry("BufferTArray.ElemLenAlloc()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemLenAlloc() cannot be nil")
	}
	cbuftary := (*buftary).cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(cbuftary))
	return uint32(elemptr.len_alloc), nil
}

// ElemLenUsed is a method to retrieve the buffer used length associated with a given buffer referenced by its index.
func (buftary *BufferTArray) ElemLenUsed(tptoken uint64, idx uint32) (uint32, error) {
	printEntry("BufferTArray.ElemLenUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemLenUsed() cannot be nil")
	}
	cbuftary := (*buftary).cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemcnt := (*buftary).elemsAlloc
	if idx > (elemcnt - 1) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return 0, &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	return uint32(elemptr.len_used), nil
}

// ElemUsed is a method to return elemsUsed from a BufferTArray.
func (buftary *BufferTArray) ElemUsed() uint32 {
	printEntry("BufferTArray.ElemUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemUsed() cannot be nil")
	}
	return (*buftary).elemsUsed
}

// ValBAry is a method to fetch the buffer of the indicated array element and return it as a byte array pointer.
func (buftary *BufferTArray) ValBAry(tptoken uint64, idx uint32) (*[]byte, error) {
	var bary []byte

	printEntry("BufferTArray.ValBAry()")
	if nil == buftary {
		panic("*BufferTArray receiver of ValBAry() cannot be nil")
	}
	elemcnt := (*buftary).elemsAlloc
	if idx > (elemcnt - 1) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := (*buftary).cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	lenused := (*elemptr).len_used
	cbufptr := (*elemptr).buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation return what we can and give error
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

// ValStr is a method to fetch the buffer of the indicated array element and return it as a string pointer.
func (buftary *BufferTArray) ValStr(tptoken uint64, idx uint32) (*string, error) {
	var str string

	printEntry("BufferTArray.ValStr()")
	if nil == buftary {
		panic("*BufferTArray receiver of ValStr() cannot be nil")
	}
	elemcnt := (*buftary).elemsAlloc
	if idx > (elemcnt - 1) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := (*buftary).cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	lenused := (*elemptr).len_used
	cbufptr := (*elemptr).buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation return what we can and give error
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

// SetElemLenUsed is a method to set the len_used field of a given ydb_buffer_t struct in the BufferTArray.
func (buftary *BufferTArray) SetElemLenUsed(tptoken uint64, idx, newLen uint32) error {
	printEntry("BufferTArray.SetElemLenUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetElemLenUsed() cannot be nil")
	}
	elemcnt := (*buftary).elemsAlloc
	if idx > (elemcnt - 1) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := (*buftary).cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	if newLen > uint32(lenalloc) { // INVSTRLEN from last operation - return what we can and give error
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	// Set the new used length
	(*elemptr).len_used = C.uint(newLen)
	return nil
}

// SetElemUsed is a method to set the number of used buffers in the BufferTArray.
func (buftary *BufferTArray) SetElemUsed(tptoken uint64, newUsed uint32) error {
	printEntry("BufferTArray.SetElemUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetElemUsed() cannot be nil")
	}
	elemcnt := (*buftary).elemsAlloc
	if newUsed > elemcnt {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	// Set the number of used buffers
	(*buftary).elemsUsed = newUsed
	return nil
}

// SetValBAry is a method to set a byte array (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValBAry(tptoken uint64, idx uint32, value *[]byte) error {
	printEntry("BufferTArray.SetValBAry()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetValBAry() cannot be nil")
	}
	elemcnt := (*buftary).elemsAlloc
	if idx > (elemcnt - 1) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := (*buftary).cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := uint32((*elemptr).len_alloc)
	vallen := uint32(len(*value))
	if vallen > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INVSTRLEN), errmsg}
	}
	// Copy the Golang buffer to the C buffer
	if 0 < vallen {
		C.memcpy(unsafe.Pointer((*elemptr).buf_addr),
			unsafe.Pointer(&((*value)[0])),
			C.size_t(vallen))
	}
	(*elemptr).len_used = C.uint(vallen) // Set the used length of the buffer for this element
	return nil
}

// SetValStr is a method to set a string (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValStr(tptoken uint64, idx uint32, value *string) error {
	printEntry("BufferTArray.SetValStr()")
	valuebary := []byte(*value)
	return buftary.SetValBAry(tptoken, idx, &valuebary)
}

// SetValStrLit is a method to set a string literal (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValStrLit(tptoken uint64, idx uint32, value string) error {
	printEntry("BufferTArray.SetValStrLit()")
	valuebary := []byte(value)
	return buftary.SetValBAry(tptoken, idx, &valuebary)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Simple (Threaded) API methods for BufferTArray
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// DeleteExclST is a method to delete all local variables EXCEPT the variables listed in the method BufferTArray.
// If the input array is empty, then ALL local variables are deleted.
func (buftary *BufferTArray) DeleteExclST(tptoken uint64) error {
	printEntry("BufferTArray.DeleteExclST()")
	if nil == buftary {
		panic("*BufferTArray receiver of DeleteExclST() cannot be nil")
	}
	rc := C.ydb_delete_excl_st(C.uint64_t(tptoken), C.int((*buftary).elemsUsed),
		(*C.ydb_buffer_t)(unsafe.Pointer((*buftary).cbuftary)))
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// TpST is a STAPI method to invoke transaction processing.
//
// Parameters
//
// tpfn - C function pointer routine that either performs the transaction or immediately calls a Golang routine to
// perform the transaction. On return from that routine, the transaction is committed.
//
// tpfnparm - A single parameter that can be a pointer to a structure to provide parameters to the transaction routine.
//              Note these parameters MUST LIVE in C allocated memory or the call is likely to fail.
//
// transid  - See docs for ydb_tp_s() in the MLPG.
func (buftary *BufferTArray) TpST(tptoken uint64, tpfn unsafe.Pointer, tpfnparm unsafe.Pointer, transid string) error {
	printEntry("BufferTArray.TpST()")
	if nil == buftary {
		panic("*BufferTArray receiver of TpST() cannot be nil")
	}
	tid := C.CString(transid)
	defer C.free(unsafe.Pointer(tid)) // Should stay regular free since this was system malloc'd
	cbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*buftary).cbuftary))
	rc := C.ydb_tp_st(C.uint64_t(tptoken), (C.ydb_tpfnptr_t)(tpfn), tpfnparm, tid,
		C.int((*buftary).elemsUsed), cbuftary)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}
