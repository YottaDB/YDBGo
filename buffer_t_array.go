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
	"io"
	"os"
	"sync"
	"unsafe"
)

// #include <stdlib.h>
// #include <string.h>
// #include "libyottadb.h"
// int ydb_tp_st_wrapper_cgo(uint64_t tptoken, void *tpfnparm);
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
// for each of those buffers, allocate 'nBytes' byte buffers anchoring them in the ydb_buffer_t structure.
func (buftary *BufferTArray) Alloc(numBufs, nBytes uint32) {
	var i uint32

	printEntry("BufferTArray.Alloc()")
	if nil == buftary {
		panic("*BufferTArray receiver of Alloc() cannot be nil")
	}
	if nil != buftary.cbuftary {
		buftary.Free() // Get rid of previous allocations and re-allocate
	}
	if 0 != numBufs {
		// Allocate new ydb_buffer_t array and initialize
		len := C.size_t(uint32(C.sizeof_ydb_buffer_t) * numBufs)
		cbuftary := (*[]C.ydb_buffer_t)(C.malloc(len))
		buftary.cbuftary = cbuftary
		buftary.elemsAlloc = numBufs
		buftary.elemsUsed = 0
		// Allocate a buffer for each ydb_buffer_t structure of nBytes bytes
		for i = 0; numBufs > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i)))
			(*elemptr).buf_addr = (*C.char)(C.malloc(C.size_t(nBytes)))
			(*elemptr).len_alloc = C.uint(nBytes)
			(*elemptr).len_used = 0
		}
	} else {
		// Make sure our potentially de-allocated array has a proper uninitialized state
		buftary.elemsAlloc = 0
		buftary.elemsUsed = 0
		buftary.cbuftary = nil
	}
}

// Dump is a STAPI method to dump (print) the contents of this BufferTArray block for debugging purposes. It dumps to stdout - cbuftary as a hexadecimal address,
// elemsAlloc and elemsUsed as integers, and for each element of the smaller of elemsAlloc and elemsUsed elements of the ydb_buffer_t array referenced by cbuftary,
// buf_addr as a hexadecimal address, len_alloc and len_used as integers and the smaller of len_used and len_alloc bytes at the address buf_addr in zwrite format.
func (buftary *BufferTArray) Dump() {
	buftary.DumpToWriter(os.Stdout)
}

//DumpToWriter is a writer that allows the tests or user code to dump to other than stdout.
func (buftary *BufferTArray) DumpToWriter(writer io.Writer) {
	printEntry("BufferTArray.Dump()")
	if nil == buftary {
		panic("*BufferTArray receiver of Dump() cannot be nil")
	}
	cbuftary := buftary.cbuftary
	if nil != cbuftary {
		fmt.Fprintf(writer, "BufferTArray.Dump(): cbuftary: %p, elemsAlloc: %d, elemsUsed: %d\n", cbuftary,
			buftary.elemsAlloc, buftary.elemsUsed)
		for i := 0; int(buftary.elemsUsed) > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer((uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i))))
			valstr := C.GoStringN((*elemptr).buf_addr, C.int((*elemptr).len_used))
			fmt.Fprintf(writer, "  %d: %s\n", i, valstr)
		}
	}
}

// Free is a method to release all allocated C storage in a BufferTArray. It is the inverse of the Alloc() method: release the numSubs buffers
// and the ydb_buffer_t array. Set cbuftary to nil, and elemsAlloc and elemsUsed to zero.
func (buftary *BufferTArray) Free() {
	printEntry("BufferTArray.Free()")
	if nil != buftary {
		// Deallocate the buffers in each ydb_buffer_t
		cbuftary := buftary.cbuftary
		if nil == cbuftary {
			return // Nothing to do
		}
		for i := 0; int(buftary.elemsAlloc) > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i)))
			if 0 != (*elemptr).len_alloc {
				C.free(unsafe.Pointer((*elemptr).buf_addr))
			}
		}
		// Array buffers are freed, now free the array of ydb_buffer_t structs if it exists
		C.free(unsafe.Pointer(cbuftary))
		buftary.cbuftary = nil
		buftary.elemsAlloc = 0
		buftary.elemsUsed = 0
	}
}

// ElemAlloc is a method to return elemsAlloc from a BufferTArray.
func (buftary *BufferTArray) ElemAlloc() uint32 {
	printEntry("BufferTArray.ElemAlloc()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemAlloc() cannot be nil")
	}
	return buftary.elemsAlloc
}

// ElemLenAlloc is a method to retrieve the buffer allocation length associated with our BufferTArray.
// Since all buffers are the same size in this array, just return the value from the first array entry.
// If nothing is allocated yet, return 0.
func (buftary *BufferTArray) ElemLenAlloc(tptoken uint64) uint32 {
	var retlen uint32

	printEntry("BufferTArray.ElemLenAlloc()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemLenAlloc() cannot be nil")
	}
	cbuftary := buftary.cbuftary
	if nil != cbuftary {
		elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(cbuftary))
		retlen = uint32(elemptr.len_alloc)
	} else { // Nothing is allocated yet so "allocated length" is 0
		retlen = 0
	}
	return retlen
}

// ElemLenUsed is a method to retrieve the buffer used length associated with a given buffer referenced by its index.
func (buftary *BufferTArray) ElemLenUsed(tptoken uint64, errstr *BufferT, idx uint32) (uint32, error) {
	printEntry("BufferTArray.ElemLenUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of ElemLenUsed() cannot be nil")
	}
	cbuftary := buftary.cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemcnt := buftary.elemsAlloc
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INSUFFSUBS))
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
	return buftary.elemsUsed
}

// ValBAry is a method to fetch the buffer of the indicated array element and return it as a byte array pointer.
func (buftary *BufferTArray) ValBAry(tptoken uint64, errstr *BufferT, idx uint32) (*[]byte, error) {
	var bary []byte

	printEntry("BufferTArray.ValBAry()")
	if nil == buftary {
		panic("*BufferTArray receiver of ValBAry() cannot be nil")
	}
	elemcnt := buftary.elemsAlloc
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
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
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INVSTRLEN))
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
func (buftary *BufferTArray) ValStr(tptoken uint64, errstr *BufferT, idx uint32) (*string, error) {
	var str string

	printEntry("BufferTArray.ValStr()")
	if nil == buftary {
		panic("*BufferTArray receiver of ValStr() cannot be nil")
	}
	elemcnt := buftary.elemsAlloc
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return nil, &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
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
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INVSTRLEN))
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
func (buftary *BufferTArray) SetElemLenUsed(tptoken uint64, errstr *BufferT, idx, newLen uint32) error {
	printEntry("BufferTArray.SetElemLenUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetElemLenUsed() cannot be nil")
	}
	elemcnt := buftary.elemsAlloc
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	if newLen > uint32(lenalloc) { // INVSTRLEN from last operation - return what we can and give error
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INVSTRLEN))
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
func (buftary *BufferTArray) SetElemUsed(tptoken uint64, errstr *BufferT, newUsed uint32) error {
	printEntry("BufferTArray.SetElemUsed()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetElemUsed() cannot be nil")
	}
	elemcnt := buftary.elemsAlloc
	if newUsed > elemcnt {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	// Set the number of used buffers
	buftary.elemsUsed = newUsed
	return nil
}

// SetValBAry is a method to set a byte array (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValBAry(tptoken uint64, errstr *BufferT, idx uint32, value *[]byte) error {
	printEntry("BufferTArray.SetValBAry()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetValBAry() cannot be nil")
	}
	elemcnt := buftary.elemsAlloc
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.cbuftary
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := uint32((*elemptr).len_alloc)
	vallen := uint32(len(*value))
	if vallen > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		errmsg, err := MessageT(tptoken, nil, (int)(C.YDB_ERR_INVSTRLEN))
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
func (buftary *BufferTArray) SetValStr(tptoken uint64, errstr *BufferT, idx uint32, value *string) error {
	printEntry("BufferTArray.SetValStr()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetValBAry() cannot be nil")
	}
	valuebary := []byte(*value)
	return buftary.SetValBAry(tptoken, errstr, idx, &valuebary)
}

// SetValStrLit is a method to set a string literal (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValStrLit(tptoken uint64, errstr *BufferT, idx uint32, value string) error {
	printEntry("BufferTArray.SetValStrLit()")
	if nil == buftary {
		panic("*BufferTArray receiver of SetValBAry() cannot be nil")
	}
	valuebary := []byte(value)
	return buftary.SetValBAry(tptoken, errstr, idx, &valuebary)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Simple (Threaded) API methods for BufferTArray
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// DeleteExclST is a method to delete all local variables EXCEPT the variables listed in the method BufferTArray.
// If the input array is empty, then ALL local variables are deleted. DeleteExclST() wraps ydb_delete_excl_st() to delete all
// local variable trees except those of local variables whose names are specified in the BufferTArray structure. In the special case
// where elemsUsed is zero, the method deletes all local variable trees.
//
// In the event that the elemsUsed exceeds C.YDB_MAX_NAMES, the error return is ERRNAMECOUNT2HI.
//
// As M and Go application code cannot be mixed in the same process, the warning in ydb_delete_excl_s() does not apply.
func (buftary *BufferTArray) DeleteExclST(tptoken uint64, errstr *BufferT) error {
	var cbuft *C.ydb_buffer_t
	printEntry("BufferTArray.DeleteExclST()")
	if nil == buftary {
		panic("*BufferTArray receiver of DeleteExclST() cannot be nil")
	}
	if errstr != nil {
		cbuft = errstr.cbuft
	}
	rc := C.ydb_delete_excl_st(C.uint64_t(tptoken), cbuft, C.int(buftary.elemsUsed),
		(*C.ydb_buffer_t)(unsafe.Pointer(buftary.cbuftary)))
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// TpST wraps ydb_tp_st() to implement transaction processing. tpfn is a pointer to a C function with two
// parameters, the first of which is a tptoken and the second of which is tpfnparm, a pointer to an arbitrary
// data structure in YottaDB heap space.
//
// Since Go does not permit a pointer to a Go function to be passed as a parameter to a C function, tpfn is required to be a pointer to
// a C function. For a pure Go application, the C function is a glue routine that in turn calls the Go function. The shell script
// GenYDBGlueRoutine.sh generates glue routine functions.
//
// Any function implementing logic for a transaction should return an error code with one of the following:
//
// - A normal return (nil) to indicate that per application logic, the transaction can be committed. The YottaDB database engine
// will commit the transaction if it is able to, and if not, will call the function again.
//
// - TPRESTART to indicate that the transaction should restart, either because application logic has so determined or because a YottaDB
// function called by the function has returned TPRESTART.
//
// - ROLLBACK to indicate that TpST() should not commit the transaction, and should return ROLLBACK to the caller.
//
// In order to provide the function implementing the transaction logic with a parameter or parameters, tpfnparm is passed to the glue
// routine, in turn be passed to the Go function called by the glue routine. As tpfnparm is passed from Go to YottaDB and back to
// Go, the memory it references should be allocated using Go Malloc() to protect it from the Go garbage collector.
//
// The BufferTArray receiving the TpST() method is a list of local variables whose values should be saved, and restored to their
// original values when the transaction restarts. If the cbuftary structures have not been allocated or elemsUsed is zero, no
// local variables are saved and restored; and if elemsUsed is 1, and that sole element references the string "*" all local variables
// are saved and restored.
//
// A case-insensitive value of "BA" or "BATCH" for transid indicates to YottaDB that it need not ensure Durability for this
// transaction (it continues to ensure Atomicity, Consistency, and Isolation)
//
// A special note: as the definition and implementation of Go protect against dangling pointers in pure Go code, Go application code may not
// be designed and coded with the same level of defensiveness against dangling pointers that C applications are. In the case of
// TpST(), owing to the need to use unsafe.Pointer parameters, please take additional care in designing and coding your
// application to ensure the validity of the pointers passed to TpST().
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
func (buftary *BufferTArray) TpST(tptoken uint64, errstr *BufferT, tpfn unsafe.Pointer, tpfnparm unsafe.Pointer, transid string) error {
	var cbuft *C.ydb_buffer_t
	printEntry("BufferTArray.TpST()")
	if nil == buftary {
		panic("*BufferTArray receiver of TpST() cannot be nil")
	}
	tid := C.CString(transid)
	defer C.free(unsafe.Pointer(tid)) // Should stay regular free since this was system malloc'd
	cbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(buftary.cbuftary))
	if errstr != nil {
		cbuft = errstr.cbuft
	}
	rc := C.ydb_tp_st(C.uint64_t(tptoken), cbuft, (C.ydb_tpfnptr_t)(tpfn), tpfnparm, tid,
		C.int(buftary.elemsUsed), cbuftary)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// Variables used by TpST2 to wrap passing in func
//  so the callback can retrieve it without passing pointers to C.
var tpMutex sync.Mutex
var tpIndex uint64
var tpMap map[uint64]func(uint64, *BufferT) int32

// TpST2 wraps ydb_tp_st() to implement transaction processing. The difference between
// TpST() and TpST2() is that the former uses C glue code to pass a parameter to the function implementing transaction logic,
// whereas the latter is a pure Go function call (which may be a closure).
//
// Refer to TpST() for a more detailed discussion of transaction processing.
//
// Parameters
//
// tptoken - the token used to identify nested transaction; start with yottadb.NOTTP
//
// tpfn - the closure which will be run during the transaction. This closure may get
//  invoked multiple times if a transaction fails for some reason (concurrent changes,
//  for example), so should not change any data outside of the database
//
// transid  - See docs for ydb_tp_s() in the MLPG.
func (buftary *BufferTArray) TpST2(tptoken uint64, errstr *BufferT, tpfn func(uint64, *BufferT) int32, transid string) error {
	var cbuft *C.ydb_buffer_t
	tid := C.CString(transid)
	tpMutex.Lock()
	tpfnparm := tpIndex
	tpIndex++
	if tpMap == nil {
		tpMap = make(map[uint64]func(uint64, *BufferT) int32)
	}
	tpMap[tpfnparm] = tpfn
	tpMutex.Unlock()
	defer C.free(unsafe.Pointer(tid))
	cbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*buftary).cbuftary))
	if errstr != nil {
		cbuft = errstr.cbuft
	}
	rc := C.ydb_tp_st(C.uint64_t(tptoken), cbuft, (C.ydb_tpfnptr_t)(C.ydb_tp_st_wrapper_cgo),
		unsafe.Pointer(&tpfnparm), tid, C.int((*buftary).elemsUsed), cbuftary)
	tpMutex.Lock()
	delete(tpMap, tpfnparm)
	tpMutex.Unlock()
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// YdbTpStWrapper is a private callback to wrap calls to the Go closure required for TpST2.
//export ydbTpStWrapper
func ydbTpStWrapper(tptoken uint64, errstr *C.ydb_buffer_t, tpfnparm unsafe.Pointer) int32 {
	index := *((*uint64)(tpfnparm))
	tpMutex.Lock()
	v, ok := tpMap[index]
	tpMutex.Unlock()
	var errbuff BufferT
	errbuff.FromPtr((unsafe.Pointer)(errstr))
	if !ok {
		panic("Couldn't find callback routine")
	}
	return (v)(tptoken, &errbuff)
}
