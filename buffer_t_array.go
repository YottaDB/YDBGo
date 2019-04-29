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
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// #include <stdlib.h>
// #include <string.h>
// #include "libyottadb.h"
// int ydb_tp_st_wrapper_cgo(uint64_t tptoken, void *tpfnparm);
import "C"

// Variables used by TpST to wrap passing in func so the callback can retrieve it without passing pointers to C.
var tpIndex uint64
var tpMap sync.Map

// BufferTArray is an array of ydb_buffer_t structures. The reason this is not an array of BufferT structures is because
// we can't pass a pointer to those Golang structures to a C routine (cgo restriction) so we have to have this separate
// array of the C structures instead. Also, cgo doesn't support indexing of C structures so we have to do that ourselves
// as well. Because this structure's contents contain pointers to C allocated storage, this structure is NOT safe for
// concurrent access unless those accesses are to different array elements and do not affect the overall structure.
type BufferTArray struct {
	cbuftary *internalBufferTArray
}

type internalBufferTArray struct {
	elemUsed  uint32            // Number of elements used
	elemAlloc uint32            // Number of elements in array
	cbuftary  *[]C.ydb_buffer_t // C flavor of ydb_buffer_t array
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
		panic("YDB: *BufferTArray receiver of Alloc() cannot be nil")
	}
	// Forget the previous structure, then allocate a new one if needed
	buftary.cbuftary = nil
	if 0 != numBufs {
		// Allocate new ydb_buffer_t array and initialize
		len := C.size_t(uint32(C.sizeof_ydb_buffer_t) * numBufs)
		cbuftary := (*[]C.ydb_buffer_t)(C.malloc(len))
		buftary.cbuftary = &internalBufferTArray{0, numBufs, cbuftary}
		// Allocate a buffer for each ydb_buffer_t structure of nBytes bytes
		for i = 0; numBufs > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i)))
			(*elemptr).buf_addr = (*C.char)(C.malloc(C.size_t(nBytes)))
			(*elemptr).len_alloc = C.uint(nBytes)
			(*elemptr).len_used = 0
		}
		runtime.SetFinalizer(buftary.cbuftary, func(o *internalBufferTArray) {
			o.Free()
		})
	}
}

// Dump is a STAPI method to dump (print) the contents of this BufferTArray block for debugging purposes. It dumps to stdout
//   - cbuftary as a hexadecimal address,
//   - elemAlloc and elemUsed as integers,
//   - and for each element of the smaller of elemAlloc and elemUsed elements of the ydb_buffer_t array referenced by cbuftary,
//     buf_addr as a hexadecimal address, len_alloc and len_used as integers and the smaller of len_used and len_alloc bytes at
//     the address buf_addr in zwrite format.
func (buftary *BufferTArray) Dump() {
	buftary.DumpToWriter(os.Stdout)
}

//DumpToWriter is a writer that allows the tests or user code to dump to other than stdout.
func (buftary *BufferTArray) DumpToWriter(writer io.Writer) {
	printEntry("BufferTArray.Dump()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of Dump() cannot be nil")
	}
	cbuftary := buftary.getCPtr()
	if nil != cbuftary {
		fmt.Fprintf(writer, "BufferTArray.Dump(): cbuftary: %p, elemAlloc: %d, elemUsed: %d\n", cbuftary,
			buftary.ElemAlloc(), buftary.ElemUsed())
		for i := 0; int(buftary.ElemUsed()) > i; i++ {
			elemptr := (*C.ydb_buffer_t)(unsafe.Pointer((uintptr(unsafe.Pointer(cbuftary)) +
				uintptr(C.sizeof_ydb_buffer_t*i))))
			// It is possible len_used is greater than len_alloc (if this buffer was populated by SimpleAPI C code)
			// Ensure we do not overrun the allocated buffer while dumping this object in that case.
			min := (*elemptr).len_used
			if min > (*elemptr).len_alloc {
				min = (*elemptr).len_alloc
			}
			valstr := C.GoStringN((*elemptr).buf_addr, C.int(min))
			fmt.Fprintf(writer, "  %d: %s\n", i, valstr)
		}
	}
}

// Free is a method to release all allocated C storage in a BufferTArray. It is the inverse of the Alloc() method: release the numSubs buffers
// and the ydb_buffer_t array. Set cbuftary to nil, and elemAlloc and elemUsed to zero.
func (buftary *BufferTArray) Free() {
	printEntry("BufferTArray.Free()")
	if nil == buftary {
		return
	}
	buftary.cbuftary.Free()
	buftary.cbuftary = nil
}

// Free cleans up C memory for the internal BufferTArray object
func (ibuftary *internalBufferTArray) Free() {
	printEntry("internalBufferTArray.Free()")
	if nil == ibuftary {
		return
	}
	// Deallocate the buffers in each ydb_buffer_t
	cbuftary := ibuftary.cbuftary
	if nil == cbuftary {
		return // Nothing to do
	}
	for i := 0; int(ibuftary.elemAlloc) > i; i++ {
		elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) +
			uintptr(C.sizeof_ydb_buffer_t*i)))
		if 0 != (*elemptr).len_alloc {
			C.free(unsafe.Pointer((*elemptr).buf_addr))
		}
	}
	// Array buffers are freed, now free the array of ydb_buffer_t structs if it exists
	C.free(unsafe.Pointer(cbuftary))
	ibuftary.cbuftary = nil
}

// ElemAlloc is a method to return elemAlloc from a BufferTArray.
func (buftary *BufferTArray) ElemAlloc() uint32 {
	printEntry("BufferTArray.ElemAlloc()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of ElemAlloc() cannot be nil")
	}
	if nil == buftary.cbuftary {
		return 0
	}
	return buftary.cbuftary.elemAlloc
}

// ElemLenAlloc is a method to retrieve the buffer allocation length associated with our BufferTArray.
// Since all buffers are the same size in this array, just return the value from the first array entry.
// If nothing is allocated yet, return 0.
func (buftary *BufferTArray) ElemLenAlloc() uint32 {
	var retlen uint32

	printEntry("BufferTArray.ElemLenAlloc()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of ElemLenAlloc() cannot be nil")
	}
	cbuftary := buftary.getCPtr()
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
		panic("YDB: *BufferTArray receiver of ElemLenUsed() cannot be nil")
	}
	cbuftary := buftary.getCPtr()
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemcnt := buftary.ElemAlloc()
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return 0, &YDBError{(int)(YDB_ERR_INSUFFSUBS), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	return uint32(elemptr.len_used), nil
}

// ElemUsed is a method to return elemUsed from a BufferTArray.
func (buftary *BufferTArray) ElemUsed() uint32 {
	printEntry("BufferTArray.ElemUsed()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of ElemUsed() cannot be nil")
	}
	if nil == buftary.cbuftary {
		return 0
	}
	return buftary.cbuftary.elemUsed
}

// ValBAry is a method to fetch the buffer of the indicated array element and return it as a byte array pointer.
func (buftary *BufferTArray) ValBAry(tptoken uint64, errstr *BufferT, idx uint32) (*[]byte, error) {
	var bary []byte

	printEntry("BufferTArray.ValBAry()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of ValBAry() cannot be nil")
	}
	elemcnt := buftary.ElemAlloc()
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return nil, &YDBError{(int)(YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.getCPtr()
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	lenused := (*elemptr).len_used
	cbufptr := (*elemptr).buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation return what we can and give error
		bary = C.GoBytes(unsafe.Pointer(cbufptr), C.int(lenalloc)) // Return what we can (alloc size)
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &bary, &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
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
		panic("YDB: *BufferTArray receiver of ValStr() cannot be nil")
	}
	elemcnt := buftary.ElemAlloc()
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return nil, &YDBError{(int)(YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.getCPtr()
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	lenused := (*elemptr).len_used
	cbufptr := (*elemptr).buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation return what we can and give error
		str = C.GoStringN(cbufptr, C.int(lenalloc)) // Return what we can (alloc size)
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &str, &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
	}
	// The entire buffer is there so return that
	str = C.GoStringN(cbufptr, C.int(lenused))
	return &str, nil
}

// SetElemLenUsed is a method to set the len_used field of a given ydb_buffer_t struct in the BufferTArray.
func (buftary *BufferTArray) SetElemLenUsed(tptoken uint64, errstr *BufferT, idx, newLen uint32) error {
	printEntry("BufferTArray.SetElemLenUsed()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of SetElemLenUsed() cannot be nil")
	}
	elemcnt := buftary.ElemAlloc()
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.getCPtr()
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := (*elemptr).len_alloc
	if newLen > uint32(lenalloc) { // INVSTRLEN from last operation - return what we can and give error
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
	}
	// Set the new used length
	(*elemptr).len_used = C.uint(newLen)
	return nil
}

// SetElemUsed is a method to set the number of used buffers in the BufferTArray.
func (buftary *BufferTArray) SetElemUsed(tptoken uint64, errstr *BufferT, newUsed uint32) error {
	printEntry("BufferTArray.SetElemUsed()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of SetElemUsed() cannot be nil")
	}
	elemcnt := buftary.ElemAlloc()
	if newUsed > elemcnt {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_INSUFFSUBS), errmsg}
	}
	// Set the number of used buffers
	if nil != buftary.cbuftary {
		buftary.cbuftary.elemUsed = newUsed
	}
	return nil
}

// SetValBAry is a method to set a byte array (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValBAry(tptoken uint64, errstr *BufferT, idx uint32, value *[]byte) error {
	printEntry("BufferTArray.SetValBAry()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of SetValBAry() cannot be nil")
	}
	elemcnt := buftary.ElemAlloc()
	if !(idx < elemcnt) {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INSUFFSUBS))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INSUFFSUBS: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_INSUFFSUBS), errmsg}
	}
	cbuftary := buftary.getCPtr()
	if nil == cbuftary {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	elemptr := (*C.ydb_buffer_t)(unsafe.Pointer(uintptr(unsafe.Pointer(cbuftary)) + uintptr(C.sizeof_ydb_buffer_t*idx)))
	lenalloc := uint32((*elemptr).len_alloc)
	vallen := uint32(len(*value))
	if vallen > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INVSTRLEN))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
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
		panic("YDB: *BufferTArray receiver of SetValBAry() cannot be nil")
	}
	valuebary := []byte(*value)
	return buftary.SetValBAry(tptoken, errstr, idx, &valuebary)
}

// SetValStrLit is a method to set a string literal (value) into the buffer at the given index (idx).
func (buftary *BufferTArray) SetValStrLit(tptoken uint64, errstr *BufferT, idx uint32, value string) error {
	printEntry("BufferTArray.SetValStrLit()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of SetValBAry() cannot be nil")
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
// where elemUsed is zero, the method deletes all local variable trees.
//
// In the event that the elemUsed exceeds YDB_MAX_NAMES, the error return is ERRNAMECOUNT2HI.
//
// As M and Go application code cannot be mixed in the same process, the warning in ydb_delete_excl_s() does not apply.
func (buftary *BufferTArray) DeleteExclST(tptoken uint64, errstr *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("BufferTArray.DeleteExclST()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of DeleteExclST() cannot be nil")
	}
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_delete_excl_st(C.uint64_t(tptoken), cbuft, C.int(buftary.ElemUsed()),
		(*C.ydb_buffer_t)(unsafe.Pointer(buftary.getCPtr())))
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// TpST wraps ydb_tp_st() to implement transaction processing.
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
// The BufferTArray receiving the TpST() method is a list of local variables whose values should be saved, and restored to their
// original values when the transaction restarts. If the cbuftary structures have not been allocated or elemUsed is zero, no
// local variables are saved and restored; and if elemUsed is 1, and that sole element references the string "*" all local variables
// are saved and restored.
//
// A case-insensitive value of "BA" or "BATCH" for transid indicates to YottaDB that it need not ensure Durability for this
// transaction (it continues to ensure Atomicity, Consistency, and Isolation)
//
// Parameters:
//
// tptoken - the token used to identify nested transaction; start with yottadb.NOTTP
// errstr  - Buffer to hold error string that is used to report errors and avoid race conditions with setting $ZSTATUS.
// tpfn    - the closure function which will be run during the transaction. This closure function may get invoked multiple times
//           if a transaction fails for some reason (concurrent changes, for example), so should not change any data outside of
//           the database.
// transid - See docs for ydb_tp_s() in the MLPG.
func (buftary *BufferTArray) TpST(tptoken uint64, errstr *BufferT, tpfn func(uint64, *BufferT) int32, transid string) error {
	var cbuft *C.ydb_buffer_t

	printEntry("TpST()")
	if nil == buftary {
		panic("YDB: *BufferTArray receiver of TpST() cannot be nil")
	}
	tid := C.CString(transid)
	defer C.free(unsafe.Pointer(tid))
	tpfnparm := atomic.AddUint64(&tpIndex, 1)
	tpMap.Store(tpfnparm, tpfn)
	cbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*buftary).getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_tp_st(C.uint64_t(tptoken), cbuft, (C.ydb_tpfnptr_t)(C.ydb_tp_st_wrapper_cgo),
		unsafe.Pointer(&tpfnparm), tid, C.int((*buftary).ElemUsed()), cbuftary)
	tpMap.Delete(tpfnparm)
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// YdbTpStWrapper is a private callback to wrap calls to the Go closure required for TpST.
//export ydbTpStWrapper
func ydbTpStWrapper(tptoken uint64, errstr *C.ydb_buffer_t, tpfnparm unsafe.Pointer) int32 {
	var errbuff BufferT

	index := *((*uint64)(tpfnparm))
	v, ok := tpMap.Load(index)
	if !ok {
		panic("YDB: Could not find callback routine")
	}
	errbuff.BufferTFromPtr((unsafe.Pointer)(errstr))
	return (v.(func(uint64, *BufferT) int32))(tptoken, &errbuff)
}

// getCPtr returns a pointer to the internal C.ydb_buffer_t
func (buftary *BufferTArray) getCPtr() *C.ydb_buffer_t {
	ptr := (*C.ydb_buffer_t)(nil)
	if nil != buftary && nil != buftary.cbuftary {
		ptr = (*C.ydb_buffer_t)(unsafe.Pointer(buftary.cbuftary.cbuftary))
	}
	return ptr
}
