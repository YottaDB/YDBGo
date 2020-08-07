//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2020 YottaDB LLC and/or its subsidiaries.	//
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
	"sync/atomic"
	"unsafe"
)

// #include <stdlib.h>
// #include <string.h>
// #include "libyottadb.h"
import "C"

// BufferT is a Go structure that serves as an anchor point for a C allocated ydb_buffer_t structure used
// to call the YottaDB C Simple APIs. Because this structure's contents contain pointers to C allocated storage,
// this structure is NOT safe for concurrent access.
type BufferT struct {
	cbuft    *internalBufferT // Allows BufferT to be copied via assignment without causing double free problems
	ownsBuff bool             // If true, we should clean the cbuft when Free'd
}

type internalBufferT struct {
	cbuft *C.ydb_buffer_t // C flavor of the ydb_buffer_t struct
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Data manipulation methods for BufferT
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// bufferTFromPtr sets this BufferT internal structure to point to the given buffer.
//
// Intended for use by functions implementing transaction logic, the method sets cbuft in the BufferT structure to errstr.
//
// Note: Modifying errstr, or accessing memory it references may lead to code that behaves unpredictably and is hard to debug. Always
// "wrap" it using bufferTFromPtr() and use the methods for the BufferT structure.
func (buft *BufferT) bufferTFromPtr(pointer unsafe.Pointer) {
	if nil == buft {
		panic("YDB: *BufferT receiver of BufferTFromPtr() cannot be nil")
	}
	// Note that we don't set a Finalizer here because another process has already done
	//  so under a different BufferT; the lifespan of this object must be a subset of that
	//  object
	buft.cbuft = &internalBufferT{(*C.ydb_buffer_t)(pointer)}
	buft.ownsBuff = false
}

// Alloc is a method to allocate the ydb_buffer_t C storage and allocate or re-allocate the buffer pointed
// to by that struct.
//
// It allocates a buffer in YottaDB heap space of size nBytes; and a C.ydb_buffer_t structure, also in YottaDB heap space, with
// its buf_addr referencing the buffer, its len_alloc set to nBytes and its len_used set to zero. Set cbuft in the BufferT
// structure to reference the C.ydb_buffer_t structure.
func (buft *BufferT) Alloc(nBytes uint32) {
	var cbuftptr *C.ydb_buffer_t

	printEntry("BufferT.Alloc()")
	if nil == buft {
		panic("YDB: *BufferT receiver of Alloc() cannot be nil")
	}

	// Allocate a C flavor ydb_buffer_t struct to pass to simpleAPI
	buft.cbuft = &internalBufferT{(*C.ydb_buffer_t)(allocMem(C.size_t(C.sizeof_ydb_buffer_t)))}
	cbuftptr = buft.getCPtr()
	cbuftptr.len_used = 0
	cbuftptr.len_alloc = C.uint(nBytes)
	cbuftptr.buf_addr = nil
	// Allocate a new buffer of the given size; if size is 0, we just leave it as nil
	if 0 < nBytes {
		cbuftptr.buf_addr = (*C.char)(allocMem(C.size_t(nBytes)))
	}
	buft.ownsBuff = true
	// Set a finalizer
	runtime.SetFinalizer(buft.cbuft, func(o *internalBufferT) {
		o.Free()
	})
}

// Dump is a method to dump the contents of a BufferT block for debugging purposes.
//
// For debugging purposes, dump on stdout:
//
// - cbuft as a hexadecimal address;
//
// - for the C.ydb_buffer_t structure referenced by cbuft: buf_addr as a hexadecimal address, and len_alloc and len_used as integers; and
//
// - at the address buf_addr, the lower of len_used or len_alloc bytes in zwrite format.
func (buft *BufferT) Dump() {
	if nil == buft {
		panic("YDB: *BufferT receiver of Dump() cannot be nil")
	}
	buft.DumpToWriter(os.Stdout)
}

// DumpToWriter dumps a textual representation of this buffer to the writer
func (buft *BufferT) DumpToWriter(writer io.Writer) {
	printEntry("BufferT.Dump()")
	if nil == buft {
		panic("YDB: *BufferT receiver of DumpToWriter() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	fmt.Fprintf(writer, "BufferT.Dump(): cbuftptr: %p", cbuftptr)
	if nil != cbuftptr {
		fmt.Fprintf(writer, ", buf_addr: %v, len_alloc: %v, len_used: %v", cbuftptr.buf_addr,
			cbuftptr.len_alloc, cbuftptr.len_used)
		// It is possible len_used is greater than len_alloc (if this buffer was populated by SimpleAPI C code)
		// Ensure we do not overrun the allocated buffer while dumping this object in that case.
		min := cbuftptr.len_used
		if min > cbuftptr.len_alloc {
			min = cbuftptr.len_alloc
		}
		if 0 < min {
			strval := C.GoStringN(cbuftptr.buf_addr, C.int(min))
			fmt.Fprintf(writer, ", value: %s", strval)
		}
	}
	fmt.Fprintf(writer, "\n")
	runtime.KeepAlive(buft) // Make sure buft hangs around through the YDB call
}

// Free is a method to release both the buffer and ydb_buffer_t block associated with the BufferT block.
//
// The inverse of the Alloc() method: release the buffer in YottaDB heap space referenced by the C.ydb_buffer_t structure,
// release the C.ydb_buffer_t, and set cbuft in the BufferT structure to nil.
func (buft *BufferT) Free() {
	if nil == buft {
		return
	}
	if buft.ownsBuff {
		buft.cbuft.Free()
	}
	buft.cbuft = nil
}

// Calls freeMem on any C memory owned by this internalBuffer
func (ibuft *internalBufferT) Free() {
	printEntry("internalBufferT.Free()")
	if nil == ibuft {
		return
	}
	cbuftptr := ibuft.cbuft
	if nil != cbuftptr {
		// ydb_buffer_t block exists - free its buffer first if it exists
		if nil != cbuftptr.buf_addr {
			freeMem(unsafe.Pointer(cbuftptr.buf_addr), C.size_t(cbuftptr.len_alloc))
		}
		freeMem(unsafe.Pointer(cbuftptr), C.sizeof_ydb_buffer_t)
		// The below keeps ibuft around long enough to get rid of this block's C memory. No KeepAlive() necessary.
		ibuft.cbuft = nil
	}
}

// getCPtr returns a pointer to the internal ydb_buffer_t
func (buft *BufferT) getCPtr() *C.ydb_buffer_t {
	ptr := (*C.ydb_buffer_t)(nil)
	if nil != buft && nil != buft.cbuft {
		ptr = buft.cbuft.cbuft
	}
	return ptr
}

// LenAlloc is a method to fetch the ydb_buffer_t.len_alloc field containing the allocated length of the buffer.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// Otherwise, return the len_alloc field of the C.ydb_buffer_t structure referenced by cbuft.
func (buft *BufferT) LenAlloc(tptoken uint64, errstr *BufferT) (uint32, error) {
	printEntry("BufferT.LenAlloc()")
	if nil == buft {
		panic("YDB: *BufferT receiver of LenAlloc() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	retval := uint32(cbuftptr.len_alloc)
	runtime.KeepAlive(buft)
	return retval, nil
}

// LenUsed is a method to fetch the ydb_buffer_t.len_used field containing the used length of the buffer. Note
// that if len_used > len_alloc, thus indicating a previous issue, an INVSTRLEN error is raised.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// Otherwise, return the len_used field of the C.ydb_buffer_t structure referenced by cbuft.
func (buft *BufferT) LenUsed(tptoken uint64, errstr *BufferT) (uint32, error) {
	printEntry("BufferT.LenUsed()")
	if nil == buft {
		panic("YDB: *BufferT receiver of LenUsed() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return 0, &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenused := cbuftptr.len_used
	runtime.KeepAlive(buft)
	return uint32(lenused), nil
}

// ValBAry is a method to fetch the buffer contents as a byte array.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If the len_used field of the C.ydb_buffer_t structure is greater than its len_alloc field (owing to a prior
// INVSTRLEN error), return an INVSTRLEN error. Otherwise, return len_used bytes of the buffer as a byte array.
func (buft *BufferT) ValBAry(tptoken uint64, errstr *BufferT) ([]byte, error) {
	var bary []byte

	printEntry("BufferT.ValBAry()")
	if nil == buft {
		panic("YDB: *BufferT receiver of ValBAry() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return nil, &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := cbuftptr.len_alloc
	lenused := cbuftptr.len_used
	cbufptr := cbuftptr.buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		bary = C.GoBytes(unsafe.Pointer(cbufptr), C.int(lenalloc)) // Return what we can (alloc size)
		errmsg := formatINVSTRLEN(tptoken, errstr, lenalloc, lenused)
		return bary, &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
	}
	// The entire buffer is there so return that.
	bary = C.GoBytes(unsafe.Pointer(cbufptr), C.int(lenused))
	runtime.KeepAlive(buft) // Make sure buft hangs around
	return bary, nil
}

// ValStr is a method to fetch the buffer contents as a string.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If the len_used field of the C.ydb_buffer_t structure is greater than its len_alloc field (owing to a prior
// INVSTRLEN error), return an INVSTRLEN error. Otherwise, return len_used bytes of the buffer as a string.
func (buft *BufferT) ValStr(tptoken uint64, errstr *BufferT) (string, error) {
	var str string

	printEntry("BufferT.ValStr()")
	if nil == buft {
		panic("YDB: *BufferT receiver of ValStr() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return "", &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := cbuftptr.len_alloc
	lenused := cbuftptr.len_used
	cbufptr := cbuftptr.buf_addr
	if lenused > lenalloc { // INVSTRLEN from last operation - return what we can and give error
		str = C.GoStringN(cbufptr, C.int(lenalloc)) // Return what we can (alloc size)
		errmsg := formatINVSTRLEN(tptoken, errstr, lenalloc, lenused)
		return str, &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
	}
	// The entire buffer is there so return that
	str = C.GoStringN(cbufptr, C.int(lenused))
	runtime.KeepAlive(buft) // Make sure buft hangs around
	return str, nil
}

// SetLenUsed is a method to set the used length of buffer in the ydb_buffer_t block (must be <= alloclen).
//
// Use this method to change the length of a used substring of the contents of the buffer referenced by the buf_addr field of the
// referenced C.ydb_buffer_t.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If newLen is greater than the len_alloc field of the referenced C.ydb_buffer_t, make no changes and return with
// an error return of INVSTRLEN. Otherwise, set the len_used field of the referenced C.ydb_buffer_t to newLen.
//
// Note that even if newLen is not greater than the value of len_alloc, setting a len_used value greater than the
// number of meaningful bytes in the buffer will likely lead to hard-to-debug errors.
func (buft *BufferT) SetLenUsed(tptoken uint64, errstr *BufferT, newLen uint32) error {
	printEntry("BufferT.SetLenUsed()")
	if nil == buft {
		panic("YDB: *BufferT receiver of SetLenUsed() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	lenalloc := cbuftptr.len_alloc
	if newLen > uint32(lenalloc) {
		errmsg := formatINVSTRLEN(tptoken, errstr, lenalloc, C.uint(newLen))
		return &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
	}
	cbuftptr.len_used = C.uint(newLen)
	runtime.KeepAlive(buft) // Make sure buft hangs around
	return nil
}

// SetValBAry is a method to set a []byte array into the given buffer.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If the length of value is greater than the len_alloc field of the C.ydb_buffer_t structure referenced by
// cbuft, make no changes and return INVSTRLEN. Otherwise, copy the bytes of value to the location referenced
// by the buf_addr field of the C.ydbbuffer_t structure, set the len_used field to the length of value.
func (buft *BufferT) SetValBAry(tptoken uint64, errstr *BufferT, value []byte) error {
	printEntry("BufferT.SetValBAry()")
	if nil == buft {
		panic("YDB: *BufferT receiver of SetValBAry() cannot be nil")
	}
	cbuftptr := buft.getCPtr()
	if nil == cbuftptr {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	vallen := C.uint(len(value))
	lenalloc := cbuftptr.len_alloc
	if vallen > lenalloc {
		errmsg := formatINVSTRLEN(tptoken, errstr, lenalloc, vallen)
		return &YDBError{(int)(YDB_ERR_INVSTRLEN), errmsg}
	}
	// Copy the Go buffer to the C buffer
	if 0 < vallen {
		C.memcpy(unsafe.Pointer(cbuftptr.buf_addr), unsafe.Pointer(&value[0]), C.size_t(vallen))
	}
	cbuftptr.len_used = vallen
	runtime.KeepAlive(buft) // Make sure buft hangs around
	return nil
}

// SetValStr is a method to set a string into the given buffer.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If the length of value is greater than the len_alloc field of the C.ydb_buffer_t structure referenced by
// cbuft, make no changes and return INVSTRLEN. Otherwise, copy the bytes of value to the location referenced
// by the buf_addr field of the C.ydbbuffer_t structure, set the len_used field to the length of value.
func (buft *BufferT) SetValStr(tptoken uint64, errstr *BufferT, value string) error {
	printEntry("BufferT.SetValStr()")
	if nil == buft {
		panic("YDB: *BufferT receiver of SetValStr() cannot be nil")
	}
	valuebary := []byte(value)
	return buft.SetValBAry(tptoken, errstr, valuebary)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Simple (Threaded) API methods for BufferT
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// Str2ZwrST is a STAPI method to take the given string and return it in ZWRITE format.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If len_alloc is not large enough, set len_used to the required length, and return an INVSTRLEN error. In this case,
// len_used will be greater than len_alloc until corrected by application code. Otherwise, set the buffer referenced by buf_addr
// to the zwrite format string, and set len_used to the length.
func (buft *BufferT) Str2ZwrST(tptoken uint64, errstr *BufferT, zwr *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("BufferT.Str2ZwrST()")
	if nil == buft {
		panic("YDB: *BufferT receiver of Str2ZwrST() cannot be nil")
	}
	if nil == zwr {
		panic("YDB: *BufferT 'zwr' parameter to Str2ZwrST() cannot be nil")
	}
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	if nil == buft.getCPtr() || nil == zwr.getCPtr() {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_str2zwr_st(C.uint64_t(tptoken), cbuft, buft.getCPtr(), zwr.getCPtr())
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	runtime.KeepAlive(buft)
	runtime.KeepAlive(errstr)
	runtime.KeepAlive(zwr)
	return nil
}

// Zwr2StrST is a STAPI method to take the given ZWRITE format string and return it as a normal ASCII string.
//
// If the C.ydb_buffer_t structure referenced by cbuft has not yet been allocated, return the STRUCTNOTALLOCD error.
// If len_alloc is not large enough, set len_used to the required length, and return an INVSTRLEN error. In this case,
// len_used will be greater than len_alloc until corrected by application code. If str has errors and is not in valid zwrite format, set
// len_used to zero, and return the error code returned by ydb_zwr2str_s() e.g., INVZWRITECHAR. Otherwise, set the buffer referenced
// by buf_addr to the unencoded string, set len_used to the length.
//
// Note that the length of a string in zwrite format is always greater than or equal to the string in its original, unencoded format.
func (buft *BufferT) Zwr2StrST(tptoken uint64, errstr *BufferT, str *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("BufferT.Zwr2StrST()")
	if nil == buft {
		panic("YDB: *BufferT receiver of Zwr2StrST() cannot be nil")
	}
	if nil == str {
		panic("YDB: *BufferT 'str' parameter to Zwr2StrST() cannot be nil")
	}
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	if nil == buft.getCPtr() || nil == str.getCPtr() {
		// Create an error to return
		errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_STRUCTNOTALLOCD))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching STRUCTNOTALLOCD: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_STRUCTNOTALLOCD), errmsg}
	}
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_zwr2str_st(C.uint64_t(tptoken), cbuft, buft.getCPtr(), str.getCPtr())
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	runtime.KeepAlive(buft)
	runtime.KeepAlive(errstr)
	runtime.KeepAlive(str)
	return nil
}
