//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2019 YottaDB LLC. and/or its subsidiaries.//
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
	"io"
	"os"
	"unsafe"
)

// #include "libyottadb.h"
import "C"

// KeyT defines a database key including varname and optional subscripts.
type KeyT struct {
	Varnm  *BufferT
	Subary *BufferTArray
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Data manipulation methods for KeyT
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// Alloc is a STAPI method to allocate both pieces of the KeyT according to the supplied parameters.
//
// Invoke Varnm.Alloc(varSiz) and SubAry.Alloc(numSubs, subSiz)
//
// Parameters:
//   varSiz  - Length of buffer for varname (current var max is 31).
//   numSubs - Number of subscripts to supply (current subscript max is 31).
//   subSiz  - Length of the buffers for subscript values.
func (key *KeyT) Alloc(varSiz, numSubs, subSiz uint32) {
	printEntry("KeyT.Alloc()")
	if nil == key {
		panic("*KeyT receiver of Alloc() cannot be nil")
	}
	var buffertary BufferTArray
	var buffer BufferT
	key.Varnm = &buffer
	key.Varnm.Alloc(varSiz)
	key.Subary = &buffertary
	key.Subary.Alloc(numSubs, subSiz)
}

// Dump is a STAPI method to dump the contents of the KeyT structure.
//
// Invoke Varnm.Dump() and SubAry.Dump().
func (key *KeyT) Dump() {
	printEntry("KeyT.Dump()")
	if nil == key {
		panic("*KeyT receiver of Dump() cannot be nil")
	}
	key.DumpToWriter(os.Stdout)
}

// DumpToWriter dumps a textual representation of this key to the writer.
func (key *KeyT) DumpToWriter(writer io.Writer) {
	if nil == key {
		panic("*KeyT receiver of DumpWriter() cannot be nil")
	}
	if key.Varnm != nil {
		key.Varnm.DumpToWriter(writer)
	}
	if key.Subary != nil {
		key.Subary.DumpToWriter(writer)
	}
}

// Free is a STAPI method to free both pieces of the KeyT structure.
//
// Invoke Varnm.Free() and SubAry.Free().
func (key *KeyT) Free() {
	printEntry("KeyT.Free()")
	if nil != key { // Ignore if no struct passed
		key.Varnm.Free()
		key.Subary.Free()
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Simple (Threaded) API methods for KeyT
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// DataST is a STAPI method to determine the status of a given node and its successors.
//
// Matching DataE(), DataST() returns the result of ydb_data_st(). In the event an error is returned, the return value
// is unspecified.
func (key *KeyT) DataST(tptoken uint64, errstr *BufferT) (uint32, error) {
	var retval C.uint
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.DataST()")
	if nil == key {
		panic("*KeyT receiver of DataST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_data_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()), subbuftary,
		&retval)
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return 0, err
	}
	return uint32(retval), nil
}

// DeleteST is a STAPI method to delete a node and perhaps its successors depending on the value of deltype.
//
// Matching DeleteE(), DeleteST() wraps ydb_delete_st() to delete a local or global variable node or (sub)tree, with a value of
// C.YDB_DEL_NODE for deltype specifying that only the node should be deleted, leaving the (sub)tree untouched, and a value
// of C.YDB_DEL_TREE specifying that the node as well as the (sub)tree are to be deleted.
func (key *KeyT) DeleteST(tptoken uint64, errstr *BufferT, deltype int) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.DeleteST()")
	if nil == key {
		panic("*KeyT receiver of DeleteST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_delete_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()), subbuftary,
		C.int(deltype))
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// ValST is a STAPI method to fetch the given node returning its value in retval.
//
// Matching ValE(), ValST() wraps ydb_get_st() to return the value at the referenced global or local variable node, or
// intrinsic special variable, in the buffer referenced by the BufferT structure referenced by retval.
//
// If ydb_get_st() returns an error such as GVUNDEF, INVSVN, LVUNDEF, the method makes no changes to the structures under retval
// and returns the error. If the length of the data to be returned exceeds retval.getLenAlloc(), the method sets the len_used` of
// the C.ydb_buffer_t referenced by retval to the required length, and returns an INVSTRLEN error. Otherwise, it copies the data
// to the buffer referenced by the retval.buf_addr, and sets retval.lenUsed to its length.
func (key *KeyT) ValST(tptoken uint64, errstr *BufferT, retval *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.ValST()")
	if nil == key {
		panic("*KeyT receiver of ValST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_get_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()), subbuftary,
		retval.getCPtr())
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	// Returned string should be snug in the retval buffer
	return nil
}

// IncrST is a STAPI method to increment a given node and return the new value.
//
// Matching IncrE(), IncrST() wraps ydb_incr_st() to atomically increment the referenced global or local variable node
// coerced to a number, with incr coerced to a number. It stores the result in the node and also returns it through
// the BufferT structure referenced by retval.
//
// If ydb_incr_st() returns an error such as NUMOFLOW, INVSTRLEN, the method makes no changes to the structures under retval and
// returns the error. If the length of the data to be returned exceeds retval.lenAlloc, the method sets the len_used
// of the C.ydb_buffer_t referenced by retval to the required length, and returns an INVSTRLEN error.
// Otherwise, it copies the data to the buffer referenced by the retval.buf_addr, sets retval.lenUsed to its length.
//
// With a nil value for incr, the default increment is 1. Note that the value of the empty string coerced to an integer is zero.
func (key *KeyT) IncrST(tptoken uint64, errstr *BufferT, incr, retval *BufferT) error {
	var incrcbuft unsafe.Pointer
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.IncrST()")
	if nil == key {
		panic("*KeyT receiver of IncrST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if nil == incr {
		incrcbuft = nil
	} else {
		incrcbuft = unsafe.Pointer(incr.getCPtr())
	}
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_incr_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()), subbuftary,
		(*C.ydb_buffer_t)(incrcbuft),
		retval.getCPtr())
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	// Returned string should be snug in the retval buffer
	return nil
}

// LockDecrST is a STAPI method to decrement the lock-count of a given lock node.
//
// Matching LockDecrE(), LockDecrST() wraps ydb_lock_decr_st() to decrement the count of the lock name
// referenced, releasing it if the count goes to zero or ignoring the invocation if the process does not hold the lock.
func (key *KeyT) LockDecrST(tptoken uint64, errstr *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.LockDecrST()")
	if nil == key {
		panic("*KeyT receiver of LockDecrST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_lock_decr_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()), subbuftary)
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// LockIncrST is a STAPI method to increment the lock-count of a given node lock with the given timeout in nano-seconds.
//
// Matching LockIncrE(), LockIncrST() wraps ydb_lock_incr_st() to attempt to acquire the referenced lock
// resource name without releasing any locks the process already holds.
//
// If the process already holds the named lock resource, the method increments its count and returns.
// If timeoutNsec exceeds C.YDB_MAX_TIME_NSEC, the method returns with an error return TIME2LONG.
// If it is able to aquire the lock resource within timeoutNsec nanoseconds, it returns holding the lock, otherwise it returns
// LOCK_TIMEOUT. If timeoutNsec is zero, the method makes exactly one attempt to acquire the lock.
func (key *KeyT) LockIncrST(tptoken uint64, errstr *BufferT, timeoutNsec uint64) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.LockIncrST()")
	if nil == key {
		panic("*KeyT receiver of LockIncrST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_lock_incr_st(C.uint64_t(tptoken), cbuft, C.ulonglong(timeoutNsec), vargobuft,
		C.int(subgobuftary.ElemUsed()), subbuftary)
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// NodeNextST is a STAPI method to return the next subscripted node for the given global - the node logically following the
// specified node (returns *BufferTArray).
//
// Matching NodeNextE(), NodeNextST() wraps ydb_node_next_st() to facilitate depth first traversal of a local or global variable tree.
//
// If there is a next node:
//
// If the number of subscripts of that next node exceeds next.elemsAlloc, the method sets next.elemsUsed to
// the number of subscripts required, and returns an INSUFFSUBS error. In this case the elemsUsed is greater than elemsAlloc.
// If one of the C.ydb_buffer_t structures referenced by next (call the first or only element n) has insufficient space for
// the corresponding subscript, the method sets next.elemsUsed to n, and the len_alloc of that C.ydb_buffer_t structure to the actual space
// required. The method returns an INVSTRLEN error. In this case the len_used of that structure is greater than its len_alloc.
// Otherwise, it sets the structure next to reference the subscripts of that next node, and next.elemsUsed to the number of subscripts.
//
// If the node is the last in the tree, the method returns the NODEEND error, making no changes to the structures below next.
func (key *KeyT) NodeNextST(tptoken uint64, errstr *BufferT, next *BufferTArray) error {
	var nextElemsPtr *uint32
	var dummyElemUsed uint32
	var nextSubaryPtr *C.ydb_buffer_t
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.NodeNextST()")
	if nil == key {
		panic("*KeyT receiver of NodeNextST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	// The output buffer does not need to be allocated at this point though it may error in ydb_node_next_s() if not.
	if nil != next {
		next.cbuftary.elemsUsed = next.ElemAlloc() // Set all elements of output array available for output
		nextElemsPtr = &next.cbuftary.elemsUsed
		nextSubaryPtr = next.getCPtr()
	} else {
		nextElemsPtr = &dummyElemUsed
		nextSubaryPtr = nil
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_node_next_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()), subbuftary,
		(*C.int)(unsafe.Pointer(nextElemsPtr)), (*C.ydb_buffer_t)(unsafe.Pointer(nextSubaryPtr)))
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// NodePrevST is a STAPI method to return the previous subscripted node for the given global - the node logically previous
// to the specified node (returns *BufferTArray).
//
// Matching NodePrevE(), NodePrevST() wraps ydb_node_previous_st() to facilitate reverse depth first traversal of a local or global variable tree.
//
// If there is a previous node:
//
// If the number of subscripts of that previous node exceeds prev.elemsAlloc, the method sets prev.elemsUsed to
// the number of subscripts required, and returns an INSUFFSUBS error. In this case the elemsUsed is greater than elemsAlloc.
// If one of the C.ydb_buffer_t structures referenced by prev (call the first or only element n) has insufficient space for
// the corresponding subscript, the method sets prev.elemsUsed to n, and the len_alloc of that C.ydb_buffer_t structure to the actual space
// required. The method returns an INVSTRLEN error. In this case the len_used of that structure is greater than its len_alloc.
// Otherwise, it sets the structure prev to reference the subscripts of that prev node, and prev.elemsUsed to the number of subscripts.
//
// If the node is the first in the tree, the method returns the NODEEND error making no changes to the structures below prev.
func (key *KeyT) NodePrevST(tptoken uint64, errstr *BufferT, prev *BufferTArray) error {
	var prevElemsPtr *uint32
	var dummyElemUsed uint32
	var prevSubaryPtr *C.ydb_buffer_t
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.NodePrevST()")
	if nil == key {
		panic("*KeyT receiver of NodePrevST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	// The output buffer does not need to be allocated at this point though it may error in ydb_node_previous_s() if not.
	if nil != prev {
		prev.cbuftary.elemsUsed = prev.ElemAlloc() // Set all elements of output array available for output
		prevElemsPtr = &prev.cbuftary.elemsUsed
		prevSubaryPtr = prev.getCPtr()
	} else {
		prevElemsPtr = &dummyElemUsed
		prevSubaryPtr = nil
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_node_previous_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()),
		subbuftary, (*C.int)(unsafe.Pointer(prevElemsPtr)), (*C.ydb_buffer_t)(unsafe.Pointer(prevSubaryPtr)))
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// SetValST is a STAPI method to set the given value into the given node (glvn or SVN).
//
// Matching SetE(), at the referenced local or global variable node, or the intrinsic special variable, SetValST() wraps
// ydb_set_st() to set the value specified by val.
func (key *KeyT) SetValST(tptoken uint64, errstr *BufferT, value *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.SetValST()")
	if nil == key {
		panic("*KeyT receiver of SetValST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	cbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(key.Subary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_set_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(key.Subary.ElemUsed()), cbuftary,
		value.getCPtr())
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// SubNextST is a STAPI method to return the next subscript following the specified node.
//
// Matching SubNextE(), SubNextST() wraps ydb_subscript_next_st() to facilitate breadth-first traversal of a
// local or global variable sub-tree.
//
// At the level of the last subscript, if there is a next subscript with a node and/or a subtree:
//
// If the length of that next subscript exceeds sub.len_alloc, the method sets sub.len_used to the
// actual length of that subscript, and returns an INVSTRLEN error. In this case sub.len_used is greater than
// sub.len_alloc. Otherwise, it copies that subscript to the buffer referenced by
// sub.buf_addr, and sets sub.len_used to its length.
//
// If there is no next node or subtree at that level of the subtree, the method returns the NODEEND error.
func (key *KeyT) SubNextST(tptoken uint64, errstr *BufferT, retval *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.SubNextST()")
	if nil == key {
		panic("*KeyT receiver of SubNextST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_subscript_next_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()),
		subbuftary, retval.getCPtr())
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}

// SubPrevST is a STAPI method to return the previous subscript following the specified node.
//
// SubPrevST() wraps ydb_subscript_previous_st() to facilitate reverse breadth-first traversal of a local or global variable sub-tree.
//
// At the level of the last subscript, if there is a previous subscript with a node and/or a subtree:
//
// If the length of that previous subscript exceeds sub.len_alloc, the method sets sub.len_used to the
// actual length of that subscript, and returns an INVSTRLEN error. In this case sub.len_used is greater than
// sub.len_alloc. Otherwise, it copies that subscript to the buffer referenced by sub.buf_addr, and sets buf.len_used to its length.
//
// If there is no previous node or subtree at that level of the subtree, the method returns the NODEEND error.
func (key *KeyT) SubPrevST(tptoken uint64, errstr *BufferT, retval *BufferT) error {
	var cbuft *C.ydb_buffer_t

	printEntry("KeyT.SubPrevST()")
	if nil == key {
		panic("*KeyT receiver of SubPrevST() cannot be nil")
	}
	vargobuft := key.Varnm.getCPtr()
	if (nil == vargobuft) || (nil == vargobuft.buf_addr) || (0 == vargobuft.len_used) {
		panic("KeyT varname is not allocated, is nil, or has a 0 length")
	}
	subgobuftary := key.Subary
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(subgobuftary.getCPtr()))
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_subscript_previous_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(subgobuftary.ElemUsed()),
		subbuftary, retval.getCPtr())
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	return nil
}
