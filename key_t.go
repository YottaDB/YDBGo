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

// #include "libyottadb.h"
// #include "libydberrors.h"
import "C"

// KeyT defines a database key including varname and optional subscripts.
type KeyT struct {
	Varnm  BufferT
	Subary BufferTArray
}

// Alloc() is a STAPI method to allocate both pieces of the KeyT according to the supplied parameters.
// Parameters:
//   varSiz  - Length of buffer for varname (current var max is 31).
//   numSubs - Number of subscripts to supply (current subscript max is 31).
//   subSiz  - Length of the buffers for subscript values.
func (key *KeyT) Alloc(varSiz, numSubs, subSiz uint32) {
	printEntry("KeyT.Alloc()")
	(&((*key).Varnm)).Alloc(varSiz)
	(&((*key).Subary)).Alloc(numSubs, subSiz)
}

// Dump() is a STAPI method to dump the contents of the KeyT structure.
func (key *KeyT) Dump() {
	printEntry("KeyT.Dump()")
	(&((*key).Varnm)).Dump()
	(&((*key).Subary)).Dump()
}

// Free() is a STAPI method to free both pieces of the KeyT structure.
func (key *KeyT) Free() {
	printEntry("KeyT.Free()")
	(&((*key).Varnm)).Free()
	(&((*key).Subary)).Free()
}

// DataST() is a STAPI method to determine the status of a given node and its successors.
func (key *KeyT) DataST(tptoken uint64) (uint32, error) {
	var retval C.uint

	printEntry("KeyT.DataST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_data_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed), subbuftary,
		&retval)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return 0xffffffff, err
	}
	return uint32(retval), nil
}

// DeleteST() is a STAPI method to delete a node and perhaps its successors depending on the value of deltype. If
// deltype is C.YDB_DEL_NODE, only the given node is deleted if it exists. If the value instead is
// C.YDB_DEL_TREE, then the tree starting at the given node is removed.
func (key *KeyT) DeleteST(tptoken uint64, deltype int) error {
	printEntry("KeyT.DeleteST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_delete_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed), subbuftary,
		C.int(deltype))
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// ValST() is a STAPI method to fetch the given node returning its value in retval.
func (key *KeyT) ValST(tptoken uint64, retval *BufferT) error {
	printEntry("KeyT.ValST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_get_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed), subbuftary,
		(*retval).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	// Returned string should be snug in the retval buffer
	return nil
}

// IncrST() is a STAPI method to increment a given node and return the new value.
func (key *KeyT) IncrST(tptoken uint64, incr, retval *BufferT) error {
	var incrcbuft unsafe.Pointer

	printEntry("KeyT.IncrST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	if nil == incr {
		incrcbuft = nil
	} else {
		incrcbuft = unsafe.Pointer((*incr).cbuft)
	}
	rc := C.ydb_incr_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed), subbuftary,
		(*C.ydb_buffer_t)(incrcbuft),
		(*retval).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	// Returned string should be snug in the retval buffer
	return nil
}

// LockDecrST() is a STAPI method to decrement the lock-count of a given lock node.
func (key *KeyT) LockDecrST(tptoken uint64) error {
	printEntry("KeyT.LockDecrST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_lock_decr_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed), subbuftary)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// LockInsrST() is a STAPI method to increment the lock-count of a given node lock with the given timeout in nano-seconds.
func (key *KeyT) LockIncrST(tptoken uint64, timeoutNsec uint64) error {
	printEntry("KeyT.LockIncrST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_lock_incr_st(C.uint64_t(tptoken), C.ulonglong(timeoutNsec), vargobuft,
		C.int((*subgobuftary).elemsUsed), subbuftary)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// NodeNextST() is a STAPI method to return the next subscripted node for the given global - the node logically following the
// specified node (returns *BufferTArray).
func (key *KeyT) NodeNextST(tptoken uint64, next *BufferTArray) error {
	printEntry("KeyT.NodeNextST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_node_next_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed), subbuftary,
		(*C.int)(unsafe.Pointer(&((*next).elemsUsed))), (*C.ydb_buffer_t)(unsafe.Pointer((*next).cbuftary)))
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	if C.YDB_NODE_END == (*next).elemsUsed {
		// We reached the end of the list - return NODEEND error
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_NODEEND))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching NODEEND: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_NODEEND), errmsg}
	}
	// Returned node should be snug in the 'next' buffer array
	return nil
}

// NodePrevST() is a STAPI method to return the previous subscripted node for the given global - the node logically previous
// to the specified node (returns *BufferTArray).
func (key *KeyT) NodePrevST(tptoken uint64, prev *BufferTArray) error {
	printEntry("KeyT.NodePrevST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_node_previous_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed),
		subbuftary, (*C.int)(unsafe.Pointer(&((*prev).elemsUsed))), (*C.ydb_buffer_t)(unsafe.Pointer((*prev).cbuftary)))
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	if C.YDB_NODE_END == (*prev).elemsUsed {
		// We reached the end of the list - return NODEEND error
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_NODEEND))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching NODEEND: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_NODEEND), errmsg}
	}
	// Returned node should be snug in the 'prev' buffer array
	return nil
}

// SetValST() is a STAPI method to set the given value into the given node (glvn or SVN).
func (key *KeyT) SetValST(tptoken uint64, value *BufferT) error {
	printEntry("KeyT.SetValST()")
	cbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*key).Subary.cbuftary))
	rc := C.ydb_set_st(C.uint64_t(tptoken), (*key).Varnm.cbuft, C.int((*key).Subary.elemsUsed), cbuftary,
		(*value).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// SubNextST() is a STAPI method to return the next subscript following the specified node.
func (key *KeyT) SubNextST(tptoken uint64, retval *BufferT) error {
	printEntry("KeyT.SubNextST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_subscript_next_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed),
		subbuftary, (*retval).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	if 0 == ((*retval).cbuft).len_used {
		// We reached the end of the list - return NODEEND error
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_NODEEND))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching NODEEND: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_NODEEND), errmsg}
	}
	// Returned string should be snug in the retval buffer
	return nil
}

// SubPrevST() is a STAPI method to return the previous subscript following the specified node.
func (key *KeyT) SubPrevST(tptoken uint64, retval *BufferT) error {
	printEntry("KeyT.SubPrevST()")
	vargobuft := (&((*key).Varnm)).cbuft
	subgobuftary := &((*key).Subary)
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer((*subgobuftary).cbuftary))
	rc := C.ydb_subscript_previous_st(C.uint64_t(tptoken), vargobuft, C.int((*subgobuftary).elemsUsed),
		subbuftary, (*retval).cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	if 0 == ((*retval).cbuft).len_used {
		// We reached the end of the list - return NODEEND error
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_NODEEND))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching NODEEND: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_NODEEND), errmsg}
	}
	// Returned string should be snug in the retval buffer
	return nil
}
