//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2022 YottaDB LLC and/or its subsidiaries.	//
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
	"strings"
	"sync/atomic"
)

// #include "libyottadb.h"
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Functions making up the EasyAPI
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// DataE is a STAPI function to return $DATA() value for a given variable subscripted or not.
//
// Matching DataST(), DataE() function wraps and returns the result of ydb_data_st(). In the event of an error, the return
// value is unspecified.
func DataE(tptoken uint64, errstr *BufferT, varname string, subary []string) (uint32, error) {
	var retval C.uint
	var dbkey KeyT
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("DataE()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	defer dbkey.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	vargobuft := dbkey.Varnm.getCPtr()
	subbuftary := dbkey.Subary.getCPtr()
	rc := C.ydb_data_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(dbkey.Subary.ElemUsed()), subbuftary, &retval)
	if YDB_OK != rc {
		err = NewError(tptoken, errstr, int(rc))
		return uint32(retval), err
	}
	runtime.KeepAlive(dbkey) // Make sure dbkey stays in tact through the call into YDB
	runtime.KeepAlive(errstr)
	return uint32(retval), nil
}

// DeleteE is a STAPI function to delete a node or a subtree (see DeleteST) given a deletion type and a varname/subscript set.
//
// Matching DeleteST(), DeleteE() wraps ydb_delete_st() to
// delete a local or global variable node or (sub)tree, with a value of
// YDB_DEL_NODE for deltype specifying that only the node should be deleted, leaving the (sub)tree untouched, and a value
// of YDB_DEL_TREE specifying that the node as well as the(sub)tree are to be deleted.
func DeleteE(tptoken uint64, errstr *BufferT, deltype int, varname string, subary []string) error {
	var dbkey KeyT
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("DeleteE()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	defer dbkey.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	vargobuft := dbkey.Varnm.getCPtr()
	subbuftary := dbkey.Subary.getCPtr()
	rc := C.ydb_delete_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(dbkey.Subary.ElemUsed()), subbuftary,
		C.int(deltype))
	if YDB_OK != rc {
		err = NewError(tptoken, errstr, int(rc))
		return err
	}
	runtime.KeepAlive(dbkey) // Make sure dbkey stays in tact through the call into YDB
	runtime.KeepAlive(errstr)
	return nil
}

// DeleteExclE is a STAPI function to do an exclusive delete by deleting all local variables except those root vars specified
// in the variable name array. If the varname array is empty, all local variables are deleted.
//
// Matching DeleteExclST(), DeleteExclE() wraps ydb_delete_excl_st() to delete all local variables except those
// specified. In the event varnames has no elements (i.e.,[]string{}), DeleteExclE() deletes all local variables.
//
// In the event that the number of variable names in varnames exceeds YDB_MAX_NAMES, the error return is
// ERRNAMECOUNT2HI. Otherwise, if ydb_delete_excl_st() returns an error, the function returns the error.
//
// As M and Go application code cannot be mixed in the same process, the warning in ydb_delete_excl_s() does not apply.
func DeleteExclE(tptoken uint64, errstr *BufferT, varnames []string) error {
	var vnames BufferTArray
	var maxvarnmlen, varnmcnt, varnmlen uint32
	var i int
	var err error
	var varname string

	printEntry("DeleteExclE()")
	defer vnames.Free()
	varnmcnt = uint32(len(varnames))
	// Find maximum length varname so know how much to allocate
	maxvarnmlen = 0
	for _, varname = range varnames {
		varnmlen = uint32(len(varname))
		if varnmlen > maxvarnmlen {
			maxvarnmlen = varnmlen
		}
	}
	vnames.Alloc(varnmcnt, maxvarnmlen)
	for i, varname = range varnames {
		err = vnames.SetValStr(tptoken, errstr, uint32(i), varname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	err = vnames.SetElemUsed(tptoken, errstr, varnmcnt)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetUsed(): %s", err))
	}
	// Drive simpleAPI wrapper and return its return code
	return vnames.DeleteExclST(tptoken, errstr)
}

// ValE is an STAPI function to return the value found for varname(subary...)
//
// Matching ValST(), ValE() wraps ydb_get_st() to return
// the value at the referenced global or local variable node, or intrinsic special variable.
//
// If ydb_get_s() returns an error such as GVUNDEF, INVSVN, LVUNDEF,
// the function returns the error. Otherwise, it returns the value at the node.
func ValE(tptoken uint64, errstr *BufferT, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbvalue BufferT
	var err error
	var dataSize uint32

	printEntry("ValE()")
	defer dbkey.Free()
	defer dbvalue.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	dataSize = easyAPIDefaultDataSize
	dbvalue.Alloc(dataSize)
	// Attempt to fetch the value multiple times. We do not know how big the incoming record is so loop till it fits.
	for {
		// dbvalue is allocated with current best-guess size of returning data
		err = dbkey.ValST(tptoken, errstr, &dbvalue)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - reallocate the size we need
				dataSize = uint32(dbvalue.getCPtr().len_used)
				dbvalue.Free()
				dbvalue.Alloc(dataSize)
				continue
			}
			// Otherwise an unexpected error occurred. Return that.
			return "", err
		}
		break // No error so success and we are done!
	}
	retval, err := dbvalue.ValStr(tptoken, errstr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
	}
	return retval, nil
}

// IncrE is a STAPI function to increment the given value by the given amount and return the new value.
//
// Matching IncrST(), IncrE() wraps ydb_incr_st() to atomically increment the referenced global or local variable node
// coerced to a number with incr coerced to a number, with the result stored in the node and returned by the function.
//
// If ydb_incr_st() returns an error such as NUMOFLOW or INVSTRLEN, the function returns the error. Otherwise, it returns the incremented value of the node.
//
// With a nil value for incr, the default increment is 1. Note that the value of the empty string coerced to an integer is zero.
func IncrE(tptoken uint64, errstr *BufferT, incr, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbvalue, incrval BufferT
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("IncrE()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	defer dbkey.Free()
	defer dbvalue.Free()
	defer incrval.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	dbvalue.Alloc(easyAPIDefaultDataSize)
	incrval.Alloc(uint32(len(incr)))
	err = incrval.SetValStr(tptoken, errstr, incr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	// Since the return value is only checked to see if it is big enough AFTER the increment
	// is done, we cannot repeat the operation with a larger buffer (or the value would be incremented
	// again) so for this call, whatever happens just happens though the default buffer should be
	// large enough for any reasonable value being incremented.
	vargobuft := dbkey.Varnm.getCPtr()
	subbuftary := dbkey.Subary.getCPtr()
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_incr_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(dbkey.Subary.ElemUsed()), subbuftary,
		incrval.getCPtr(), dbvalue.getCPtr())
	if YDB_OK != rc {
		err = NewError(tptoken, errstr, int(rc))
		return "", err
	}
	retval, err := dbvalue.ValStr(tptoken, errstr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
	}
	runtime.KeepAlive(dbkey) // Make sure dbkey and incrval stays in tact through the call into YDB
	runtime.KeepAlive(incrval)
	return retval, nil
}

// LockE is a STAPI function whose purpose is to release all locks and then lock the locks designated. The variadic list
// is pairs of arguments with the first being a string containing the variable name and the second being a string array
// containing the subscripts, if any, for that variable (null list for no subscripts).
//
// Matching LockST(), LockE() releases all lock resources currently held and then attempt to acquire the named lock resources
// referenced. If no lock resources are specified, it simply releases all lock resources currently held and returns.
//
// interface{} is a series of pairs of varname string and subary []string parameters, where a null subary parameter
// ([]string{}) specifies the unsubscripted lock resource name.
//
// If lock resources are specified, upon return, the process will have acquired all of the named lock resources or none of the
// named lock resources.
//
// If timeoutNsec exceeds YDB_MAX_TIME_NSEC, the function returns with an error return of TIME2LONG.
// If the lock resource names exceeds the maximum number supported (currently eleven), the function returns a PARMOFLOW error.
// If namesubs is not a series of alternating string and []string parameters, the function returns the INVLKNMPAIRLIST error.
// If it is able to aquire the lock resource(s) within timeoutNsec nanoseconds, the function returns holding the lock
// resource(s); otherwise it returns LOCKTIMEOUT. If timeoutNsec is zero, the function makes exactly one attempt to acquire the
// lock resource(s).
func LockE(tptoken uint64, errstr *BufferT, timeoutNsec uint64, namesnsubs ...interface{}) error {
	printEntry("LockE()")
	if 0 != (uint32(len(namesnsubs)) & 1) {
		errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_INVLKNMPAIRLIST))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVLKNMPAIRLIST: %s", err))
		}
		return &YDBError{(int)(YDB_ERR_INVLKNMPAIRLIST), errmsg}
	}
	lckparms := len(namesnsubs)
	parmlst := make([]*KeyT, lckparms/2) // Allocate parameter list of *KeyT values
	for i := 0; lckparms > i; i += 2 {
		// Pull in the next varname and verify it is a string
		newVarname, ok := namesnsubs[i].(string)
		if !ok {
			errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_PARAMINVALID))
			if nil != err {
				panic(fmt.Sprintf("YDB: Error fetching PARAMINVALID: %s", err))
			}
			errmsg = strings.Replace(errmsg, "!AD", "%v", -1)
			errmsg = fmt.Sprintf(errmsg, newVarname, "LockE()")
			return &YDBError{(int)(YDB_ERR_PARAMINVALID), errmsg}
		}
		// Pull in the next subscript array and verify it is an array of strings
		newSubs, ok := namesnsubs[i+1].([]string)
		if !ok {
			errmsg, err := MessageT(tptoken, nil, (int)(YDB_ERR_PARAMINVALID))
			if nil != err {
				panic(fmt.Sprintf("YDB: Error fetching PARAMINVALID: %s", err))
			}
			errmsg = strings.Replace(errmsg, "!AD", "%v", -1)
			errmsg = fmt.Sprintf(errmsg, newVarname, "LockE()")
			return &YDBError{(int)(YDB_ERR_PARAMINVALID), errmsg}
		}
		newKey := new(KeyT)
		// Run through subscripts to find the biggest
		maxsublen := 0
		for _, sub := range newSubs {
			if len(sub) > maxsublen {
				maxsublen = len(sub)
			}
		}
		newKey.Alloc(uint32(len(newVarname)), uint32(len(newSubs)), uint32(maxsublen))
		defer newKey.Free() // Force release of C storage without waiting for GC
		err := newKey.Varnm.SetValStr(tptoken, errstr, newVarname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
		subcnt := len(newSubs)
		for j := 0; subcnt > j; j++ {
			err := newKey.Subary.SetValStr(tptoken, errstr, uint32(j), newSubs[j])
			if nil != err {
				panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
			}
		}
		err = newKey.Subary.SetElemUsed(tptoken, errstr, uint32(subcnt))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
		parmlst[i/2] = newKey
	}
	return LockST(tptoken, errstr, timeoutNsec, parmlst...)
}

// LockDecrE is a STAPI function to decrement the lock count of the given lock. When the count goes to 0, the lock
// is considered released.
//
// Matching LockDecrST(), LockDecrE() wraps ydb_lock_decr_st() to decrement the count of the lock name
// referenced, releasing it if the count goes to zero or ignoring the invocation if the process does not hold the lock.
func LockDecrE(tptoken uint64, errstr *BufferT, varname string, subary []string) error {
	var dbkey KeyT
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("LockDecrE()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	defer dbkey.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	vargobuft := dbkey.Varnm.getCPtr()
	subbuftary := dbkey.Subary.getCPtr()
	rc := C.ydb_lock_decr_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(dbkey.Subary.ElemUsed()), subbuftary)
	if YDB_OK != rc {
		err = NewError(tptoken, errstr, int(rc))
		return err
	}
	runtime.KeepAlive(dbkey) // Make sure dbkey stays in tact through the call into YDB
	runtime.KeepAlive(errstr)
	return nil
}

// LockIncrE is a STAPI function to increase the lock count of a given node within the specified timeout in
// nanoseconds.
//
// Matching  LockIncrST(), LockIncrE() wraps ydb_lock_incr_st() to attempt to acquire the referenced lock
// resource name without releasing any locks the process already holds.
//
// If the process already holds the named lock resource, the function increments its count and returns.
// If timeoutNsec exceeds YDB_MAX_TIME_NSEC, the function returns with an error return TIME2LONG.
// If it is able to aquire the lock resource within timeoutNsec nanoseconds, it returns holding the lock, otherwise it returns
// LOCKTIMEOUT. If timeoutNsec is zero, the function makes exactly one attempt to acquire the lock.
func LockIncrE(tptoken uint64, errstr *BufferT, timeoutNsec uint64, varname string, subary []string) error {
	var dbkey KeyT
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("LockIncrE()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	defer dbkey.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	vargobuft := dbkey.Varnm.getCPtr()
	subbuftary := dbkey.Subary.getCPtr()
	rc := C.ydb_lock_incr_st(C.uint64_t(tptoken), cbuft, C.ulonglong(timeoutNsec), vargobuft,
		C.int(dbkey.Subary.ElemUsed()), subbuftary)
	if YDB_OK != rc {
		err = NewError(tptoken, errstr, int(rc))
		return err
	}
	runtime.KeepAlive(dbkey) // Make sure dbkey stays in tact through the call into YDB
	runtime.KeepAlive(errstr)
	return nil
}

// NodeNextE is a STAPI function to return a string array of the subscripts that describe the next node.
//
// Matching NodeNextST(), NodeNextE() wraps ydb_node_next_st() to facilitate depth first traversal of a local or global variable tree.
//
// If there is a next node, it returns the subscripts of that next node. If the node is the last in the tree, the function returns the NODEEND error.
func NodeNextE(tptoken uint64, errstr *BufferT, varname string, subary []string) ([]string, error) {
	var dbkey KeyT
	var dbsubs BufferTArray
	var err error
	var subscrCnt uint32
	var subscrSize uint32

	printEntry("NodeNextE()")
	defer dbkey.Free()
	defer dbsubs.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	subscrCnt = easyAPIDefaultSubscrCnt
	subscrSize = easyAPIDefaultSubscrSize
	dbsubs.Alloc(subscrCnt, subscrSize)
	// Attempt to fetch the next subscript set multiple times. We do not know how big the incoming subscripts are
	// so loop till they fit.
	for {
		// dbvalue is allocated with current best-guess size of returning data
		err = dbkey.NodeNextST(tptoken, errstr, &dbsubs)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(YDB_ERR_INSUFFSUBS) == errorcode {
				// This is INSUFFSUBS - pickup number of subscripts we actually need and reallocate
				subscrCnt = dbsubs.ElemUsed()
				dbsubs.Free()
				dbsubs.Alloc(subscrCnt, subscrSize) // Reallocate and reset dbsubs
				continue
			}
			if int(YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - the last valid subscript (as shown by elemUsed) is the element
				neededlen, err := dbsubs.ElemLenUsed(tptoken, errstr, dbsubs.ElemUsed())
				if nil != err {
					panic(fmt.Sprintf("YDB: Unexpected error with ElemLenUsed(): %s", err))
				}
				subscrSize = neededlen
				dbsubs.Free()
				dbsubs.Alloc(subscrCnt, subscrSize) // Reallocate and reset dbsubs
				continue
			}
			// Otherwise some error happened so return that
			return []string{}, err
		}
		break // No error so we had success and we are done!
	}
	// Transfer return BufferTArray to our return string array and return to user
	subcnt := int(dbsubs.ElemUsed())
	nextsubs := make([]string, subcnt)
	for i := 0; i < subcnt; i++ {
		nextsub, err := dbsubs.ValStr(tptoken, errstr, uint32(i))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
		}
		nextsubs[i] = nextsub
	}
	return nextsubs, nil
}

// NodePrevE is a STAPI function to return a string array of the subscripts that describe the next node.
//
// Matching NodePrevST(), NodePrevE() wraps ydb_node_previous_st() to facilitate reverse depth first traversal
// of a local or global variable tree.
//
// If there is a previous node, it returns the subscripts of that previous node; an empty string array if that previous node is the root.
// If the node is the first in the tree, the function returns the NODEEND error.
func NodePrevE(tptoken uint64, errstr *BufferT, varname string, subary []string) ([]string, error) {
	var dbkey KeyT
	var dbsubs BufferTArray
	var err error
	var subscrCnt uint32
	var subscrSize uint32

	printEntry("NodePrevE()")
	defer dbkey.Free()
	defer dbsubs.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	subscrCnt = easyAPIDefaultSubscrCnt
	subscrSize = easyAPIDefaultSubscrSize
	dbsubs.Alloc(subscrCnt, subscrSize)
	// Attempt to fetch the next subscript set multiple times. We do not know how big the incoming subscripts are
	// so loop till they fit.
	for {
		// dbvalue is allocated with current best-guess size of returning data
		err = dbkey.NodePrevST(tptoken, errstr, &dbsubs)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(YDB_ERR_INSUFFSUBS) == errorcode {
				// This is INSUFFSUBS - pickup number of subscripts we actually need and reallocate
				subscrCnt = dbsubs.ElemUsed()
				dbsubs.Free()
				dbsubs.Alloc(subscrCnt, subscrSize)
				continue
			}
			if int(YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - the last valid subscript (as shown by elemUsed) is the element
				neededlen, err := dbsubs.ElemLenUsed(tptoken, errstr, dbsubs.ElemUsed())
				if nil != err {
					panic(fmt.Sprintf("YDB: Unexpected error with ElemLenUsed(): %s", err))
				}
				subscrSize = neededlen
				dbsubs.Free()
				dbsubs.Alloc(subscrCnt, subscrSize)
				continue
			}
			// Otherwise some error happened so return that
			return []string{}, err
		}
		break // No error so success and we are done!
	}
	// Transfer return BufferTArray to our return string array and return to user
	subcnt := int(dbsubs.ElemUsed())
	nextsubs := make([]string, subcnt)
	for i := 0; i < int(dbsubs.ElemUsed()); i++ {
		nextsub, err := dbsubs.ValStr(tptoken, errstr, uint32(i))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
		}
		nextsubs[i] = nextsub
	}
	return nextsubs, nil
}

// SetValE is a STAPI function to set a value into the given node (varname and subscripts).
//
// Matching SetValST(), at the referenced local or global variable node, or the intrinsic special variable, SetValE() wraps
// ydb_set_st() to set the value specified.
func SetValE(tptoken uint64, errstr *BufferT, value, varname string, subary []string) error {
	var dbkey KeyT
	var dbvalue BufferT
	var maxsublen, sublen, i uint32
	var err error
	var cbuft *C.ydb_buffer_t

	printEntry("SetValE()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	defer dbkey.Free()
	defer dbvalue.Free()
	subcnt := uint32(len(subary))
	maxsublen = 0
	for i = 0; i < subcnt; i++ {
		// Find maximum length of subscript so know how much to allocate
		sublen = uint32(len(subary[i]))
		if sublen > maxsublen {
			maxsublen = sublen
		}
	}
	dbkey.Alloc(uint32(len(varname)), subcnt, maxsublen)
	dbkey.Varnm.SetValStr(tptoken, errstr, varname)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	// Load subscripts into KeyT (if any)
	for i = 0; i < subcnt; i++ {
		err = dbkey.Subary.SetValStr(tptoken, errstr, i, subary[i])
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	err = dbkey.Subary.SetElemUsed(tptoken, errstr, subcnt)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetUsed(): %s", err))
	}
	dbvalue.Alloc(uint32(len(value)))
	err = dbvalue.SetValStr(tptoken, errstr, value)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	vargobuft := dbkey.Varnm.getCPtr()
	subbuftary := dbkey.Subary.getCPtr()
	rc := C.ydb_set_st(C.uint64_t(tptoken), cbuft, vargobuft, C.int(dbkey.Subary.ElemUsed()), subbuftary, dbvalue.getCPtr())
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return err
	}
	runtime.KeepAlive(dbkey) // Make sure dbkey and dbvalue stays intact through the call into YDB
	runtime.KeepAlive(dbvalue)
	runtime.KeepAlive(errstr)
	return nil
}

// SubNextE is a STAPI function to return the next subscript at the current subscript level.
//
// Matching SubNextST(), SubNextE() wraps ydb_subscript_next_st() to facilitate breadth-first traversal of a
// local or global variable sub-tree.
//
// At the level of the last subscript, if there is a next subscript with a node and/or a subtree, it returns that subscript.
// If there is no next node or subtree at that level of the subtree, the function returns the NODEEND error.
//
// In the special case where subary is the null array, SubNextE() returns the name of the next global or local
// variable, and the NODEEND error if varname is the last global or local variable.
func SubNextE(tptoken uint64, errstr *BufferT, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbsub BufferT
	var err error
	var subscrSize uint32

	printEntry("SubNextE()")
	defer dbkey.Free()
	defer dbsub.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	subscrSize = easyAPIDefaultSubscrSize
	dbsub.Alloc(subscrSize)
	// Attempt to fetch the value multiple times. We do not know how big the incoming record is
	// so loop till it fits.
	for {
		// dbsub is allocated with current best-guess size of returning data
		err = dbkey.SubNextST(tptoken, errstr, &dbsub)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - reallocate the size we need
				subscrSize = uint32(dbsub.getCPtr().len_used)
				dbsub.Free()
				dbsub.Alloc(subscrSize)
				continue
			}
			// Otherwise something badder-er happened
			return "", err
		}
		break // No error so success and we are done!
	}
	retval, err := dbsub.ValStr(tptoken, errstr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
	}
	return retval, nil
}

// SubPrevE is a STAPI function to return the previous subscript at the current subscript level.
//
// Matching SubPrevST(), SubPrevE() wraps ydb_subscript_previous_st() to facilitate reverse breadth-first
// traversal of a local or global variable sub-tree.
//
// At the level of the last subscript, if there is a previous subscript with a node and/or a subtree, it returns that subscript.
// If there is no previous node or subtree at that level of the subtree, the function returns the NODEEND error.
//
// In the special case where subary is the null array SubNextE() returns the name of the previous global or local
// variable, and the NODEEND error if varname is the first global or local variable.
func SubPrevE(tptoken uint64, errstr *BufferT, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbsub BufferT
	var err error
	var subscrSize uint32

	printEntry("SubPrevE()")
	defer dbkey.Free()
	defer dbsub.Free()
	initkey(tptoken, errstr, &dbkey, varname, subary)
	subscrSize = easyAPIDefaultSubscrSize
	dbsub.Alloc(subscrSize)
	// Attempt to fetch the value multiple times. We do not know how big the incoming record is
	// so loop till it fits.
	for {
		// dbsub is allocated with current best-guess size of returning data
		err = dbkey.SubPrevST(tptoken, errstr, &dbsub)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - reallocate the size we need
				subscrSize = uint32(dbsub.getCPtr().len_used)
				dbsub.Free()
				dbsub.Alloc(subscrSize)
				continue
			}
			// Otherwise something badder-er happened
			return "", err
		}
		break // No error so success and we are done!
	}
	retval, err := dbsub.ValStr(tptoken, errstr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
	}
	return retval, nil
}

// TpE is a Easy API function to drive transactions.
//
// Using TpST(), TpE() wraps ydb_tp_st() to implement transaction processing.
//
// Parameters:
//
// tptoken  - the token used to identify nested transaction; start with yottadb.NOTTP.
// tpfn     - the closure which will be run during the transaction. This closure may get invoked multiple times if a
//            transaction fails for some reason (concurrent changes, for example), so should not change any data outside of
//            the database.
// transid  - See docs for ydb_tp_s() in the MLPG.
// varnames - a list of local YottaDB variables to reset should the transaction be restarted; if this is an array of 1 string
//            with a value of "*" all YDB local variables get reset after a TP_RESTART.
func TpE(tptoken uint64, errstr *BufferT, tpfn func(uint64, *BufferT) int32, transid string, varnames []string) error {
	var vnames BufferTArray
	var maxvarnmlen, varnmcnt, varnmlen uint32
	var i int
	var err error
	var varname string

	printEntry("TpE()")
	defer vnames.Free()
	varnmcnt = uint32(len(varnames))
	// Find maximum length of varname so know how much to allocate
	maxvarnmlen = 0
	for _, varname = range varnames {
		varnmlen = uint32(len(varname))
		if varnmlen > maxvarnmlen {
			maxvarnmlen = varnmlen
		}
	}
	vnames.Alloc(varnmcnt, maxvarnmlen)
	for i, varname = range varnames {
		err = vnames.SetValStr(tptoken, errstr, uint32(i), varname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	// Drive simpleAPI wrapper and return its return code
	return vnames.TpST(tptoken, errstr, tpfn, transid)
}
