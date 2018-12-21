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
	"strings"
	"unsafe"
)

// #include "libyottadb.h"
// #include "libydberrors.h"
import "C"

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Functions making up the EasyAPI
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// DataE is a STAPI function to return $DATA() value for a given variable subscripted or not.
func DataE(tptoken uint64, varname string, subary []string) (uint32, error) {
	var retval C.uint
	var dbkey KeyT
	var err error

	printEntry("DataE()")
	defer dbkey.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	vargobuft := dbkey.Varnm.cbuft
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(dbkey.Subary.cbuftary))
	rc := C.ydb_data_st(C.uint64_t(tptoken), vargobuft, C.int(dbkey.Subary.elemsUsed), subbuftary, &retval)
	if C.YDB_OK != rc {
		err = NewError(int(rc))
		return 0, err
	}
	return uint32(retval), nil
}

// DeleteE is a STAPI function to delete a node or a subtree (see DeleteST) given a deletion type and a varname/subscript set
func DeleteE(tptoken uint64, deltype int, varname string, subary []string) error {
	var dbkey KeyT
	var err error

	printEntry("DeleteE()")
	defer dbkey.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	vargobuft := dbkey.Varnm.cbuft
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(dbkey.Subary.cbuftary))
	rc := C.ydb_delete_st(C.uint64_t(tptoken), vargobuft, C.int(dbkey.Subary.elemsUsed), subbuftary,
		C.int(deltype))
	if C.YDB_OK != rc {
		err = NewError(int(rc))
		return err
	}
	return nil
}

// DeleteExclE is a STAPI function to do an exclusive delete by deleting all local variables except those root vars specified
// in the variable name array. If the varname array is empty, all local variables are deleted.
func DeleteExclE(tptoken uint64, varnames []string) error {
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
		err = vnames.SetValStr(tptoken, uint32(i), &varname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	err = vnames.SetElemUsed(tptoken, varnmcnt)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetUsed(): %s", err))
	}
	// Drive simpleAPI wrapper and return its return code
	return vnames.DeleteExclST(tptoken)
}

// ValE is an STAPI function to return the value found for varname(subary...)
func ValE(tptoken uint64, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbvalue BufferT
	var err error
	var retval *string

	printEntry("ValE()")
	defer dbkey.Free()
	defer dbvalue.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	dbvalue.Alloc(easyAPIDefaultDataSize)
	// Attempt to fetch the value multiple times. We do not know how big the incoming record is
	// so loop till it fits.
	for C.YDB_MAX_STR > easyAPIDefaultDataSize {
		// dbvalue is allocated with current best-guess size of returning data
		err = dbkey.ValST(tptoken, &dbvalue)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(C.YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - reallocate the size we need
				easyAPIDefaultDataSize = uint32(dbvalue.cbuft.len_used)
				dbvalue.Alloc(easyAPIDefaultDataSize)
				continue
			}
			// Otherwise something badder-er happened
			return "", err
		}
		break // No error so success and we are done!
	}
	retval, err = dbvalue.ValStr(tptoken)
	return *retval, err
}

// IncrE is a STAPI function to increment the given value by the given amount and return the new value
func IncrE(tptoken uint64, incr, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbvalue, incrval BufferT
	var err error
	var retval *string

	printEntry("IncrE()")
	defer dbkey.Free()
	defer dbvalue.Free()
	defer incrval.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	dbvalue.Alloc(easyAPIDefaultDataSize)
	incrval.Alloc(uint32(len(incr)))
	err = incrval.SetValStr(tptoken, &incr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	// Since the return value is only checked to see if it is big enough AFTER the increment
	// is done, we cannot repeat the operation with a larger buffer (or the value would be incremented
	// again) so for this call, whatever happens just happens though the default buffer should be
	// large enough for any reasonable value being incremented.
	vargobuft := dbkey.Varnm.cbuft
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(dbkey.Subary.cbuftary))
	rc := C.ydb_incr_st(C.uint64_t(tptoken), vargobuft, C.int(dbkey.Subary.elemsUsed), subbuftary,
		incrval.cbuft, dbvalue.cbuft)
	if C.YDB_OK != rc {
		err = NewError(int(rc))
		return "", err
	}
	retval, err = dbvalue.ValStr(tptoken)
	return *retval, err
}

// LockE is a STAPI function whose purpose is to release all locks and then lock the locks designated. The variadic list
// is pairs of arguments with the first being a string containing the variable name and the second being a string array
// containing the subscripts, if any, for that variable (null list for no subscripts).
func LockE(tptoken uint64, timeoutNsec uint64, namesnsubs ...interface{}) error {
	printEntry("LockE()")
	if 0 != (uint32(len(namesnsubs)) & 1) {
		errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_INVLNPAIRLIST))
		if nil != err {
			panic(fmt.Sprintf("YDB: Error fetching INVLNPAIRLIST: %s", err))
		}
		return &YDBError{(int)(C.YDB_ERR_INVLNPAIRLIST), errmsg}
	}
	lckparms := len(namesnsubs)
	parmlst := make([]*KeyT, lckparms/2) // Allocate parameter list of *KeyT values
	for i := 0; lckparms > i; i += 2 {
		// Pull in the next varname and verify it is a string
		newVarname, ok := namesnsubs[i].(string)
		if !ok {
			errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_PARAMINVALID))
			if nil != err {
				panic(fmt.Sprintf("YDB: Error fetching PARAMINVALID: %s", err))
			}
			errmsg = strings.Replace(errmsg, "!AD", "%v", -1)
			errmsg = fmt.Sprintf(errmsg, newVarname, "LockE()")
			return &YDBError{(int)(C.YDB_ERR_PARAMINVALID), errmsg}
		}
		// Pull in the next subscript array and verify it is an array of strings
		newSubs, ok := namesnsubs[i+1].([]string)
		if !ok {
			errmsg, err := MessageT(tptoken, (int)(C.YDB_ERR_PARAMINVALID))
			if nil != err {
				panic(fmt.Sprintf("YDB: Error fetching PARAMINVALID: %s", err))
			}
			errmsg = strings.Replace(errmsg, "!AD", "%v", -1)
			errmsg = fmt.Sprintf(errmsg, newVarname, "LockE()")
			return &YDBError{(int)(C.YDB_ERR_PARAMINVALID), errmsg}
		}
		newKey := new(KeyT)
		defer (*newKey).Free() // Need to clean up these KeyT structs when done with them.
		// Run through subscripts to find the biggest
		maxsublen := 0
		for _, sub := range newSubs {
			if len(sub) > maxsublen {
				maxsublen = len(sub)
			}
		}
		(*newKey).Alloc(uint32(len(newVarname)), uint32(len(newSubs)), uint32(maxsublen))
		err := (*newKey).Varnm.SetValStr(tptoken, &newVarname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
		subcnt := len(newSubs)
		for j := 0; subcnt > j; j++ {
			err := (*newKey).Subary.SetValStr(tptoken, uint32(j), &newSubs[j])
			if nil != err {
				panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
			}
		}
		err = (*newKey).Subary.SetElemUsed(tptoken, uint32(subcnt))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
		parmlst[i/2] = newKey
	}
	return LockST(tptoken, timeoutNsec, parmlst...)
}

// LockDecrE is a STAPI function to decrement the lock count of the given lock. When the count goes to 0, the lock
// is considered released.
func LockDecrE(tptoken uint64, varname string, subary []string) error {
	var dbkey KeyT
	var err error

	printEntry("LockDecrE()")
	defer dbkey.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	vargobuft := dbkey.Varnm.cbuft
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(dbkey.Subary.cbuftary))
	rc := C.ydb_lock_decr_st(C.uint64_t(tptoken), vargobuft, C.int(dbkey.Subary.elemsUsed), subbuftary)
	if C.YDB_OK != rc {
		err = NewError(int(rc))
		return err
	}
	return nil
}

// LockIncrE is a STAPI function to increase the lock count of a given node within the specified timeout in
// nanoseconds.
func LockIncrE(tptoken uint64, timeoutNsec uint64, varname string, subary []string) error {
	var dbkey KeyT
	var err error

	printEntry("LockIncrE()")
	defer dbkey.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	vargobuft := dbkey.Varnm.cbuft
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(dbkey.Subary.cbuftary))
	rc := C.ydb_lock_incr_st(C.uint64_t(tptoken), C.ulonglong(timeoutNsec), vargobuft,
		C.int(dbkey.Subary.elemsUsed), subbuftary)
	if C.YDB_OK != rc {
		err = NewError(int(rc))
		return err
	}
	return nil

}

// NodeNextE is a STAPI function to return a string array of the subscripts that describe the next node.
func NodeNextE(tptoken uint64, varname string, subary []string) ([]string, error) {
	var dbkey KeyT
	var dbsubs BufferTArray
	var err error

	printEntry("NodeNextE()")
	defer dbkey.Free()
	defer dbsubs.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	dbsubs.Alloc(easyAPIDefaultSubscrCnt, easyAPIDefaultSubscrSize)
	// Attempt to fetch the next subscript set multiple times. We do not know how big the incoming subscripts are
	// so loop till they fit.
	for C.YDB_MAX_STR > easyAPIDefaultDataSize {
		// dbvalue is allocated with current best-guess size of returning data
		dbsubs.elemsUsed = dbsubs.elemsAlloc // So allocation is passed as *ret_subs_cnt
		err = dbkey.NodeNextST(tptoken, &dbsubs)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(C.YDB_ERR_INSUFFSUBS) == errorcode {
				// This is INSUFFSUBS - pickup number of subscripts we actually need and reallocate
				easyAPIDefaultSubscrCnt = dbsubs.elemsUsed
				dbsubs.Alloc(easyAPIDefaultSubscrCnt, easyAPIDefaultSubscrSize) // Reallocate and reset dbsubs
				continue
			}
			if int(C.YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - the last valid subscript (as shown by elemsUsed) is the element
				neededlen, err := dbsubs.ElemLenUsed(tptoken, dbsubs.elemsUsed)
				if nil != err {
					panic(fmt.Sprintf("YDB: Unexpected error with ElemLenUsed(): %s", err))
				}
				easyAPIDefaultSubscrSize = neededlen
				dbsubs.Alloc(easyAPIDefaultSubscrCnt, easyAPIDefaultSubscrSize) // Reallocate and reset dbsubs
				continue
			}
			// Otherwise something badder-er happened so return that
			return []string{}, err
		}
		break // No error so we had success and we are done!
	}
	// Transfer return BufferTArray to our return string array and return to user
	subcnt := int(dbsubs.elemsUsed)
	nextsubs := make([]string, subcnt)
	for i := 0; i < subcnt; i++ {
		nextsub, err := dbsubs.ValStr(tptoken, uint32(i))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
		}
		nextsubs[i] = *nextsub
	}
	return nextsubs, nil
}

// NodePrevE is a STAPI function to return a string array of the subscripts that describe the next node.
func NodePrevE(tptoken uint64, varname string, subary []string) ([]string, error) {
	var dbkey KeyT
	var dbsubs BufferTArray
	var err error

	printEntry("NodePrevE()")
	defer dbkey.Free()
	defer dbsubs.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	dbsubs.Alloc(easyAPIDefaultSubscrCnt, easyAPIDefaultSubscrSize)
	// Attempt to fetch the next subscript set multiple times. We do not know how big the incoming subscripts are
	// so loop till they fit.
	for C.YDB_MAX_STR > easyAPIDefaultDataSize {
		// dbvalue is allocated with current best-guess size of returning data
		dbsubs.elemsUsed = dbsubs.elemsAlloc // So allocation is passed as *ret_subs_cnt
		err = dbkey.NodePrevST(tptoken, &dbsubs)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(C.YDB_ERR_INSUFFSUBS) == errorcode {
				// This is INSUFFSUBS - pickup number of subscripts we actually need and reallocate
				easyAPIDefaultSubscrCnt = dbkey.Subary.elemsUsed
				dbsubs.Alloc(easyAPIDefaultSubscrCnt, easyAPIDefaultSubscrSize)
				continue
			}
			if int(C.YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - the last valid subscript (as shown by elemsUsed) is the element
				neededlen, err := dbsubs.ElemLenUsed(tptoken, dbsubs.elemsUsed)
				if nil != err {
					panic(fmt.Sprintf("YDB: Unexpected error with ElemLenUsed(): %s", err))
				}
				easyAPIDefaultSubscrSize = neededlen
				dbsubs.Alloc(easyAPIDefaultSubscrCnt, easyAPIDefaultSubscrSize)
				continue
			}
			// Otherwise something badder-er happened so return that
			return []string{}, err
		}
		break // No error so success and we are done!
	}
	// Transfer return BufferTArray to our return string array and return to user
	subcnt := int(dbsubs.elemsUsed)
	nextsubs := make([]string, subcnt)
	for i := 0; i < int(dbsubs.elemsUsed); i++ {
		nextsub, err := dbsubs.ValStr(tptoken, uint32(i))
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
		}
		nextsubs[i] = *nextsub
	}
	return nextsubs, nil
}

// SetValE is a STAPI function to set a value into the given node (varname and subscripts).
func SetValE(tptoken uint64, value, varname string, subary []string) error {
	var dbkey KeyT
	var dbvalue BufferT
	var maxsublen, sublen, i uint32
	var err error

	printEntry("SetValE()")
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
	dbkey.Varnm.SetValStr(tptoken, &varname)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	// Load subscripts into KeyT (if any)
	for i = 0; i < subcnt; i++ {
		err = dbkey.Subary.SetValStr(tptoken, i, &subary[i])
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	err = dbkey.Subary.SetElemUsed(tptoken, subcnt)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetUsed(): %s", err))
	}
	dbvalue.Alloc(uint32(len(value)))
	err = dbvalue.SetValStr(tptoken, &value)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	vargobuft := dbkey.Varnm.cbuft
	subbuftary := (*C.ydb_buffer_t)(unsafe.Pointer(dbkey.Subary.cbuftary))
	rc := C.ydb_set_st(C.uint64_t(tptoken), vargobuft, C.int(dbkey.Subary.elemsUsed), subbuftary,
		dbvalue.cbuft)
	if C.YDB_OK != rc {
		err := NewError(int(rc))
		return err
	}
	return nil
}

// SubNextE is a STAPI function to return the next subscript at the current subscript level.
func SubNextE(tptoken uint64, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbsub BufferT
	var err error
	var retval *string

	printEntry("SubNextE()")
	defer dbkey.Free()
	defer dbsub.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	dbsub.Alloc(easyAPIDefaultSubscrSize)
	// Attempt to fetch the value multiple times. We do not know how big the incoming record is
	// so loop till it fits.
	for C.YDB_MAX_STR > easyAPIDefaultDataSize {
		// dbsub is allocated with current best-guess size of returning data
		err = dbkey.SubNextST(tptoken, &dbsub)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(C.YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - reallocate the size we need
				easyAPIDefaultSubscrSize = uint32(dbsub.cbuft.len_used)
				dbsub.Alloc(easyAPIDefaultSubscrSize)
				continue
			}
			// Otherwise something badder-er happened
			return "", err
		}
		break // No error so success and we are done!
	}
	retval, err = dbsub.ValStr(tptoken)
	return *retval, err
}

// SubPrevE is a STAPI function to return the previous subscript at the current subscript level.
func SubPrevE(tptoken uint64, varname string, subary []string) (string, error) {
	var dbkey KeyT
	var dbsub BufferT
	var err error
	var retval *string

	printEntry("SubPrevE()")
	defer dbkey.Free()
	defer dbsub.Free()
	initkey(tptoken, &dbkey, &varname, &subary)
	dbsub.Alloc(easyAPIDefaultSubscrSize)
	// Attempt to fetch the value multiple times. We do not know how big the incoming record is
	// so loop till it fits.
	for C.YDB_MAX_STR > easyAPIDefaultDataSize {
		// dbsub is allocated with current best-guess size of returning data
		err = dbkey.SubPrevST(tptoken, &dbsub)
		if nil != err {
			// Check if we had an INVSTRLEN error (too small an output buffer)
			errorcode := ErrorCode(err)
			if int(C.YDB_ERR_INVSTRLEN) == errorcode {
				// This is INVSTRLEN - reallocate the size we need
				easyAPIDefaultSubscrSize = uint32(dbsub.cbuft.len_used)
				dbsub.Alloc(easyAPIDefaultSubscrSize)
				continue
			}
			// Otherwise something badder-er happened
			return "", err
		}
		break // No error so success and we are done!
	}
	retval, err = dbsub.ValStr(tptoken)
	return *retval, err
}

// TpE is a STAPI function to initiate a TP transaction.
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
//
// varnames - a list of local YottaDB variables to reset should the transaction
//  be restarted; if this is an array of 1 string with a value of "*" all YDB
//  local variables get reset after a TP_RESTART
func TpE(tptoken uint64, tpfn unsafe.Pointer, tpfnparm unsafe.Pointer, transid string, varnames []string) error {
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
		err = vnames.SetValStr(tptoken, uint32(i), &varname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	// Drive simpleAPI wrapper and return its return code
	return vnames.TpST(tptoken, tpfn, tpfnparm, transid)
}

// TpE2 is a Easy API function to drive transactions.
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
//
// varnames - a list of local YottaDB variables to reset should the transaction
//  be restarted; if this is an array of 1 string with a value of "*" all YDB
//  local variables get reset after a TP_RESTART
func TpE2(tptoken uint64, tpfn func(uint64) int32, transid string, varnames []string) error {
	var vnames BufferTArray
	var maxvarnmlen, varnmcnt, varnmlen uint32
	var i int
	var err error
	var varname string

	printEntry("TpE2()")
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
		err = vnames.SetValStr(tptoken, uint32(i), &varname)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	// Drive simpleAPI wrapper and return its return code
	return vnames.TpST2(tptoken, tpfn, transid)
}
