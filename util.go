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
	"sync/atomic"
	"unsafe"
)

// #include "libyottadb.h"
// /* C routine to get around the cgo issue and its lack of support for variadic plist routines */
// void *ydb_get_cipt_funcvp(void);
// void *ydb_get_cipt_funcvp(void)
// {
// 	return (void *)&ydb_cip_t;
// }
import "C"

// CallMDesc is a struct that (ultimately) serves as an anchor point for the C call-in routine descriptor
// used by CallMDescT() that provides for less call-overhead than CallMT() as the descriptor contains fastpath
// information filled in by YottaDB after the first call so subsequent calls have minimal overhead. Because
// this structure's contents contain pointers to C allocated storage, this structure is NOT safe for
// concurrent access.
type CallMDesc struct {
	cmdesc *internalCallMDesc
}

type internalCallMDesc struct {
	rtnname  string                // Copy of rtn name in descriptor - easier for use in panic error msgs
	filledin bool                  // Indicates call has been made to fill-in parmtyps struct
	cmdesc   *C.ci_name_descriptor // Descriptor for M routine with fastpath for calls after first
	parmtyps *C.ci_parm_type       // Where we hang the information we receive about the entry point
}

// CallMTable is a struct that defines a call table (see
// https://docs.yottadb.com/ProgrammersGuide/extrout.html#calls-from-external-routines-call-ins).
// The methods associated with this struct allow call tables to be opened and to switch between them
// to give access to routines in multiple call tables.
type CallMTable struct {
	handle uintptr // Handle used to access the call table
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Utility methods
//
////////////////////////////////////////////////////////////////////////////////////////////////////
//
// First up - methods for CallMDesc struct.

// Free is a method to release both the routine name buffer and the descriptor block associated with
// the CallMDesc block.
func (mdesc *CallMDesc) Free() {
	if nil == mdesc {
		return
	}
	mdesc.cmdesc.Free()
	mdesc.cmdesc = nil
}

// Call freeMem() on any C memory owned by this internalCallMDesc
func (imdesc *internalCallMDesc) Free() {
	printEntry("internalCallMDesc.Free()")
	if nil == imdesc {
		return
	}
	cindPtr := imdesc.cmdesc
	if nil != cindPtr {
		if nil != cindPtr.rtn_name.address {
			freeMem(unsafe.Pointer(cindPtr.rtn_name.address), C.size_t(cindPtr.rtn_name.length))
			cindPtr.rtn_name.address = nil
		}
		freeMem(unsafe.Pointer(cindPtr), C.size_t(C.sizeof_ci_name_descriptor))
		// The below keeps imdesc around long enough to get rid of this block's C memory. No KeepAlive() necessary.
		imdesc.cmdesc = nil
	}
	ctypPtr := imdesc.parmtyps
	if nil != ctypPtr {
		freeMem(unsafe.Pointer(ctypPtr), C.size_t(C.sizeof_ci_parm_type))
		imdesc.parmtyps = nil
	}
}

// SetRtnName is a method for CallMDesc that sets the routine name into the descriptor.
func (mdesc *CallMDesc) SetRtnName(rtnname string) {
	var cindPtr *C.ci_name_descriptor
	var ctypPtr *C.ci_parm_type
	printEntry("CallMDesc.SetRtnName()")
	if nil == mdesc {
		panic("YDB: *CallMDesc receiver of SetRtnName() cannot be nil")
	}
	rtnnamelen := len(rtnname)
	if 0 == rtnnamelen {
		panic("YDB: Routine name string for SetRtnName() cannot be null")
	}
	// If this is a previously allocated critter, free the CString memory but don't reallocate the
	// ci_name_descriptor or set the finalizer (which is already setup).
	if nil != mdesc.cmdesc {
		if nil == mdesc.cmdesc.cmdesc {
			panic("YDB: Inner cmdesc structure allocated but has no C memory allocated")
		}
		cindPtr = mdesc.cmdesc.cmdesc
		if nil == cindPtr.rtn_name.address {
			panic("YDB: Routine name address is nil - out of design situation")
		}
		freeMem(unsafe.Pointer(cindPtr.rtn_name.address), C.size_t(cindPtr.rtn_name.length))
		cindPtr.rtn_name.address = nil
	} else {
		cindPtr = (*C.ci_name_descriptor)(allocMem(C.size_t(C.sizeof_ci_name_descriptor)))
		ctypPtr = (*C.ci_parm_type)(allocMem(C.size_t(C.sizeof_ci_parm_type)))
		mdesc.cmdesc = &internalCallMDesc{rtnname, false, cindPtr, ctypPtr}
		// Set a finalizer so this block is released when garbage collected
		runtime.SetFinalizer(mdesc.cmdesc, func(o *internalCallMDesc) { o.Free() })
	}
	cindPtr.rtn_name.address = C.CString(rtnname) // Allocates new memory we need to release when done (done by finalizer)
	cindPtr.rtn_name.length = C.ulong(rtnnamelen)
	cindPtr.handle = nil
	mdesc.cmdesc.filledin = false // Mark parm type struct as having been NOT filled in yet
	runtime.KeepAlive(mdesc)
}

// CallMDescT allows calls to M with string arguments and an optional string return value if the called function returns one
// and a return value is described in the call-in definition. Else return is nil.
func (mdesc *CallMDesc) CallMDescT(tptoken uint64, errstr *BufferT, retvallen uint32, rtnargs ...interface{}) (string, error) {
	var vplist variadicPlist
	var parmIndx, inmask, outmask, imask, omask uint32
	var err error
	var retvalPtr *C.ydb_string_t
	var cbuft *C.ydb_buffer_t
	var i int
	var strparm, retval string
	var strPtr *string
	var intPtr *int
	var int32Ptr *int32
	var int64Ptr *int64
	var uintPtr *uint
	var uint32Ptr *uint32
	var uint64Ptr *uint64
	var boolPtr *bool
	var float32Ptr *float32
	var float64Ptr *float64
	var cmplx64Ptr *complex64
	var cmplx128Ptr *complex128
	var parmOK bool

	printEntry("CallMDesc.CallMDescT()")
	if nil == mdesc {
		panic("YDB: *CallMDesc receiver of CallMDescT() cannot be nil")
	}
	if (nil == mdesc.cmdesc) || (nil == (mdesc.cmdesc.cmdesc)) || (nil == mdesc.cmdesc.parmtyps) {
		panic("YDB: SetRtnName() method has not been invoked on this descriptor")
	}
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	// If we haven't already fetched the call description from YDB, do that now
	if !mdesc.cmdesc.filledin {
		if nil != errstr {
			cbuft = errstr.getCPtr()
		}
		rc := C.ydb_ci_get_info_t(C.uint64_t(tptoken), cbuft, mdesc.cmdesc.cmdesc.rtn_name.address, mdesc.cmdesc.parmtyps)
		if YDB_OK != rc {
			err := NewError(tptoken, errstr, int(rc))
			return "", err
		}
		mdesc.cmdesc.filledin = true
	}
	defer vplist.free() // Initialize variadic plist we need to use to call ydb_cip_helper()
	vplist.alloc()
	// First two parms are the tptoken and the contents of the BufferT (not the BufferT itself).
	err = vplist.setVPlistParam64Bit(tptoken, errstr, &parmIndx, tptoken) // Takes care of bumping parmIndx
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist64Bit.setVPlistParam(): %s", err))
	}
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(cbuft)))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
	}
	parmIndx++
	// Third parm for ydb_cip_t() is the descriptor address so add that now
	err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(mdesc.cmdesc.cmdesc)))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
	}
	parmIndx++
	// Setup return value if any (first variable parm)
	if 0 != retvallen {
		retvalPtr = (*C.ydb_string_t)(allocMem(C.size_t(C.sizeof_ydb_string_t)))
		defer freeMem(unsafe.Pointer(retvalPtr), C.size_t(C.sizeof_ydb_string_t)) // Free this when we are done
		retvalPtr.address = (*C.char)(allocMem(C.size_t(retvallen)))
		defer freeMem(unsafe.Pointer(retvalPtr.address), C.size_t(retvallen))
		retvalPtr.length = (C.ulong)(retvallen)
		err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(retvalPtr)))
		if nil != err {
			return "", err
		}
		parmIndx++
	}
	// Now process each parameter into the variadic parm list. To reduce the number of mallocs/frees for this
	// step, we allocate an array of ydb_string_t structs needed to pass these parms into C however due to cgo
	// limitations, we cannot copy string args directly to C memory so just let Go allocate the memory for the
	// input strings.
	//
	// Parameters can be various types supported by external calls. They are all converted to strings for now as
	// Go does not have access to the call descriptor that defines argument types.
	parmcnt := len(rtnargs)
	if parmcnt > int(C.YDB_MAX_PARMS) {
		panic(fmt.Sprintf("YDB: Parm count of %d exceeds maximum parm count of %d", parmcnt, int(C.YDB_MAX_PARMS)))
	}
	allocLen := C.size_t(C.sizeof_ydb_string_t * parmcnt)
	parmblkPtr := (*C.ydb_string_t)(allocMem(allocLen))
	defer freeMem(unsafe.Pointer(parmblkPtr), allocLen)
	parmPtr := parmblkPtr
	inmask = uint32(mdesc.cmdesc.parmtyps.input_mask)
	outmask = uint32(mdesc.cmdesc.parmtyps.output_mask)
	// Turn each parameter into a ydb_string_t buffer descriptor and load it into our variadic plist
	for i, imask, omask = 0, inmask, outmask; i < parmcnt; i, imask, omask = (i + 1), (imask >> 1), (omask >> 1) {
		// If this parm is an input parm (versus output only), rebuffer it in C memory, else just allocate the buffer
		// without copying anything. For input parms, we take special care with pass-by-reference types since they
		// need to be de-referenced to get to the value so each type must be handled separately.
		if 1 == (1 & imask) { // This is an input parameter (note this includes IO parms
			if 0 == (1 & omask) { // The only input parms that can also currently be output parms are *string
				parmOK = true
			} else {
				parmOK = false
			}
			// The rtnargs[i] array of parameters is an interface array because it is not a homogeneous type with
			// each parm possibly a different type. Also, rtnargs[i] cannot be dereferenced without using a
			// type-assersion (the stmt above the fmt.Sprintf in each block) to convert the interface value to
			// the proper type. But to know what type to convert to, we first need to use the big type-switch
			// below to specifically test for values passed as addresses so they can be dereferenced. All of the
			// basic Go types are represented here.
			switch rtnargs[i].(type) {
			case *string:
				parmOK = true
				strPtr = rtnargs[i].(*string)
				strparm = fmt.Sprintf("%v", *strPtr)
			case *int:
				intPtr = rtnargs[i].(*int)
				strparm = fmt.Sprintf("%v", *intPtr)
			case *int32:
				int32Ptr = rtnargs[i].(*int32)
				strparm = fmt.Sprintf("%v", *int32Ptr)
			case *int64:
				int64Ptr = rtnargs[i].(*int64)
				strparm = fmt.Sprintf("%v", *int64Ptr)
			case *uint:
				uintPtr = rtnargs[i].(*uint)
				strparm = fmt.Sprintf("%v", *uintPtr)
			case *uint32:
				uint32Ptr = rtnargs[i].(*uint32)
				strparm = fmt.Sprintf("%v", *uint32Ptr)
			case *uint64:
				uint64Ptr = rtnargs[i].(*uint64)
				strparm = fmt.Sprintf("%v", *uint64Ptr)
			case *bool:
				boolPtr = rtnargs[i].(*bool)
				strparm = fmt.Sprintf("%v", *boolPtr)
			case *float32:
				float32Ptr = rtnargs[i].(*float32)
				strparm = fmt.Sprintf("%v", *float32Ptr)
			case *float64:
				float64Ptr = rtnargs[i].(*float64)
				strparm = fmt.Sprintf("%v", *float64Ptr)
			case *complex64:
				cmplx64Ptr = rtnargs[i].(*complex64)
				strparm = fmt.Sprintf("%v", *cmplx64Ptr)
			case *complex128:
				cmplx128Ptr = rtnargs[i].(*complex128)
				strparm = fmt.Sprintf("%v", *cmplx128Ptr)
			default:
				// Assume passed by value - this generic string conversion suffices
				strparm = fmt.Sprintf("%v", rtnargs[i])
			}
			if !parmOK {
				panic(fmt.Sprintf("YDB: Call-in routine %s parm %d is an output parm and must be *string but is not",
					mdesc.cmdesc.rtnname, i+1))
			}
			// Initialize our ydb_string_t (parmPtr) that describes the parm
			parmPtr.length = C.ulong(len(strparm))
			if 0 < parmPtr.length {
				// Check if parm is pass-by-value or pass-by-reference by checking ci info
				parmPtr.address = C.CString(strparm)
				defer freeMem(unsafe.Pointer(parmPtr.address), C.size_t(parmPtr.length))
			} else {
				parmPtr.address = nil
			}
		} else { // Otherwise, this is an output-only parameter - allocate a C buffer for it but otherwise leave it alone
			// Note this parm is always passed by reference (verify it).
			if 0 == (1 & omask) { // Check for unexpected parameter
				panic(fmt.Sprintf("YDB: Call-in routine %s parm %d is not a parameter defined in the call-in table",
					mdesc.cmdesc.rtnname, i+1))
			}
			switch rtnargs[i].(type) {
			case *string: // passed-by-reference string parameter
				parmOK = true
			default:
				parmOK = false
			}
			if !parmOK {
				panic(fmt.Sprintf("YDB: Call-in routine %s parm %d is an output parm and must be *string but is not",
					mdesc.cmdesc.rtnname, i+1))
			}
			pval := rtnargs[i].(*string)
			// Setup ydb_string_t (parmPtr) to point to our string
			parmPtr.length = C.ulong(len(*pval))
			if 0 < parmPtr.length {
				parmPtr.address = (*C.char)(allocMem(C.size_t(parmPtr.length)))
				defer freeMem(unsafe.Pointer(parmPtr.address), C.size_t(parmPtr.length))
			} else {
				parmPtr.address = nil
			}
		}
		// Now add parmPtr to the variadic plist
		err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(parmPtr)))
		if nil != err {
			return "", err
		}
		// Increment parmPtr to next ydb_buffer_t and the variadic list to its next slot
		parmPtr = (*C.ydb_string_t)(unsafe.Pointer(uintptr(unsafe.Pointer(parmPtr)) + uintptr(C.sizeof_ydb_string_t)))
		parmIndx++
	}
	err = vplist.setUsed(tptoken, errstr, uint32(parmIndx))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setUsed(): %s", err))
	}
	// Now drive the variadic plist call - we have to drive the C glue routine defined at the top of this file in
	// the cgo header in order to drive ydb_cip_t().
	rc := vplist.callVariadicPlistFunc(C.ydb_get_cipt_funcvp()) // Drive ydb_cip_t()
	if YDB_OK != rc {
		err = NewError(tptoken, errstr, int(rc))
		return "", err
	}
	if 0 != retvallen { // If we have a return value
		// Build a string of the length of the return value
		retval = C.GoStringN(retvalPtr.address, C.int(retvalPtr.length))
	} else {
		retval = ""
	}
	// Go through the parameters again to locate the output parameters and copy their values back into Go space
	parmPtr = parmblkPtr
	for i, omask = 0, outmask; i < parmcnt; i, omask = i+1, omask>>1 {
		if 1 == (1 & omask) { // This is an output parameter
			rtnargPtr := rtnargs[i].(*string)
			*rtnargPtr = C.GoStringN(parmPtr.address, C.int(parmPtr.length))
		}
		// Increment parmPtr to next ydb_buffer_t and the variadic list to its next slot
		parmPtr = (*C.ydb_string_t)(unsafe.Pointer(uintptr(unsafe.Pointer(parmPtr)) + uintptr(C.sizeof_ydb_string_t)))
	}
	runtime.KeepAlive(mdesc) // Make sure mdesc hangs around through the YDB call
	runtime.KeepAlive(vplist)
	runtime.KeepAlive(rtnargs)
	runtime.KeepAlive(errstr)
	return retval, nil
}

// Methods for CallMTable struct

// CallMTableSwitchT method switches whatever the current call table is (only one active at a time) with the supplied
// call table and returns the call table that was in effect (or nil if none).
func (newcmtable *CallMTable) CallMTableSwitchT(tptoken uint64, errstr *BufferT) (*CallMTable, error) {
	var cbuft *C.ydb_buffer_t
	var callmtabret CallMTable

	if nil == newcmtable {
		panic("YDB: Non-nil CallMTable structure must be specified")
	}
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_ci_tab_switch_t(C.uint64_t(tptoken), cbuft, C.uintptr_t(newcmtable.handle),
		(*C.uintptr_t)(unsafe.Pointer(&callmtabret.handle)))
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return nil, err
	}
	runtime.KeepAlive(errstr)
	return &callmtabret, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Utility functions
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// CallMT allows calls to M with string arguments and an optional string return value if the called function returns one
// and a return value is described in the call-in definition. Else return is nil. This function differs from CallMDescT()
// in that the name of the routine is specified here and must always be looked up in the routine list. To avoid having
// two routines nearly identical, this routine is written to invoke CallMDescT().
func CallMT(tptoken uint64, errstr *BufferT, retvallen uint32, rtnname string, rtnargs ...interface{}) (string, error) {
	var mdesc CallMDesc

	printEntry("CallMDesc.CallMT()")
	if "" == rtnname {
		panic("YDB: Name of routine to call cannot be null string")
	}
	mdesc.SetRtnName(rtnname)
	return mdesc.CallMDescT(tptoken, errstr, retvallen, rtnargs...)
}

// CallMTableOpenT function opens a new call table or one for which the process had no handle and returns a
// CallMTable for it.
func CallMTableOpenT(tptoken uint64, errstr *BufferT, tablename string) (*CallMTable, error) {
	var callmtab CallMTable
	var cbuft *C.ydb_buffer_t

	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	cstr := C.CString(tablename)
	defer C.free(unsafe.Pointer(cstr))
	if nil != errstr {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_ci_tab_open_t(C.uint64_t(tptoken), cbuft, cstr, (*C.uintptr_t)(unsafe.Pointer(&callmtab.handle)))
	if YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return nil, err
	}
	runtime.KeepAlive(errstr)
	return &callmtab, nil
}

// MessageT is a STAPI utility function to return the error message (sans argument substitution) of a given error number.
func MessageT(tptoken uint64, errstr *BufferT, status int) (string, error) {
	var msgval BufferT
	var cbuft *C.ydb_buffer_t
	var err error
	var errorMsg string

	printEntry("MessageT()")
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	statusOriginal := status
	if 0 > status { // Get absolute value of status so we can extract facility bits correctly
		status = -status
	}
	// First check the "facility" id buried in the error number (status)
	facility := (uint32(status) >> 16) & 0x7ff // See error format in sr_port/error.h of YottaDB - isolate the 11 bit facility
	switch facility {
	case 246: // GT.M facility (use C.ydb_message_t())
		fallthrough
	case 256: // YDB facility (use C.ydb_message_t())
		// Check for a couple of special cases first. First, if the error is YDB_ERR_THREADEDAPINOTALLOWED, the same error
		// will prevent the below call into ydb_message_t() from working so create a hard-return error message for that
		// case before attempting the call.
		switch statusOriginal {
		case YDB_ERR_THREADEDAPINOTALLOWED:
			return "%YDB-E-THREADEDAPINOTALLOWED, Process cannot switch to using threaded Simple API while " +
				"already using Simple API", nil
		case YDB_ERR_CALLINAFTERXIT:
			// The engine is shut down so calling ydb_message_t will fail if we attempt it so just hard-code this
			// error return value.
			return "%YDB-E-CALLINAFTERXIT, After a ydb_exit(), a process cannot create a valid YottaDB context", nil
		}
		defer msgval.Free()
		msgval.Alloc(uint32(YDB_MAX_ERRORMSG))
		if nil != errstr {
			cbuft = errstr.getCPtr()
		}
		rc := C.ydb_message_t(C.uint64_t(tptoken), cbuft, C.int(status), msgval.getCPtr())
		if YDB_OK != rc {
			panic(fmt.Sprintf("YDB: Error calling ydb_message_t() for argument %d: %d", status, int(rc)))
		}
		// Returned string should be snug in the retval buffer. Pick it out so can return it as a string
		errorMsg, err = msgval.ValStr(tptoken, errstr)
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with ValStr(): %s", err))
		}
	case 264: // Facility id for YDBGo wrapper errors
		errorMsg = getWrapperErrorMsg(statusOriginal)
		if "" == errorMsg {
			panic(fmt.Sprintf("YDB: Wrapper error message %d not found", statusOriginal))
		}
	default:
		panic(fmt.Sprintf("YDB: Unknown message facility: %d from error id %d", facility, statusOriginal))
	}
	runtime.KeepAlive(errstr)
	runtime.KeepAlive(msgval)
	return errorMsg, err
}

// ReleaseT is a STAPI utility function to return release information for the current underlying YottaDB version
func ReleaseT(tptoken uint64, errstr *BufferT) (string, error) {
	printEntry("ReleaseT()")
	zyrel, err := ValE(tptoken, errstr, "$ZYRELEASE", []string{})
	if nil != err {
		return "", err
	}
	retval := fmt.Sprintf("gowr %s %s", WrapperRelease, zyrel)
	return retval, nil
}
