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
	"runtime"
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
	cmdesc *C.ci_name_descriptor // Descriptor for M routine with fastpath for calls after first
}

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Utility methods
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// Free is a method to release both the routine name buffer and the descriptor block associated with
// the CallMDesc block.
func (mdesc *CallMDesc) Free() {
	if nil == mdesc {
		return
	}
	mdesc.cmdesc.Free()
	mdesc.cmdesc = nil
}

// Call C.free() on any C memory owned by this internalCallMDesc
func (imdesc *internalCallMDesc) Free() {
	printEntry("internalCallMDesc.Free()")
	if nil == imdesc {
		return
	}
	cindPtr := imdesc.cmdesc
	if nil != cindPtr {
		if nil != cindPtr.rtn_name.address {
			C.free(unsafe.Pointer(cindPtr.rtn_name.address))
			cindPtr.rtn_name.address = nil
		}
		C.free(unsafe.Pointer(cindPtr))
		imdesc.cmdesc = nil
	}
}

// SetRtnName is a method for CallMDesc that sets the routine name into the descriptor.
func (mdesc *CallMDesc) SetRtnName(rtnname string) {
	var cindPtr *C.ci_name_descriptor

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
		C.free(unsafe.Pointer(cindPtr.rtn_name.address))
		cindPtr.rtn_name.address = nil
	} else {
		cindPtr = (*C.ci_name_descriptor)(C.malloc(C.size_t(C.sizeof_ci_name_descriptor)))
		mdesc.cmdesc = &internalCallMDesc{cindPtr}
		// Set a finalizer so this block is released when garbage collected
		runtime.SetFinalizer(mdesc.cmdesc, func(o *internalCallMDesc) { o.Free() })
	}
	cindPtr.rtn_name.address = C.CString(rtnname) // Allocates new memory we need to release when done (done by finalizer)
	cindPtr.rtn_name.length = C.ulong(rtnnamelen)
	cindPtr.handle = nil
}

// CallMDescT allows calls to M with string arguments and an optional string return value if the called function returns one
// and a return value is described in the call-in definition. Else return is nil.
func (mdesc *CallMDesc) CallMDescT(tptoken uint64, errstr *BufferT, retvallen uint32, rtnargs ...interface{}) (string, error) {
	var vplist variadicPlist
	var parmIndx int
	var err error
	var retvalptr *C.ydb_string_t
	var cbuft *C.ydb_buffer_t
	var i int
	var strparm, retval string

	printEntry("CallMDesc.CallMDescT()")
	if nil == mdesc {
		panic("YDB: *CallMDesc receiver of CallMDescT() cannot be nil")
	}
	if (nil == mdesc.cmdesc) || (nil == (mdesc.cmdesc.cmdesc)) {
		panic("YDB: SetRtnName() method has not been invoked on this descriptor")
	}
	defer vplist.free() // Initialize variadic plist we need to use to call ydb_cip_helper()
	vplist.alloc()
	// First two parms are the tptoken and the contents of the BufferT (not the BufferT itself).
	err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(tptoken))
	if nil != err {
		panic(fmt.Sprintf("YDB: Unknown error with varidicPlist.setVPlistParam(): %s", err))
	}
	parmIndx++
	if errstr != nil {
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
		retvalptr = (*C.ydb_string_t)(C.malloc(C.size_t(C.sizeof_ydb_string_t)))
		defer C.free(unsafe.Pointer(retvalptr)) // Free this when we are done
		retvalptr.address = (*C.char)(C.malloc(C.size_t(retvallen)))
		defer C.free(unsafe.Pointer(retvalptr.address))
		retvalptr.length = (C.ulong)(retvallen)
		err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(retvalptr)))
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
	// golang does not have access to the call descriptor that defines argument types.
	parmcnt := len(rtnargs)
	parmblkptr := (*C.ydb_string_t)(C.malloc(C.size_t(C.sizeof_ydb_string_t * parmcnt)))
	defer C.free(unsafe.Pointer(parmblkptr))
	parmptr := parmblkptr
	// Turn each parameter into a ydb_string_t buffer descriptor and load it into our variadic plist
	for i = 0; i < parmcnt; i++ {
		// Fetch next parm and validate type to get a string out of it
		strparm = fmt.Sprintf("%v", rtnargs[i])
		// First initialize our ydb_string_t
		parmptr.length = C.ulong(len(strparm))
		if 0 < parmptr.length {
			parmptr.address = C.CString(strparm)
			defer C.free(unsafe.Pointer(parmptr.address))
		}
		// Now add parmptr to the variadic plist
		err = vplist.setVPlistParam(tptoken, errstr, parmIndx, uintptr(unsafe.Pointer(parmptr)))
		if nil != err {
			return "", err
		}
		// Increment parmptr to next ydb_buffer_t and the variadic list to its next slot
		parmptr = (*C.ydb_string_t)(unsafe.Pointer(uintptr(unsafe.Pointer(parmptr)) + uintptr(C.sizeof_ydb_string_t)))
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
		retval = C.GoStringN(retvalptr.address, C.int(retvalptr.length))
	} else {
		retval = ""
	}
	return retval, nil
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
func CallMT(tptoken uint64, errstr *BufferT, rtnname string, retvallen uint32, rtnargs ...interface{}) (string, error) {
	var mdesc CallMDesc

	printEntry("CallMDesc.CallMT()")
	if "" == rtnname {
		panic("YDB: Name of routine to call cannot be null string")
	}
	mdesc.SetRtnName(rtnname)
	return mdesc.CallMDescT(tptoken, errstr, retvallen, rtnargs...)
}

// MessageT is a STAPI utility function to return the error message (sans argument substitution) of a given error number.
func MessageT(tptoken uint64, errstr *BufferT, status int) (string, error) {
	var msgval BufferT

	printEntry("MessageT()")
	defer msgval.Free()
	msgval.Alloc(uint32(C.YDB_MAX_ERRORMSG))
	var cbuft *C.ydb_buffer_t
	if errstr != nil {
		cbuft = errstr.getCPtr()
	}
	rc := C.ydb_message_t(C.uint64_t(tptoken), cbuft, C.int(status), msgval.getCPtr())
	if C.YDB_OK != rc {
		err := NewError(tptoken, errstr, int(rc))
		return "", err
	}
	// Returned string should be snug in the retval buffer. Pick it out so can return it as a string
	msgptr, err := msgval.ValStr(tptoken, errstr)
	if nil != err {
		panic(fmt.Sprintf("Unexpected error with GetValStr(): %s", err))
	}
	return *msgptr, err
}

// ReleaseT is a STAPI utility function to return release information for this verison of the Golang wrapper plus
// info on the release of YottaDB itself.
func ReleaseT(tptoken uint64, errstr *BufferT) (string, error) {
	printEntry("ReleaseT()")
	zyrel, err := ValE(tptoken, errstr, "$ZYRELEASE", []string{})
	if nil != err {
		return "", err
	}
	retval := fmt.Sprintf("gowr %s %s", WrapperRelease, zyrel)
	return retval, nil
}
