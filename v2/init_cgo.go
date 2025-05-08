//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

/*
// The code in this module is necesitated by cgo restrictions. Go/cgo does not allow us to pass function pointers to Go
// functions in to C routines. Because of this we need small C wrapper routines for any such parms we are going to be passing.
// Then, we need another small C routine to fetch the address of the C wrapper routine. So init.go calls the
// ydb_get_gowrapper_panic_callback() routine to fetch the address of ydbWrapperPanicCallback() and
// passes the returned function pointer in to C. Then C can call the Go routine ydbWrapperPanicCallback().
// (which is itself a C wrapper for a Go routine that is created by the export of it in init.go

// This function must be in another file or we we get duplicate entry point compilation issues.
extern void ydbWrapperPanicCallback(int sigtype);

// C routine to fetch the address of ydb_gowrapper_panic_callback
void *ydb_get_gowrapper_panic_callback(void) {
  return (void *)&ydbWrapperPanicCallback;
}
*/
import "C"
