//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb

// The code in this module is necesitated by cgo restrictions. Go/cgo does not allow us to pass function pointers to Go
// functions in to C routines. Because of this we need small C wrapper routines for any such parms we are going to be passing.
// Then, we need another small C routine to fetch the address of the C wrapper routine. So init.go calls the
// ydb_get_gowrapper_panic_callback_funcvp() routine to fetch the address of the ydb_gowrapper_panic_callback() routine and
// passes the returned function pointer in to C. Then C will call ydb_gowrapper_panic_callback() which then can directly drive
// the Go routine YDBWrapperPanic().
//
// Also, if these routines are included in init.go we run into yet another known issue with Go with the compile giving duplicate
// entry point errors so these routines are placed in their own module.

// /* C routine whose address is passed at initialization time via ydb_main_lang_init() that is used when a deferred fatal
//  * signal is processed to call back into Go to do a panic. The parameter determines the type of panic to be raised
//  * with the types defined in libyottadb.h. Note this must be in a separate physical file or we get duplicate entry
//  * point compilation issues.
//  */
// extern void YDBWrapperPanicCallback(int sigtype);
// /* C routine to fetch the address of ydb_gowrapper_panic_callback */
// void *ydb_get_gowrapper_panic_callback(void);
// void *ydb_get_gowrapper_panic_callback(void)
// {
//        return (void *)&YDBWrapperPanicCallback;
// }
import "C"
