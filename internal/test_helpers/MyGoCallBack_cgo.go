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

package test_helpers

import "unsafe"

/*
#include <libyottadb.h>
#include <inttypes.h>
int MyGoCallBack(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm);
int MyGoCallBack_cgo(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm) {
    return MyGoCallBack(tptoken, errstr, tpfnparm);
}
*/
import "C"

func GetMyGoCallBackCgo() unsafe.Pointer {
	return C.MyGoCallBack_cgo
}
