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
// The gateway function
#include <libyottadb.h>
#include <inttypes.h>
int TestTpRtn(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm);
int TestTpRtn_cgo(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm)
{
	return TestTpRtn(tptoken, errstr, tpfnparm);
}
*/
import "C"

func TpRtn_cgo() unsafe.Pointer {
	return C.TestTpRtn_cgo
}
