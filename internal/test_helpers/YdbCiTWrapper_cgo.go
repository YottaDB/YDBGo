//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.     //
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package test_helpers

// #include "libyottadb.h"
// #include "libydberrors.h"
// int TestTpRtn_cgo(uint64_t tptoken, ydb_buffer_t *errmsg, uintptr_t in); // Forward declaration
// void ydb_ci_t_wrapper(unsigned long tptoken, ydb_buffer_t *errmsg, char *name, ydb_string_t *arg) {
//     ydb_ci_t((uint64_t)tptoken, errmsg, name, arg);
// }
import "C"
