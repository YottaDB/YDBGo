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

// #include <stdlib.h>
// #include <string.h>
// #include "libyottadb.h"
// int ydbTpStWrapper(uint64_t, ydb_buffer_t *, void *);
// int ydb_tp_st_wrapper_cgo(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm) {
//   return ydbTpStWrapper(tptoken, errstr, tpfnparm);
// }
import "C"
