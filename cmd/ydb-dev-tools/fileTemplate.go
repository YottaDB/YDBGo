//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2019 YottaDB LLC. and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package main

var fileTemplate = `
package {{ .PackageName }}

import "unsafe"

/*
#cgo pkg-config: yottadb
#include <libyottadb.h>
#include <inttypes.h>
int {{ .FunctionName }}(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm);
int {{ .FunctionName }}_cgo(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm) {
    return {{ .FunctionName }}(tptoken, errstr, tpfnparm);
}
*/
import "C"

// Get{{ .FunctionName }}Cgo returns a pointer to the C wrapper for {{ .FunctionName }} to pass to yottadb.TpE
func Get{{ .FunctionName }}Cgo() unsafe.Pointer {
    return C.{{ .FunctionName }}_cgo
}
`
