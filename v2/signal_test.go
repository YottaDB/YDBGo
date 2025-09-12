//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"fmt"
	"runtime/debug"
	"testing"
	"unsafe"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

// TestSigSegv tests ability to catch a SIGSEGV and do cleanup rather than exit immediately leaving a core file.
// This test is derived from a test case in the YDBGo repository: YottaDB/Lang/YDBGo#25 (comment 258495195) by @zapkub
func TestSigSegv(t *testing.T) {
	conn := NewConn()
	conn.Node("avariable").Set("a value") // dummy Set

	// Access invalid address -1 to test this because accessing nil always produces a panic regardless of SetPanicOnFault
	var p uintptr
	// The following *Sizeof(p) is needed to avoid alignment errors when trying to access badPointer when `test -race` is used
	// The 2* is to avoid "straddles multiple allocations" error when accessing the last aligned memory address during `test -race`
	var badPointer *string = (*string)(unsafe.Add(unsafe.Pointer(nil), -2*int(unsafe.Sizeof(p))))

	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true)) // No core - just a panic please
	assert.Panics(t, func() { fmt.Println(*badPointer) })
}
