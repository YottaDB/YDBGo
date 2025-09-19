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
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func TestInitCheck(t *testing.T) {
	// Make sure nested shutdowns work
	assert.Equal(t, 1, int(initCount.Load()))
	db1 := MustInit()
	db2 := MustInit()
	Shutdown(db2)
	Shutdown(db1)
	assert.Equal(t, 1, int(initCount.Load()))

	// a hacky way to exercise initCheck() code coverage.
	// not safe for concurrent tests
	saved := initCount.Load()
	initCount.Store(0)
	assert.Panics(t, func() { initCheck() })
	assert.Panics(t, func() { Shutdown(dbHandle) }) // Shutdown called when not initialized
	initCount.Store(saved)
}
