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

func TestRecoverMessage(t *testing.T) {
	conn := SetupTest(t)
	assert.Equal(t, "%SYSTEM-E-ENO123, No medium found", conn.recoverMessage(123))
	assert.Equal(t, "%SYSTEM-E-ENO123, No medium found", conn.recoverMessage(-123))
	assert.PanicsWithError(t, "%YDB-E-UNKNOWNSYSERR, [2147483646] does not correspond to a known YottaDB error code", func() { conn.recoverMessage(-2147483646) })
}
