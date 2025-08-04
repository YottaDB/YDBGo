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
	"strings"
	"testing"

	assert "github.com/stretchr/testify/require"
	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// ---- Tests

func TestRecoverMessage(t *testing.T) {
	conn := SetupTest(t)
	assert.Equal(t, "%SYSTEM-E-ENO123, No medium found", conn.recoverMessage(123))
	assert.Equal(t, "%SYSTEM-E-ENO123, No medium found", conn.recoverMessage(-123))
	expected := "%YDB-E-UNKNOWNSYSERR, [1048576 (0x100000) returned by ydb_* C API] does not correspond to a known YottaDB error code"
	assert.PanicsWithError(t, expected, func() { conn.recoverMessage(0x100000) })

	assert.Equal(t, "YDB_TP_RESTART", conn.recoverMessage(YDB_TP_RESTART))
	assert.Equal(t, "YDB_TP_ROLLBACK", conn.recoverMessage(YDB_TP_ROLLBACK))
	assert.Equal(t, "YDB_NOTOK", conn.recoverMessage(YDB_NOTOK))
	assert.Equal(t, "YDB_LOCK_TIMEOUT", conn.recoverMessage(YDB_LOCK_TIMEOUT))
	assert.Equal(t, "YDB_DEFER_HANDLER", conn.recoverMessage(YDB_DEFER_HANDLER))
	assert.Equal(t, "%YDB-E-THREADEDAPINOTALLOWED", conn.recoverMessage(ydberr.THREADEDAPINOTALLOWED)[:28])
	assert.Equal(t, "%YDB-E-CALLINAFTERXIT", conn.recoverMessage(ydberr.CALLINAFTERXIT)[:21])

	// Generate an actual error from YDB
	_, err := conn.Str2Zwr(strings.Repeat("A", YDB_MAX_STR))
	assert.NotNil(t, err)
	lastCode := conn.lastCode()
	assert.Equal(t, "%YDB-E-MAXSTRLEN", conn.recoverMessage(lastCode)[:16])
	// Limit string length to exercise another failure code path in recoverMessage()
	conn.cconn.errstr.len_alloc = 47
	assert.Panics(t, func() { conn.recoverMessage(lastCode) })
}

func TestUnwrap(t *testing.T) {
	err := &Error{Code: ydberr.INVSTRLEN, Message: "string too long"}
	wrapper := newError(ydberr.InvalidValueType, fmt.Sprintf("wrapped: %s", err), err)
	errs := wrapper.(*Error).Unwrap()
	assert.Equal(t, len(errs), 1)
	assert.Equal(t, err, errs[0])
}

func TestLastError(t *testing.T) {
	conn := SetupTest(t)
	assert.Nil(t, conn.lastError(YDB_OK))
	assert.Equal(t, newError(YDB_TP_RESTART, "YDB_TP_RESTART"), conn.lastError(YDB_TP_RESTART))
	assert.Equal(t, newError(YDB_TP_ROLLBACK, "YDB_TP_ROLLBACK"), conn.lastError(YDB_TP_ROLLBACK))

	// Generate an actual error from YDB
	_, err := conn.Str2Zwr(strings.Repeat("A", YDB_MAX_STR))
	assert.NotNil(t, err)
	// Test that getErrorString() works
	lastCode := conn.lastCode()
	assert.Equal(t, err.Error(), conn.lastError(lastCode).Error())

	// Test code path when invalid message is returned
	conn.cconn.errstr.len_used = 2
	assert.Equal(t, ydberr.YDBMessageInvalid, conn.lastError(lastCode).(*Error).Code)
	assert.Panics(t, func() { conn.lastCode() })
	*conn.cconn.errstr.buf_addr = ','
	assert.Panics(t, func() { conn.lastCode() })

	// Clear the returned error to test code paths where that is the case
	conn.cconn.errstr.len_used = 0
	assert.Equal(t, 0, int(conn.lastCode()))
}
