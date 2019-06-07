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

package yottadb_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"lang.yottadb.com/go/yottadb"
	"testing"
)

func TestErrorErrorCode(t *testing.T) {
	// Attempt to get a non-YDB error
	err := errors.New("This is a test error")
	r := yottadb.ErrorCode(err)
	assert.Equal(t, r, -1)

	// Get a valid YDB-error
	err = yottadb.NewError(yottadb.NOTTP, nil, yottadb.YDB_ERR_INVLNPAIRLIST)
	assert.NotNil(t, err)
	r = yottadb.ErrorCode(err)
	assert.Equal(t, r, yottadb.YDB_ERR_INVLNPAIRLIST)
}

func verifyErrorCode(t *testing.T, errcode int) {
	var errstr yottadb.BufferT

	err := yottadb.NewError(yottadb.NOTTP, nil, errcode)
	assert.NotNil(t, err)
	r := yottadb.ErrorCode(err)
	assert.Equal(t, r, errcode)

	// Try this with a errstr passed in
	defer errstr.Free()
	errstr.Alloc(64)
	err = yottadb.NewError(yottadb.NOTTP, &errstr, errcode)
	assert.NotNil(t, err)
	r = yottadb.ErrorCode(err)
	assert.Equal(t, r, errcode)
}

func TestErrorNewError(t *testing.T) {
	verifyErrorCode(t, yottadb.YDB_ERR_TPRESTART)
}

func TestErrorVerifyFastPathErrorCodes(t *testing.T) {
	verifyErrorCode(t, yottadb.YDB_TP_RESTART)
	verifyErrorCode(t, yottadb.YDB_TP_ROLLBACK)
	verifyErrorCode(t, yottadb.YDB_ERR_NODEEND)
}
