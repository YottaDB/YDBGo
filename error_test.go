//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.	//
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
	err = yottadb.NewError(-151027930)
	assert.NotNil(t, err)
	r = yottadb.ErrorCode(err)
	assert.Equal(t, r, -151027930)
}

func TestErrorNewError(t *testing.T) {
	err_tprestart := -150376595

	// Attempt to get a TPRESTART error, which has special handling
	err := yottadb.NewError(err_tprestart)
	r := yottadb.ErrorCode(err)
	assert.Equal(t, r, err_tprestart)
}
