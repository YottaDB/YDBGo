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
	"lang.yottadb.com/go/yottadb"
//	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"testing"
	"github.com/stretchr/testify/assert"
	"errors"
)

func TestMiscIsLittleEndian(t *testing.T) {
	// For now, we only see this getting run on LittleEndian machines, so just verify
	//  true
	assert.True(t, yottadb.IsLittleEndian())
}

func TestMiscAssertnoerror(t *testing.T) {
	err := errors.New("This is a test error")

	defer func() {
		r := recover()
		assert.NotNil(t, r)
	}()

	yottadb.Assertnoerror(err)
	t.Errorf("We should have never gotten here")
}
