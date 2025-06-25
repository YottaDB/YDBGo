//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestForceInit(t *testing.T) {
	Init()
	// Verify that Init() turns on ydbInitialized flag
	assert.Equal(t, uint32(1), atomic.LoadUint32(&ydbInitialized))
	// Turn it off manually and verify that
	atomic.StoreUint32(&ydbInitialized, 0)                         // YottaDB wrapper is now initialized
	assert.Equal(t, uint32(0), atomic.LoadUint32(&ydbInitialized)) // Verify that Init turns it on
	// Verify that ForceInit() turns it on
	ForceInit()
	assert.Equal(t, uint32(1), atomic.LoadUint32(&ydbInitialized)) // Verify that Init turns it on
}
