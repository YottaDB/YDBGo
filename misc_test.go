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
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"sync"
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

// This is actually testing an internal test-helper feature
func TestYDBCi(t *testing.T) {
	if !Available("ydb_ci") {
		t.Skipf("Skipping call-in tests as ydb_ci is not configured")
	}
	r := YDBCi(yottadb.NOTTP, true, "hello^helloM", "World")
	assert.Equal(t, r, "World")
}

func TestMiscGoTimers(t *testing.T) {
	// Verify that Go timers do not interfere with YDB timers; kick off a thread
	//  which invokes a M routine that sleeps 100ms 10 times, kick off 10 go routines
	//  which sleep for 100ms
	var wg sync.WaitGroup
	if !Available("ydb_ci") {
		t.Skipf("Skipping call-in tests as ydb_ci is not configured")
	}
	wg.Add(1)
	go func() {
		for i := 0; i < 2; i++ {
			start := time.Now()
			r := YDBCi(yottadb.NOTTP, false, "run^TestMiscGoTimers")
			elapsed := time.Since(start)
			// This test failed on a loaded system with a 11% insteasd
			//  of the allowed 10 % on 2019-01-01, if it continues to fail
			//  we might consider adopting a strategy of "retrying" the
			//  first timeout failure, then steadily increasing it until
			//  the test passes
			//  (i.e., test for 1s with 10% delta, then 2s with 10% delta
			//   then 4s with 10% delta, etc.)
			assert.InEpsilon(t, 1, elapsed.Seconds(), .1)
			assert.Equal(t, "", r)
		}
		wg.Done()
	}()
	sleepDuration, e := time.ParseDuration("100ms")
	assert.Nil(t, e)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 20; j++ {
				start := time.Now()
				time.Sleep(sleepDuration)
				elapsed := time.Since(start)
				assert.InEpsilon(t, .1, elapsed.Seconds(), .1)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
