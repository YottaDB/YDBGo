//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2022 YottaDB LLC and/or its subsidiaries.	//
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
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"sync"
	"testing"
	"time"
)

// Allow for 30% difference between Expected and Actual in timed tests.
// In testing, we have seen a difference up to 23% show up some times in the "miscGoTimersHelper()" timed test.
// Hence the current choice of 30% in the "allowedErrorPct" variable.
var allowedErrorPct float64 = 0.30

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
	panic(err)
}

func miscGoTimersHelper(t *testing.T, wg *sync.WaitGroup, loops int) {
	wg.Add(1)
	go func() {
		for i := 0; i < loops; i++ {
			start := time.Now()
			r, err := yottadb.CallMT(yottadb.NOTTP, nil, 0, "TestMGoTimers")
			assert.Nil(t, err)
			elapsed := time.Since(start)
			assert.InEpsilon(t, 1, elapsed.Seconds(), allowedErrorPct)
			assert.Equal(t, "", r)
		}
		wg.Done()
	}()
}

func TestMiscGoTimers(t *testing.T) {
	// Verify that Go timers do not interfere with YDB timers; kick off a thread
	//  which invokes a M routine that sleeps 100ms 10 times, kick off 10 go routines
	//  which sleep for 100ms
	var wg sync.WaitGroup
	SkipTimedTests(t)
	SkipCITests(t)
	miscGoTimersHelper(t, &wg, 2)
	sleepDuration, e := time.ParseDuration("100ms")
	assert.Nil(t, e)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 20; j++ {
				start := time.Now()
				time.Sleep(sleepDuration)
				elapsed := time.Since(start)
				assert.InEpsilon(t, .1, elapsed.Seconds(), allowedErrorPct)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestMiscGoSelectWithYdbTimers(t *testing.T) {
	// Verify Go channels do not interfere with YDB timers
	var wg sync.WaitGroup
	SkipTimedTests(t)
	SkipCITests(t)
	miscGoTimersHelper(t, &wg, 10)
	// Spawn off consume-producer routines, sending 100 messages at 10ms intervals (10s test)
	sleepDuration, e := time.ParseDuration("10ms")
	recvCount := 0
	ch := make(chan int)
	assert.Nil(t, e)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			ch <- i
			time.Sleep(sleepDuration)
		}
		close(ch)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for range ch {
			recvCount++
		}
		wg.Done()
	}()
	wg.Wait()
	assert.Equal(t, 100, recvCount)
}

func TestMiscGoSelectWithYdbTimers2(t *testing.T) {
	// Verify Go select/channels do not interfere with YDB timers
	var wg sync.WaitGroup
	SkipTimedTests(t)
	SkipCITests(t)
	miscGoTimersHelper(t, &wg, 10)
	// Spawn off consume-producer routines, sending 100 messages at 10ms intervals (10s test)
	sleepDuration, e := time.ParseDuration("10ms")
	recvCount := 0
	ch := make(chan int)
	assert.Nil(t, e)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			ch <- i
			time.Sleep(sleepDuration)
		}
		ch <- -1
		close(ch)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for true {
			done := false
			select {
			case x := <-ch:
				if x == -1 {
					done = true
					break
				}
				recvCount++
			default:
				continue
			}
			if done {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	assert.Equal(t, 100, recvCount)
}

func TestMiscGoSelectWithYdbTimers3(t *testing.T) {
	// Verify Go select/channels/time.After does not interfere with YDB timers
	var wg sync.WaitGroup
	SkipTimedTests(t)
	SkipCITests(t)
	miscGoTimersHelper(t, &wg, 10)
	// Spawn off consume-producer routines, sending 100 messages at 10ms intervals (10s test)
	sleepDuration, e := time.ParseDuration("10ms")
	recvCount := 0
	ch := make(chan int)
	assert.Nil(t, e)
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			ch <- i
			time.Sleep(sleepDuration)
		}
		ch <- -1
		close(ch)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for true {
			done := false
			select {
			case x := <-ch:
				if x == -1 {
					done = true
					break
				}
				recvCount++
			case <-time.After(5 * time.Millisecond):
				continue
			}
			if done {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()
	assert.Equal(t, 100, recvCount)
}

// Test the 4 timer global variable timer values to make sure they can be accessed and updated.
func testTimerParameter(t *testing.T, waitValue *int, defaultValue int) {
	assert.Equal(t, *waitValue, defaultValue)   // Expect it to be its default value initially
	*waitValue++                                // Bump it by a second
	assert.Equal(t, *waitValue, defaultValue+1) // Verify the update worked
}

func TestMaximumNormalExitWait(t *testing.T) {
	testTimerParameter(t, &yottadb.MaximumNormalExitWait, yottadb.DefaultMaximumNormalExitWait)
}

func TestMaximumPanicExitWait(t *testing.T) {
	testTimerParameter(t, &yottadb.MaximumPanicExitWait, yottadb.DefaultMaximumPanicExitWait)
}

func TestMaximumCloseWait(t *testing.T) {
	testTimerParameter(t, &yottadb.MaximumCloseWait, yottadb.DefaultMaximumCloseWait)
}

func TestMaximumSigAckWait(t *testing.T) {
	testTimerParameter(t, &yottadb.MaximumSigAckWait, yottadb.DefaultMaximumSigAckWait)
}
