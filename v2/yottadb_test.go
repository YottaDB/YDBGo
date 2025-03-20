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
	"math/rand/v2"
	"os"
	"testing"
)

var conn *Conn // global connection for use in testing

// ---- Utility functions for tests

var randstrArray = make([]string, 0, 10000) // Array of random strings for use in testing
var randstrIndex = 0

// initRandstr prepares a list of many random strings.
func initRandstr() {
	if len(randstrArray) > 0 {
		return // early return if already filled randstrArray
	}
	rnd := rand.New(rand.NewChaCha8([32]byte{}))
	for range cap(randstrArray) {
		s := fmt.Sprintf("%x", rnd.Uint32())
		randstrArray = append(randstrArray, s)
	}
}

// randstr fetches a random string from our cache of pre-calculated random strings.
func Randstr() string {
	randstrIndex = (randstrIndex + 1) % len(randstrArray)
	return randstrArray[randstrIndex]
}

func RandstrReset() {
	randstrIndex = 0
}

// multi returns multiple parameters as a single slice of interfaces.
// Useful, for example, in asserting test validity of functions that return both a value and an error.
func multi(v ...interface{}) []interface{} {
	return v
}

// ---- Initialize test system

// This benchmark is purely to provide a long name that causes benchmark outputs to align.
// It calls skip which prevents it from running.
func Benchmark________________________________(b *testing.B) {
	b.Skip()
}

// _testMain is factored out of TestMain to let us defer Init() properly
// since os.Exit() must not be run in the same function as defer.
func _testMain(m *testing.M) int {
	defer Exit(Init())
	initRandstr()
	conn = NewConn()
	return m.Run()
}

// TestMain is the entry point for tests and benchmarks.
func TestMain(m *testing.M) {
	code := _testMain(m)
	os.Exit(code) // os.Exit is the official way to exit a test suite
}
