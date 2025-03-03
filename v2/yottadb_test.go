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

// assert processes panic errors that occur during test.
func assert(err error) {
	if err != nil {
		panic(err)
	}
}

var randstr = make([]string, 0, 10000) // Array of random strings for use in testing
var randstrIndex = 0

// initRandstr prepares a list of many random strings.
func initRandstr() {
	if len(randstr) > 0 {
		return // early return if already filled randstr
	}
	rnd := rand.New(rand.NewChaCha8([32]byte{}))
	for range cap(randstr) {
		s := fmt.Sprintf("%x", rnd.Uint32())
		randstr = append(randstr, s)
	}
}

// Randstr fetches a random string from our cache of pre-calculated random strings.
func Randstr() string {
	randstrIndex = (randstrIndex + 1) % len(randstr)
	return randstr[randstrIndex]
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
