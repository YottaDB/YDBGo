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
	"flag"
	"strings"
	"testing"
	"unicode"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

var noInit bool

func init() {
	flag.BoolVar(&noInit, "noinit", false, "don't init database in yottadb_test.go startup code so as to check failure paths during YDB Init()")
}

// TestNoInit checks that Init() including its failure paths work properly.
// Note: requires an external helper program to run the test with flags: -run TestNoInit -noinit
func TestNoInit(t *testing.T) {
	if !noInit {
		return
	}

	// Make sure that Init() fails if the MinYDBRelease 'constant' is incorrectly formatted.
	originalMinYDBRelease := MinYDBRelease
	MinYDBRelease = "rX.YY" // Check that decode of this non-number panics properly
	assert.Panics(t, func() { MustInit() })
	MinYDBRelease = originalMinYDBRelease

	// Make sure that Init() fails if the release number is incorrectly formatted.
	// Do so by patching getZYRelease so that Init() uses our test string instead of $ZYRELEASE
	getZYRelease = func(conn *Conn) string { return "YottaDB v2.03" } // use v2.03 instead of r2.03
	assert.Panics(t, func() { MustInit() })
	getZYRelease = func(conn *Conn) string { return "YottaDB rA.03" } // use rA.03 instead of r2.03
	assert.Panics(t, func() { MustInit() })
	assert.Panics(t, func() { MustInit() })
	getZYRelease = func(conn *Conn) string { return "YottaDB r2.xx" } // use r2.xx instead of r2.03
	assert.Panics(t, func() { MustInit() })
	getZYRelease = func(conn *Conn) string { return "YottaDB r1.00" } // return an old YDB release
	assert.Panics(t, func() { MustInit() })
	// Test the code path that correctly interprets release numbers without a dot.
	// This time have to check the exact error messages to make sure it gets through the number decoding part,
	// but still make it produce an error so that we can re-test Init() again below without re-running the program.
	getZYRelease = func(conn *Conn) string { return "YottaDB r1" }
	assert.PanicsWithError(t, "Not running with at least minimum YottaDB release. Needed: r1.34  Have: r1.00", func() { MustInit() })

	// Test the logic that allows a dev-only build letter appended to the version number
	// Test this with a version of getZYRelease() that returns the real release number with a letter suffix
	getZYRelease = func(conn *Conn) string {
		str := conn.Node("$ZYRELEASE").Get()
		fields := strings.Fields(str)
		num := fields[1] // Fetch second field which is the release number
		if unicode.IsDigit(rune(num[len(num)-1])) {
			num = num + "x" // append a build letter
		}
		fields[1] = num
		return strings.Join(fields, " ")
	}
	Shutdown(MustInit())
	assert.Panics(t, func() { MustInit() })

}

func TestInitCheck(t *testing.T) {
	// Make sure nested shutdowns work
	assert.Equal(t, 1, int(initCount.Load()))
	db1 := MustInit()
	db2 := MustInit()
	Shutdown(db2)
	Shutdown(db1)
	assert.Equal(t, 1, int(initCount.Load()))

	// a hacky way to exercise initCheck() code coverage.
	// not safe for concurrent tests
	saved := initCount.Load()
	initCount.Store(0)
	assert.Panics(t, func() { initCheck() })
	assert.Panics(t, func() { Shutdown(dbHandle) }) // Shutdown called when not initialized
	initCount.Store(saved)
}
