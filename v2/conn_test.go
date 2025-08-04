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
	"time"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func TestEnsureValueSize(t *testing.T) {
	tconn := SetupTest(t)
	assert.Panics(t, func() { tconn.ensureValueSize(YDB_MAX_STR + 1) })
}

func TestZwr2Str(t *testing.T) {
	tconn := SetupTest(t)
	str, err := tconn.Zwr2Str(`"X"_$C(0)_"ABC"`)
	assert.Nil(t, err)
	assert.Equal(t, str, "X\x00ABC")
	_, err = tconn.Zwr2Str(`"` + strings.Repeat("A", YDB_MAX_STR-2) + `"`)
	assert.Nil(t, err)
	_, err = tconn.Zwr2Str(`"` + strings.Repeat("A", YDB_MAX_STR-1) + `"`)
	assert.NotNil(t, err)
}

func TestStr2Zwr(t *testing.T) {
	tconn := SetupTest(t)
	str, err := tconn.Str2Zwr("X\x00ABC")
	assert.Nil(t, err)
	assert.Equal(t, str, `"X"_$C(0)_"ABC"`)
	input := strings.Repeat("A", YDB_MAX_STR-2)
	str, err = tconn.Str2Zwr(input)
	assert.Nil(t, err)
	assert.Equal(t, `"`+input+`"`, str)
	str, err = tconn.Str2Zwr(input + "A")
	assert.NotNil(t, err)

	assert.Panics(t, func() { tconn.Quote(input + "\x00") })
}

func TestKillLocalsExcept(t *testing.T) {
	tconn := SetupTest(t)
	n1 := tconn.Node("var1")
	n2 := tconn.Node("var2")
	n3 := tconn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasValueOnly(), n3.HasBoth()))
	tconn.KillLocalsExcept("var1", "var3")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasBoth()))
	tconn.KillLocalsExcept()
	assert.Equal(t, multi(true, true, true), multi(n1.HasNone(), n2.HasNone(), n3.HasNone()))

	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	tconn.KillAllLocals()
	assert.Equal(t, multi(true, true, true), multi(n1.HasNone(), n2.HasNone(), n3.HasNone()))
}

func TestLock(t *testing.T) {
	tconn := SetupTest(t)
	n := tconn.Node("^var", "Don't", "Panic!")
	// Increment lock 3 times
	assert.Equal(t, true, n.Lock(100*time.Millisecond))
	assert.Equal(t, true, n.Lock(100*time.Millisecond))
	assert.Equal(t, true, n.Lock(100*time.Millisecond))

	// Check that lock now exists
	lockpath := fmt.Sprint(n)
	assert.Equal(t, true, lockExists(lockpath))

	// Decrement 3 times and each time check whether lock exists
	n.Unlock()
	assert.Equal(t, true, lockExists(lockpath))
	n.Unlock()
	assert.Equal(t, true, lockExists(lockpath))
	n.Unlock()
	assert.Equal(t, false, lockExists(lockpath))

	// Now lock two paths and check that Lock(0) releases them
	n2 := tconn.Node("^var2")
	n.Lock()
	n2.Lock()
	assert.Equal(t, true, lockExists(fmt.Sprint(n)))
	assert.Equal(t, true, lockExists(fmt.Sprint(n2)))
	assert.Equal(t, true, tconn.Lock(0)) // Release all locks
	assert.Equal(t, false, lockExists(fmt.Sprint(n)))
	assert.Equal(t, false, lockExists(fmt.Sprint(n2)))

	// Now lock both using Lock() and make sure they get locked and unlocked
	assert.Equal(t, true, tconn.Lock(100*time.Millisecond, n, n2)) // Release all locks
	assert.Equal(t, true, lockExists(fmt.Sprint(n)))
	assert.Equal(t, true, lockExists(fmt.Sprint(n2)))
	assert.Equal(t, true, tconn.Lock(time.Duration(0))) // Release all locks
	assert.Equal(t, false, lockExists(fmt.Sprint(n)))
	assert.Equal(t, false, lockExists(fmt.Sprint(n2)))
}
