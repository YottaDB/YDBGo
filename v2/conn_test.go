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
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func TestEnsureValueSize(t *testing.T) {
	conn := SetupTest(t)
	assert.Panics(t, func() { conn.ensureValueSize(YDB_MAX_STR + 1) })
}

func TestZwr2Str(t *testing.T) {
	conn := SetupTest(t)
	str, err := conn.Zwr2Str(`"X"_$C(0)_"ABC"`)
	assert.Nil(t, err)
	assert.Equal(t, str, "X\x00ABC")
	// Test InvalidZwriteFormat format error
	_, err = conn.Zwr2Str(`"X"_$C(1234`)
	assert.NotNil(t, err)
	// Test test the same again but now exercise code path that truncates the string for the error message
	bigString := strings.Repeat("A", 200)
	_, err = conn.Zwr2Str(`"X"_$C(1234` + bigString)
	assert.NotNil(t, err)
	bigString = strings.Repeat("A", YDB_MAX_STR-2)
	_, err = conn.Zwr2Str(`"` + bigString + `"`)
	assert.Nil(t, err)
	_, err = conn.Zwr2Str(`"` + bigString + `A"`)
	assert.NotNil(t, err)
}

func TestStr2Zwr(t *testing.T) {
	conn := SetupTest(t)
	str, err := conn.Str2Zwr("X\x00ABC")
	assert.Nil(t, err)
	assert.Equal(t, str, `"X"_$C(0)_"ABC"`)

	// Make sure ZWrite string is longer than input string by at least overalloc to ensure reallocation code gets traversed
	input := strings.Repeat("A\x00", overalloc)
	_, err = conn.Str2Zwr(input)
	assert.Nil(t, err)

	// Test maximum length strings
	input = strings.Repeat("A", YDB_MAX_STR-2)
	str, err = conn.Str2Zwr(input)
	assert.Nil(t, err)
	assert.Equal(t, `"`+input+`"`, str)
	str, err = conn.Str2Zwr(input + "A")
	assert.NotNil(t, err)
	str, err = conn.Str2Zwr(input + "AAAA")
	assert.NotNil(t, err)

	assert.Panics(t, func() { conn.Quote(input + "\x00") })
}

func TestKillLocalsExcept(t *testing.T) {
	conn := SetupTest(t)
	n1 := conn.Node("var1")
	n2 := conn.Node("var2")
	n3 := conn.Node("var3")
	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	n3.Child("sub1").Set("subval")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasValueOnly(), n3.HasBoth()))
	conn.KillLocalsExcept("var1", "var3")
	assert.Equal(t, multi(true, true, true), multi(n1.HasValueOnly(), n2.HasNone(), n3.HasBoth()))
	conn.KillLocalsExcept()
	assert.Equal(t, multi(true, true, true), multi(n1.HasNone(), n2.HasNone(), n3.HasNone()))
	assert.Panics(t, func() { conn.KillLocalsExcept("$asdf") })

	n1.Set("v1")
	n2.Set("v2")
	n3.Set("v3")
	conn.KillAllLocals()
	assert.Equal(t, multi(true, true, true), multi(n1.HasNone(), n2.HasNone(), n3.HasNone()))
}

func TestLock(t *testing.T) {
	conn := SetupTest(t)
	n := conn.Node("^var", "Don't", "Panic!")
	// Increment lock 3 times
	assert.Equal(t, true, n.Lock(100*time.Millisecond))
	assert.Equal(t, true, n.Lock(100*time.Millisecond))
	assert.Equal(t, true, n.Lock(100*time.Millisecond))

	// Check that lock now exists
	lockpath := n.String()
	assert.Equal(t, true, lockExists(lockpath))

	// Decrement 3 times and each time check whether lock exists
	n.Unlock()
	assert.Equal(t, true, lockExists(lockpath))
	n.Unlock()
	assert.Equal(t, true, lockExists(lockpath))
	n.Unlock()
	assert.Equal(t, false, lockExists(lockpath))

	// Now lock two paths and check that Lock(0) releases them
	n2 := conn.Node("^var2")
	n.Lock()
	n2.Lock()
	assert.Equal(t, true, lockExists(n.String()))
	assert.Equal(t, true, lockExists(n2.String()))
	assert.Equal(t, true, conn.Lock(0)) // Release all locks
	assert.Equal(t, false, lockExists(n.String()))
	assert.Equal(t, false, lockExists(n2.String()))

	// Now lock both using Lock() and make sure they get locked and unlocked
	assert.Equal(t, true, conn.Lock(100*time.Millisecond, n, n2)) // Release all locks
	assert.Equal(t, true, lockExists(n.String()))
	assert.Equal(t, true, lockExists(n2.String()))
	assert.Equal(t, true, conn.Lock(time.Duration(0))) // Release all locks
	assert.Equal(t, false, lockExists(n.String()))
	assert.Equal(t, false, lockExists(n2.String()))

	// Lock node n in an external process for n seconds so that we can test its lock timeout here.
	cmd := exec.Command(os.Getenv("ydb_dist")+"/yottadb", "-r", "%XCMD", fmt.Sprintf("lock +%s:1 hang 1 lock -%s", n, n))
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	// Wait for it to get locked externally
	for locked := n.Lock(0); locked; locked = n.Lock(0) {
		n.Unlock()
	}
	// Test that a lock timeout works
	assert.Equal(t, false, n.Lock(10*time.Millisecond))
}

// lockExists return whether a lock exists using YottaDB's LKE utility.
func lockExists(lockpath string) bool {
	const debug = false // set true to print output of LKE command
	var outbuff bytes.Buffer

	// Run LKE and scan result
	cmd := exec.Command(os.Getenv("ydb_dist")+"/lke", "show", "-all", "-wait")
	cmd.Stdout = &outbuff
	cmd.Stderr = &outbuff
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	output := outbuff.Bytes()
	if debug {
		fmt.Printf("finding '%s' in:\n%s\n", lockpath+" Owned", string(output))
	}
	return bytes.Contains(output, []byte(lockpath+" Owned"))
}
