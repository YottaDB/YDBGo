//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025-2026 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

//nolint:staticcheck // ST1019: require is used for test errors, assert for test checks
import (
	"bytes"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	assert "github.com/stretchr/testify/require"  // normally assert produces an error without exiting but this makes it exit
	require "github.com/stretchr/testify/require" // for ensuring tests exit, like if err { panic }
)

// ---- Tests

func TestEnsureValueSize(t *testing.T) {
	conn := SetupTest(t)
	assert.Panics(t, func() { conn.ensureValueSize(YDB_MAX_STR + 1) })
}

// TestCalloc checks that calloc panics when C cannot allocate the requested memory.
func TestCalloc(t *testing.T) {
	SetupTest(t)
	// Use the largest size_t on any platform, which C can never allocate
	assert.PanicsWithError(t, "out of memory", func() { calloc(math.MaxUint) })
}

// TestCallCGo runs callCGo to provide test coverage as otherwise it is only run by benchmarks.
func TestCallCGo(t *testing.T) {
	SetupTest(t)
	callCGo()
}

func TestZwr2Str(t *testing.T) {
	conn := SetupTest(t)
	str, err := conn.Zwr2Str(`"X"_$C(0)_"ABC"`)
	assert.Nil(t, err)
	assert.Equal(t, str, "X\x00ABC")
	// Test InvalidZwriteFormat format error using an invalid UTF-8 character
	_, err = conn.Zwr2Str(`"X"_$C(1234567`)
	assert.NotNil(t, err)
	// Test test the same again but now exercise code path that truncates the string for the error message
	bigString := strings.Repeat("A", 200)
	_, err = conn.Zwr2Str(`"X"_$C(1234567` + bigString)
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
	_, err = conn.Str2Zwr(input + "A")
	assert.NotNil(t, err)
	_, err = conn.Str2Zwr(input + "AAAA")
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

	// Lock n in an external process and hold it until this test closes the process's stdin.
	// The spawned process reports whether its lock attempt failed by setting n=$TEST.
	n.Set("")
	cmd := exec.Command(os.Getenv("ydb_dist")+"/yottadb", "-r", "%XCMD", fmt.Sprintf("lock +%s:60 set %s=$test  read x:60  lock -%s", n, n, n))
	cmd.Stderr = os.Stderr // Make subprocess errors appear in my stdout
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	defer cmd.Wait()
	defer stdin.Close() // Defers run last in first out, so this releases the spawned process to exit before cmd.Wait()
	require.NoError(t, cmd.Start())
	// Wait for it to get locked externally
	timeout := time.Now().Add(120 * time.Second)
	for n.Get() == "" && time.Now().Before(timeout) {
		time.Sleep(10 * time.Millisecond)
	}
	if n.Get() != "1" {
		t.Fatalf("Spawned process did not get lock %s: attempt returned %q instead of \"1\"", n, n.Get())
	}
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
		fmt.Fprintf(os.Stderr, "Error running '$ydb_dist/lke show -all -wait': %#v:\n%s\n", err, outbuff.String())
		panic(err)
	}
	output := outbuff.Bytes()
	if debug {
		fmt.Printf("finding '%s' in:\n%s\n", lockpath+" Owned", string(output))
	}
	return bytes.Contains(output, []byte(lockpath+" Owned"))
}
