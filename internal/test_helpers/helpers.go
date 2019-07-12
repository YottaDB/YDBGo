//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2019 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package test_helpers

import (
	"bufio"
	"bytes"
	"fmt"
	"lang.yottadb.com/go/yottadb"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// #cgo pkg-config: yottadb
// #include "libyottadb.h"
// #include "libydberrors.h"
// int TestTpRtn_cgo(uint64_t tptoken, uintptr_t in); // Forward declaration
import "C"

const VarSiz uint32 = 15                        // Max size of varname including '^'
const AryDim uint32 = 10                        // Dimension (capacity) of array of YDBBufferT structs we create here
const SubSiz uint32 = 15                        // Max length of subscripts
const ValSiz uint32 = 128                       // Max size of values
const Timeout uint64 = 10 * uint64(time.Second) // 10 second timeout (in nanoseconds)
const DebugFlag bool = false                    // Enable/Disable some simple debugging

//
// Define assert function to validate return codes and panic is assertion fails
//
func Assertnoerr(err error, t *testing.T) {
	if nil != err {
		_, file, line, ok := runtime.Caller(1)
		if ok {
			t.Fatalf("Assertion failure in %v at line %v with error: %v", file, line, err)
		} else {
			t.Fatalf("Assertion failure: %v", err)
		}
	}
}

//
// Routine to empty the database of everything currently defined in it
//
func Dbdeleteall(tptoken uint64, errstr *yottadb.BufferT, errors *int, t *testing.T) {
	var dbkey yottadb.KeyT

	defer dbkey.Free()
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	err := dbkey.Varnm.SetValStr(tptoken, nil, "^%") // Start with first possible key
	Assertnoerr(err, t)
	for {
		err = dbkey.SubNextST(tptoken, nil, dbkey.Varnm) // Find the 'next' global name
		if nil != err {
			if int(C.YDB_ERR_NODEEND) == yottadb.ErrorCode(err) {
				break
			}
			if DebugFlag {
				t.Log("FAIL - dbdeleteall failed:", err)
				(*errors)++
			}
			Assertnoerr(err, t) // Unknown error - cause panic
		}
		err = dbkey.DeleteST(tptoken, nil, C.YDB_DEL_TREE) // Delete the tree at that global
		Assertnoerr(err, t)
	}
}

//
// Routine to validate a lock exists - or not
//
func VerifyLockExists(lockvalidation []byte, errors *int, giveerror bool, t *testing.T) bool {
	var outbuff bytes.Buffer
	var outbuffB []byte
	var err error
	var varexists bool

	// Run LKE and scan result
	cmd := exec.Command(os.Getenv("ydb_dist")+"/lke", "show", "-all", "-wait")
	cmd.Stdout = &outbuff
	cmd.Stderr = &outbuff
	err = cmd.Run()
	Assertnoerr(err, t)
	outbuffB = outbuff.Bytes() // Extract the bytes from the buffer
	if !bytes.Contains(outbuffB, append(lockvalidation, " Owned"...)) {
		if giveerror {
			fmt.Printf("\nLKE output does not contain validation that the lock succeeded:%s", outbuffB)
			(*errors)++
		}
		varexists = false
	} else {
		varexists = true
	}
	outbuff.Reset()
	return varexists
}

//
// Routine to take a BufferTArray full of subscripts and turn it into a string array of the same subscripts
//
func Buftary2strary(tptoken uint64, errstr *yottadb.BufferT, buftary *yottadb.BufferTArray, t *testing.T) (*[]string, error) {
	arraylen := int(buftary.ElemUsed())
	retval := make([]string, arraylen)
	for i := 0; arraylen > i; i++ {
		subval, err := buftary.ValStr(tptoken, nil, uint32(i))
		Assertnoerr(err, t)
		retval[i] = subval
	}
	return &retval, nil
}

//
// Routine to compare two string arrays for equality. Returns true if they are the same (used length and strings)
//
func Cmpstrary(astr, bstr *[]string) bool {
	if len(*astr) != len(*bstr) {
		return false
	}
	for i, v := range *astr {
		if v != (*bstr)[i] {
			return false
		}
	}
	return true
}

//
// Routine to perform a TP transaction (TP callback routine)
//
// Note - below export statement is needed so this routine is known.
//
//export TestTpRtn
func TestTpRtn(tptoken uint64, errstr unsafe.Pointer, tpfnparm unsafe.Pointer) int {
	var dbkey yottadb.KeyT
	var dbval yottadb.BufferT
	var noset yottadb.KeyT
	var err error
	var rc int = 0

	//fmt.Println("Entered TestTpRtn")
	defer dbkey.Free()
	defer dbval.Free()
	defer noset.Free()
	if nil != tpfnparm {
		fmt.Println("Non-zero value for parameter (no idea why)")
	}
	err = yottadb.SetValE(tptoken, nil, "I am not the value you seek", "^Variable1A", []string{})
	if nil != err {
		fmt.Println("First SetE error: ", err)
	}
	dbkey.Alloc(VarSiz, AryDim, SubSiz)
	dbval.Alloc(ValSiz)
	err = dbkey.Varnm.SetValStr(tptoken, nil, "^Variable1A")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetValStr(tptoken, nil, 0, "Index0")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetValStr(tptoken, nil, 1, "Index1")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetValStr(tptoken, nil, 2, "Index2")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 3)
	Assertnoerr(err, nil)
	err = dbval.SetValStr(tptoken, nil, "The value of Variable1A")
	Assertnoerr(err, nil)
	err = dbkey.SetValST(tptoken, nil, &dbval)
	if nil != err {
		fmt.Println("First SetS error: ", err)
	}
	err = dbkey.Varnm.SetValStr(tptoken, nil, "^Variable2B")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetValStr(tptoken, nil, 0, "Idx0")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetValStr(tptoken, nil, 1, "Idx1")
	Assertnoerr(err, nil)
	err = dbval.SetValStr(tptoken, nil, "The value of Variable2B")
	Assertnoerr(err, nil)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 2)
	Assertnoerr(err, nil)
	err = dbkey.SetValST(tptoken, nil, &dbval)
	if nil != err {
		fmt.Println("Second SetS error: ", err)
		rc = yottadb.ErrorCode(err)
	}
	return rc
}

func CheckErrorExpectYDB_ERR_NODEEND(errorcode int) bool {
	if int(C.YDB_ERR_NODEEND) == errorcode {
		return true
	}
	return false
}

func CheckErrorExpectYDB_ERR_STRUCTNOTALLOCD(errorcode int) bool {
	if int(C.YDB_ERR_STRUCTNOTALLOCD) == errorcode {
		return true
	}
	return false
}

func CheckErrorExpectYDB_ERR_INSUFFSUBS(errorcode int) bool {
	if int(C.YDB_ERR_INSUFFSUBS) == errorcode {
		return true
	}
	return false
}

//export MyGoCallBack
func MyGoCallBack(tptoken uint64, tpfnarg unsafe.Pointer) int {
	// This violates TP transactions, but useful for demonstration
	fmt.Printf("Hello from MyGoCallBack!\n")
	return 0
}

func Available(name string) bool {
	return os.Getenv(name) != ""
}

// By default, we run timed tests; if we are running in a place
//  where we expect the system will be loaded, we might skip them
func RunTimedTests() bool {
	return !Available("YDB_GO_SKIP_TIMED_TESTS")
}

func SkipTimedTests(t *testing.T) {
	if RunTimedTests() {
		t.Logf("Running a timed test which may fail on a loaded system")
		return
	}
	t.Skipf("Skipping timed test which may fail on a loaded system")
}

func SkipCITests(t *testing.T) {
	if !Available("ydb_ci") {
		t.Skipf("Skipping call-in tests as ydb_ci is not configured")
	}
}

func SkipARMV7LTests(t *testing.T) {
	if os.Getenv("real_mach_type") == "armv7l" {
		t.Skipf("Some issue with arm7l processors causes this test to panic")
	}
}

func SkipMemIntensiveTests(t *testing.T) {
	// We read this as kB, so convert to MB then GB
	if GetSystemMemory(t) < (1024 * 1024) {
		t.Skipf("Machine appears to have less then 1 GB memory, skipping test")
	}
}

func GetSystemMemory(t *testing.T) int {
	file, err := os.Open("/proc/meminfo")
	Assertnoerr(err, t)
	// Skip through lines in file until we one that says "MemTotal"
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		matched, err := regexp.Match("MemTotal", []byte(scanner.Text()))
		Assertnoerr(err, t)
		if matched {
			break
		}
	}
	Assertnoerr(scanner.Err(), t)
	// Finally, read the value
	fields := strings.Fields(scanner.Text())
	if len(fields) < 2 {
		return 0
	}
	i, err := strconv.Atoi(fields[1])
	Assertnoerr(err, t)
	return i
}

func GetHeapUsage(t *testing.T) int {
	file, err := os.Open("/proc/self/status")
	Assertnoerr(err, t)
	// Skip through lines in file until we one that says "VmSize"
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		matched, err := regexp.Match("VmSize", []byte(scanner.Text()))
		Assertnoerr(err, t)
		if matched {
			break
		}
	}
	Assertnoerr(scanner.Err(), t)
	// Finally, read the value
	fields := strings.Fields(scanner.Text())
	if len(fields) < 2 {
		return 0
	}
	i, err := strconv.Atoi(fields[1])
	Assertnoerr(err, t)
	return i
}
