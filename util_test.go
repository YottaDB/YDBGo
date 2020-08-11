//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2019-2020 YottaDB LLC and/or its subsidiaries.	//
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
	"fmt"
	"github.com/stretchr/testify/assert"
	"lang.yottadb.com/go/yottadb"
	"os"
	"strings"
	"testing"
)

// Note we do not test MessageT() here as it gets quite a workout in the other tests that run since it is used in
// several tests which intentionally raise errors.

func TestReleaseT(t *testing.T) {
	relText, err := yottadb.ReleaseT(yottadb.NOTTP, nil)
	assert.Nil(t, err)
	fields := strings.Fields(relText)
	assert.Equal(t, len(fields), 6) // Verify we have 6 words in the response and that the constants are correct
	assert.Equal(t, fields[0], "gowr")
	assert.Equal(t, fields[2], "YottaDB")
	// Make sure first char of YottaDB release number begins with 'r'.
	ydbRel := []byte(fields[3])
	assert.Equal(t, string(ydbRel[0]), "r")
}

func TestCallMTNoArgs(t *testing.T) {
	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	err := os.Setenv("ydb_ci", "calltab.ci")
	assert.Nil(t, err)
	// Set up ydb_routines if doesn't already have an m_routines component
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	retval, err := yottadb.CallMT(yottadb.NOTTP, nil, 64, "HelloWorld1")
	restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	assert.Nil(t, err)
	assert.Equal(t, "entry called", retval)
}

func TestCallMTWithArgs(t *testing.T) {
	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	err := os.Setenv("ydb_ci", "calltab.ci")
	assert.Nil(t, err)
	// Set up ydb_routines if doesn't already have an m_routines component
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	retval, err := yottadb.CallMT(yottadb.NOTTP, nil, 64, "HelloWorld2", "parm1", "parm2", "parm3")
	restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	assert.Nil(t, err)
	assert.Equal(t, "parm3parm2parm1", retval)
}

func TestCallMDescTNoArgs(t *testing.T) {
	var mrtn yottadb.CallMDesc

	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	err := os.Setenv("ydb_ci", "calltab.ci")
	assert.Nil(t, err)
	// Set up ydb_routines if doesn't already have an m_routines component
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	mrtn.SetRtnName("HelloWorld1")
	retval, err := mrtn.CallMDescT(yottadb.NOTTP, nil, 64)
	restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	assert.Nil(t, err)
	assert.Equal(t, "entry called", retval)
}

func TestCallMDescTWithArgs(t *testing.T) {
	var mrtn yottadb.CallMDesc

	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	err := os.Setenv("ydb_ci", "calltab.ci")
	assert.Nil(t, err)
	// Set up ydb_routines if doesn't already have an m_routines component
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	mrtn.SetRtnName("HelloWorld2")
	retval, err := mrtn.CallMDescT(yottadb.NOTTP, nil, 64, "parm1", "parm2", "parm3")
	restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	assert.Nil(t, err)
	assert.Equal(t, "parm3parm2parm1", retval)
}

func TestCallMT(t *testing.T) {
	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	err := os.Setenv("ydb_ci", "calltab.ci")
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	defer restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")

	var errstr yottadb.BufferT
	errstr.Alloc(2048)
	defer errstr.Free()
	cmpstr := "150375522,(SimpleThreadAPI),%YDB-E-INVSTRLEN, Invalid string length 20: max 15"

	/* M callin that returns 20 characters, using a 20 character buffer */
	retval, err := yottadb.CallMT(yottadb.NOTTP, &errstr, 20, "CallMTStrTest")
	if nil != err {
		panic(err)
	}
	if "a0a1a2a3a4a5a6a7a8a9" != retval {
		panic(fmt.Sprintf("CallMT() did not return the correct string. Got: %s; Expected: a0a1a2a3a4a5a6a7a8a9", retval))
	}
	if 20 != len(retval) {
		panic(fmt.Sprintf("CallMT() return is not the correct length. Got: %d; Expected: 20", len(retval)))
	}

	/* M callin that returns 20 characters, using a 15 character buffer; should return INVSTRLEN */
	retval, err = yottadb.CallMT(yottadb.NOTTP, &errstr, 15, "CallMTStrTest")
	out, _ := errstr.ValStr(yottadb.NOTTP, nil)
	errCode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSTRLEN != errCode {
		panic(fmt.Sprintf("CallMT() returned wrong ErrorCode. Got: %d; Expected: %d", errCode, yottadb.YDB_ERR_INVSTRLEN))
	}
	if out != cmpstr {
		panic(fmt.Sprintf("CallMT() returned wrong errstr. Got: %s; Expected: %s", out, cmpstr))
	}

}

func TestCallMDescT(t *testing.T) {
	// env setup
	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	err := os.Setenv("ydb_ci", "calltab.ci")
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	defer restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")

	var errstr yottadb.BufferT
	errstr.Alloc(2048)
	defer errstr.Free()
	cmpstr := "150375522,(SimpleThreadAPI),%YDB-E-INVSTRLEN, Invalid string length 20: max 15"

	var callin yottadb.CallMDesc
	callin.SetRtnName("CallMTStrTest")

	/* M callin that returns 20 characters, using a 20 character buffer */
	retval, err := callin.CallMDescT(yottadb.NOTTP, &errstr, 20)
	if nil != err {
		panic(err)
	}
	if "a0a1a2a3a4a5a6a7a8a9" != retval {
		panic(fmt.Sprintf("CallMT() did not return the correct string. Got: %s; Expected: a0a1a2a3a4a5a6a7a8a9", retval))
	}
	if 20 != len(retval) {
		panic(fmt.Sprintf("CallMT() return is not the correct length. Got: %d; Expected: 20", len(retval)))
	}

	/* M callin that returns 20 characters, using a 15 character buffer; should return INVSTRLEN */
	retval, err = callin.CallMDescT(yottadb.NOTTP, &errstr, 15)
	out, _ := errstr.ValStr(yottadb.NOTTP, &errstr)
	errCode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSTRLEN != errCode {
		panic(fmt.Sprintf("CallMT() returned wrong ErrorCode. Got: %d; Expected: %d", errCode, yottadb.YDB_ERR_INVSTRLEN))
	}
	if out != cmpstr {
		panic(fmt.Sprintf("CallMT() returned wrong errstr. Got: %s; Expected: %s", out, cmpstr))
	}
}

func TestCallMTab(t *testing.T) {
	var errstr yottadb.BufferT
	var err error

	// Environment setup
	envvarSave := make(map[string]string)
	saveEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")
	//err = os.Setenv("ydb_ci", "calltab.ci")
	includeInEnvvar(t, "ydb_routines", "./m_routines")
	defer restoreEnvvars(t, &envvarSave, "ydb_ci", "ydb_routines")

	// Note, we did not set ydb_ci - open first calltable directly
	calltabTable, err := yottadb.CallMTableOpenT(yottadb.NOTTP, &errstr, "calltab.ci")
	assert.Nil(t, err)
	_, err = calltabTable.CallMTableSwitchT(yottadb.NOTTP, &errstr)
	assert.Nil(t, err)
	retval, err := yottadb.CallMT(yottadb.NOTTP, nil, 64, "HelloWorld1")
	assert.Nil(t, err)
	assert.Equal(t, "entry called", retval)
	// Try to invoke our test routine but expect error since it does not exist in this calltab
	retval, err = yottadb.CallMT(yottadb.NOTTP, nil, 64, "HelloWorld99")
	assert.NotNil(t, err)
	assert.Equal(t, yottadb.YDB_ERR_CINOENTRY, yottadb.ErrorCode(err))
	// Now open the new package and make it current
	newtabTable, err := yottadb.CallMTableOpenT(yottadb.NOTTP, &errstr, "testcalltab.ci")
	assert.Nil(t, err)
	savedTable, err := newtabTable.CallMTableSwitchT(yottadb.NOTTP, &errstr)
	assert.Nil(t, err)
	// And try running our program again
	retval, err = yottadb.CallMT(yottadb.NOTTP, nil, 64, "HelloWorld99")
	assert.Nil(t, err)
	assert.Equal(t, "entry was called", retval)
	// Validate we can switch back and re-run HelloWorld1
	_, err = savedTable.CallMTableSwitchT(yottadb.NOTTP, &errstr)
	assert.Nil(t, err)
	retval, err = yottadb.CallMT(yottadb.NOTTP, nil, 64, "HelloWorld1")
	assert.Nil(t, err)
	assert.Equal(t, "entry called", retval)
}
