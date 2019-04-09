//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2019 YottaDB LLC and/or its subsidiaries.	//
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
