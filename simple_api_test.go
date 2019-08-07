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

package yottadb_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"lang.yottadb.com/go/yottadb"
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestSimpleAPILockST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var timeout uint64
	var errors int

	dbkey.Alloc(VarSiz, AryDim, SubSiz) // Reallocate the key
	err = dbkey.Varnm.SetValStr(tptoken, nil, "^Variable1A")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStr(tptoken, nil, 0, "Index0")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, timeout, &dbkey) // 10 second timeout
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^Variable1A(\"Index0\")"), &errors, true, t)
	err = yottadb.LockST(tptoken, nil, 0) // Release all locks
	Assertnoerr(err, t)
}

func TestSimpleAPILockSTErrors(t *testing.T) {
	var dbkey yottadb.KeyT
	var errstr yottadb.BufferT
	var tptoken uint64 = yottadb.NOTTP
	var err error

	errstr.Alloc(128)

	// YDB_ERR_INVVARNAME
	dbkey.Alloc(1, 0, 0)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "^")
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, 0, &dbkey)
	errcode := yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVVARNAME != errcode {
		t.Error("The LockST() errorcode for ^ expected to be", yottadb.YDB_ERR_INVVARNAME, "but was", errcode)
	}
	// YDB_ERR_UNIMPLOP
	dbkey.Alloc(8, 0, 0)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "$ZCHSET")
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, 0, &dbkey)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_UNIMPLOP != errcode {
		t.Error("The LockST() errorcode for $ZCHSET expected to be", yottadb.YDB_ERR_UNIMPLOP, "but was", errcode)
	}
	// YDB_ERR_INVSVN
	dbkey.Alloc(16, 0, 0)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "$NOTATHING")
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, 0, &dbkey)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_INVSVN != errcode {
		t.Error("The LockST() errorcode for $NOTATHING expected to be", yottadb.YDB_ERR_INVSVN, "but was", errcode)
	}
	// YDB_ERR_VARNAME2LONG
	dbkey.Alloc(64, 0, 0)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "a1a2a3a4a5a6a7a8a9a0b1b2b3b4b5b6b7b8b9b0")
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, 0, &dbkey)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_VARNAME2LONG != errcode {
		t.Error("The LockST() errorcode for a too long VarName expected to be", yottadb.YDB_ERR_VARNAME2LONG, "but was", errcode)
	}
	// YDB_ERR_MAXNRSUBSCRIPTS
	dbkey.Alloc(1, 35, 2)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "a")
	Assertnoerr(err, t)
	for i := 0; i < 35; i++ {
		err = dbkey.Subary.SetValStr(tptoken, &errstr, uint32(i), strconv.Itoa(i))
		Assertnoerr(err, t)
	}
	err = dbkey.Subary.SetElemUsed(tptoken, &errstr, 35)
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, 0, &dbkey)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_MAXNRSUBSCRIPTS != errcode {
		t.Error("The LockST() errorcode for node with 35 subscripts expected to be", yottadb.YDB_ERR_MAXNRSUBSCRIPTS, "but was", errcode)
	}
	// YDB_ERR_TIME2LONG
	dbkey.Alloc(1, 0, 0)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "a")
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, yottadb.YDB_MAX_TIME_NSEC+1, &dbkey)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_TIME2LONG != errcode {
		t.Error("The LockST() errorcode for timeout of YDB_MAX_TIME_NSEC+1 expected to be", yottadb.YDB_ERR_TIME2LONG, "but was", errcode)
	}
	// YDB_ERR_NAMECOUNT2HI
	dbkey.Alloc(1, 0, 0)
	err = dbkey.Varnm.SetValStr(tptoken, &errstr, "a")
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, 0, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey, &dbkey)
	errcode = yottadb.ErrorCode(err)
	if yottadb.YDB_ERR_NAMECOUNT2HI != errcode {
		t.Error("The LockE() errorcode for 12 locks expected to be", yottadb.YDB_ERR_NAMECOUNT2HI, "but was", errcode)
	}
}

func TestSimpleAPILockManyParms(t *testing.T) {
	var errmsg string
	var locks [](*yottadb.KeyT)

	maxvparms := 36 // Currently hard coded in simple_api.go as a C decl.

	locks = make([](*yottadb.KeyT), maxvparms+1)

	for i := 0; i < maxvparms+1; i++ {
		var t yottadb.KeyT
		t.Alloc(31, 31, 64)
		locks[i] = &t
	}

	err := yottadb.LockST(yottadb.NOTTP, nil, 0, locks...)
	assert.NotNil(t, err)
	errmsg = err.Error()
	expectederrmsg := "%YDB-E-NAMECOUNT2HI, Number of varnames specified as the namecount parameter in a LockST() call (37) exceeds the maximum (10)"
	// For 32 bit, our 64 bit parms take up more space so reduce the maximum by 1
	if 32 == strconv.IntSize {
		expectederrmsg = strings.Replace(errmsg, "(10)", "(9)", 1)
	}
	assert.Equal(t, expectederrmsg, errmsg)
}

func TestSimpleAPITpFullNesting(t *testing.T) {
	var wg sync.WaitGroup

	hit_tp_too_deep := 0
	var fn func(string, uint64) error
	fn = func(myId string, tptoken uint64) error {
		return yottadb.TpE(tptoken, nil, func(tptoken uint64, errstr *yottadb.BufferT) int32 {
			curTpLevel, err := yottadb.ValE(tptoken, nil, "$TLEVEL", []string{})
			Assertnoerr(err, t)
			err = yottadb.SetValE(tptoken, nil, "", "^x", []string{myId, curTpLevel})
			Assertnoerr(err, t)
			err = fn(myId, tptoken)
			if nil != err {
				errcode := yottadb.ErrorCode(err)
				if errcode == yottadb.YDB_ERR_TPTOODEEP {
					hit_tp_too_deep++
				} else {
					assert.Fail(t, "Error other than TPTOODEEP")
				}
				return int32(errcode)
			}
			return 0
		}, "BATCH", []string{})
	}
	// checks the machine archtecture for armv6l and armv7l and reduces the number of routines created
	var routines_to_make int
	march, _ := exec.Command("uname", "-m").Output()
	if string(march[:]) == "armv7l\n" || string(march[:]) == "armv6l\n" {
		routines_to_make = 10
	} else {
		routines_to_make = 100
	}
	for i := 0; i < routines_to_make; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fn(fmt.Sprintf("%d", i), yottadb.NOTTP)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, routines_to_make*126, hit_tp_too_deep)
}
