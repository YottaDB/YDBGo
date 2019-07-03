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
	err = dbkey.Varnm.SetValStrLit(tptoken, nil, "^Variable1A")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, nil, 0, "Index0")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, nil, 1)
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, nil, timeout, &dbkey) // 10 second timeout
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^Variable1A(\"Index0\")"), &errors, true, t)
	err = yottadb.LockST(tptoken, nil, 0) // Release all locks
	Assertnoerr(err, t)
}

func TestSimpleAPILockManyParms(t *testing.T) {
	var errmsg string

	maxvparms := 36 // Currently hard coded in simple_api.go as a C decl.
	var locks [](*yottadb.KeyT)

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
