//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.	//
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
	. "lang.yottadb.com/go/yottadb/internal/test_helpers"
	"testing"
	"sync"
	"fmt"
)

func TestSimpleAPILockST(t *testing.T) {
	var dbkey yottadb.KeyT
	var tptoken uint64 = yottadb.NOTTP
	var err error
	var timeout uint64
	var errors int

	dbkey.Alloc(VarSiz, AryDim, SubSiz) // Reallocate the key
	err = dbkey.Varnm.SetValStrLit(tptoken, "^Variable1A")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetValStrLit(tptoken, 0, "Index0")
	Assertnoerr(err, t)
	err = dbkey.Subary.SetElemUsed(tptoken, 1)
	Assertnoerr(err, t)
	err = yottadb.LockST(tptoken, timeout, &dbkey) // 10 second timeout
	Assertnoerr(err, t)
	VerifyLockExists([]byte("^Variable1A(\"Index0\")"), &errors, true, t)
	err = yottadb.LockST(tptoken, 0) // Release all locks
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

	err := yottadb.LockST(yottadb.NOTTP, 0, locks...)
	assert.NotNil(t, err)
	errmsg = err.Error()
	expectederrmsg := "%YDB-E-NAMECOUNT2HI, Number of varnames (namecount parameter in a LockST() call) exceeds maximum (11)"
	assert.Equal(t, errmsg, expectederrmsg)
}

func TestSimpleAPITpFullNesting(t *testing.T) {
	var wg sync.WaitGroup
	hit_tp_too_deep := 0
	var fn func(string, uint64) error
	fn = func(myId string, tptoken uint64) error {
		return yottadb.TpE2(tptoken, func(tptoken uint64) int {
			curTpLevel, err := yottadb.ValE(tptoken, "$TLEVEL", []string{})
			yottadb.Assertnoerror(err)
			err = yottadb.SetValE(tptoken, "", "^x", []string{myId, curTpLevel})
			yottadb.Assertnoerror(err)
			err = fn(myId, tptoken)
			if nil != err {
				errcode := yottadb.ErrorCode(err)
				if errcode == yottadb.YDB_ERR_TPTOODEEP {
					hit_tp_too_deep++
				} else {
					assert.Fail(t, "Error other than TPTOODEEP")
				}
				return errcode
			}
			return 0
		}, "BATCH", []string{})
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fn(fmt.Sprintf("%d", i), yottadb.NOTTP)
		}(i)
	}
	wg.Wait()
	assert.Equal(t, 100 * 126, hit_tp_too_deep)
}
