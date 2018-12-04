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

package yottadb

import (
	"testing"
)

func TestVariadicPlist(t *testing.T) {
	var results int
	res2 := TestVariadicPlistHelper(false, &results)
	if res2 != nil {
		t.Fatalf("Variadic test returns err %v", res2)
	}
}
