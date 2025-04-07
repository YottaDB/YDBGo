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

package main

import (
	"fmt"
	"os"
	"strconv"

	"lang.yottadb.com/go/yottadb/v2"
)

func main() {
	iterations, _ := strconv.Atoi(os.Args[1])

	defer yottadb.Exit(yottadb.Init())
	db := yottadb.NewConn()
	n := db.Node("x") // or: db.New("varname", "sub1", "sub2")

	// Iterate the SET command to benchmark it
	var i int
	for i = range iterations {
		n.Set(strconv.Itoa(i))
	}

	// Read data back to verify that the correct data went into the database
	str, err := n.Get("Default")
	if err != nil {
		panic(err)
	}
	if str != strconv.Itoa(i) {
		panic(fmt.Errorf("Expected result %d but got %s", i, str))
	}
}
