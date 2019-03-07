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
	"lang.yottadb.com/go/yottadb"
)

// Example demonstrating the most basic features of YottaDB using the simple API;
// setting a value, getting a value, iterating through values,
// and deleting a value.
//
// The SimpleAPI is somewhat more difficult to use than the EasyAPI, but is more
// performant. It is recommended to use the SimpleAPI if you are building a
// performance critical application.
func Example_simpleAPI() {
	// Allocate a key to set our value equal too
	var key1 yottadb.KeyT
	var buff1, cur_sub yottadb.BufferT
	var tptoken uint64
	var val1 *string
	var err error

	// The tptoken argument to many functions is either a value passed into the
	//  callback routine for TP, or yottadb.NOTTP if not in a transaction
	tptoken = yottadb.NOTTP

	// Set global node ["^hello", "world"] to "Go World"

	// When using the simple API, you MUST always defer the Free of each structure,
	//  as it allocates C memory which Go doesn't know to free!
	defer key1.Free()
	key1.Alloc(64, 10, 64)
	err = key1.Varnm.SetValStrLit(tptoken, nil, "^hello")
	if err != nil {
		panic(err)
	}
	err = key1.Subary.SetElemUsed(tptoken, nil, 1)
	if err != nil {
		panic(err)
	}
	err = key1.Subary.SetValStrLit(tptoken, nil, 0, "world")
	if err != nil {
		panic(err)
	}

	// Create a buffer which is used to specify the value we will be setting the global to
	// Reminder, you MUST always defer Free of each structure you allocate
	defer buff1.Free()
	buff1.Alloc(64)
	err = buff1.SetValStrLit(tptoken, nil, "Go world")
	if err != nil {
		panic(err)
	}

	// Set the value
	err = key1.SetValST(tptoken, nil, &buff1)
	if err != nil {
		panic(err)
	}

	// Retrieve the value that was set
	//  We can reuse the KeyT we already made for setting the value; hence part
	//  of the performance gain
	// For the sake of demonstration, we will first clear the buffer we used to set the
	//  value
	buff1.Alloc(64)
	val1, err = buff1.ValStr(tptoken, nil)
	if err != nil {
		panic(err)
	}
	if (*val1) != "" {
		panic("Buffer not empty when it should be!")
	}
	err = key1.ValST(tptoken, nil, &buff1)
	if err != nil {
		panic(err)
	}
	val1, err = buff1.ValStr(tptoken, nil)
	if (*val1) != "Go world" {
		panic("Value not what was expected; did someone else set something?")
	}

	// Set a few more nodes so we can iterate through them
	err = key1.Subary.SetValStrLit(tptoken, nil, 0, "shire")
	if err != nil {
		panic(err)
	}
	err = buff1.SetValStrLit(tptoken, nil, "Go Middle Earth")
	if err != nil {
		panic(err)
	}
	err = key1.SetValST(tptoken, nil, &buff1)
	if err != nil {
		panic(err)
	}

	err = key1.Subary.SetValStrLit(tptoken, nil, 0, "Winterfell")
	if err != nil {
		panic(err)
	}
	err = buff1.SetValStrLit(tptoken, nil, "Go Westeros")
	if err != nil {
		panic(err)
	}
	err = key1.SetValST(tptoken, nil, &buff1)
	if err != nil {
		panic(err)
	}

	// Allocate a BufferT for return values; REMEMBER TO DEFER Free
	defer cur_sub.Free()
	cur_sub.Alloc(64)

	// Start iterating through the list at the start by setting the last subscript
	//  to ""; stop when we get the error code meaning end
	err = key1.Subary.SetValStrLit(tptoken, nil, 0, "")
	for true {
		err = key1.SubNextST(tptoken, nil, &cur_sub)
		if err != nil {
			error_code := yottadb.ErrorCode(err)
			if error_code == yottadb.YDB_ERR_NODEEND {
				break
			} else {
				panic(err)
			}
		}
		val1, err = cur_sub.ValStr(tptoken, nil)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%s ", (*val1))
		// Move to that key by setting the next node in the key
		key1.Subary.SetValStr(tptoken, nil, 0, val1)
	}
	/* Output: Winterfell shire world */
}
