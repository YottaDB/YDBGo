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

package yottadb_test

import (
	"errors"
	"fmt"

	"lang.yottadb.com/go/yottadb/v2"
	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// Example checking whether error is a particular YottaDB error.
func ExampleErrorIs() {
	err := &yottadb.YDBError{Code: ydberr.INVSTRLEN, Message: "string too long"}

	fmt.Println("Error is INVSTRLEN:", yottadb.ErrorIs(err, ydberr.INVSTRLEN))
	fmt.Println(" or using longform:", errors.Is(err, &yottadb.YDBError{Code: ydberr.INVSTRLEN}))

	wrapped := fmt.Errorf("wrapped: %w", err)
	fmt.Println("Wrapped error is still INVSTRLEN:", yottadb.ErrorIs(wrapped, ydberr.INVSTRLEN))

	var e *yottadb.YDBError
	fmt.Println("Error is type YDBError:", errors.As(err, &e))
	if errors.As(err, &e) {
		fmt.Println("  and the error is:", e)
	}

	err2 := fmt.Errorf("string too long")
	fmt.Println("Error is type YDBError:", errors.As(err2, &e))
	if errors.As(err2, &e) {
		fmt.Println("Error is type YDBError:", e)
	}
	// Output:
	// Error is INVSTRLEN: true
	//  or using longform: true
	// Wrapped error is still INVSTRLEN: true
	// Error is type YDBError: true
	//   and the error is: string too long
	// Error is type YDBError: false
}
