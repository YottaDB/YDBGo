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
	"fmt"

	"lang.yottadb.com/go/yottadb/v2"
)

// ---- Examples

// Example that imports a call-in table which specifies Go functions that wrap M functions.
func ExampleConn_Import() {
	// The following table may be supplied from a string or a file
	table := `
		AddVerbose: string[1024] addVerbose^arithmetic(*string[10], *int, int, string)
		Add: int add^arithmetic(int, int)
		Sub: int sub^arithmetic(int, int)
		AddFloat: float64 add^arithmetic(float64, float64)
		Noop: noop^arithmetic()
	`

	// Import the call table
	conn := yottadb.NewConn()
	m := conn.MustImport(table)

	// Call an imported function directly. Notice that the 'any' return type requires a type assertion
	fmt.Printf("Double (5 + 11) is: %d\n", 2*m.Call("Add", 5, 11).(int))
	fmt.Printf("Half (5 - 11) is: %d\n", m.Call("Sub", 5, 11).(int)/2)
	fmt.Printf("5.5 + 1.2 is: %0.1f\n\n", m.Call("AddFloat", 5.5, 1.2))

	// Wrap these same M routines as Go functions for easier and faster calling
	add, sub, addFloat := m.WrapRetInt("Add"), m.WrapRetInt("Sub"), m.WrapRetFloat("AddFloat")
	fmt.Printf("Double (5 + 11) is: %d\n", 2*add(5, 11))
	fmt.Printf("Half (5 - 11) is: %d\n", sub(5, 11)/2)
	fmt.Printf("5.5 + 1.2 is: %0.1f\n\n", addFloat(5.5, 1.2))

	// Demonstrate calling an M routine that passes variables by reference
	s := "test"
	n := 3
	result := m.Call("AddVerbose", &s, &n, 4, "100").(string)
	fmt.Println("Result =", result)
	fmt.Println("s =", s)
	fmt.Println("n+1 =", n+1)
	fmt.Println()

	// Call a function that returns nothing in its Go signature
	m.Call("Noop")

	// Import a different table that invokes the same M routine sub(), but this time
	// with a different Go function signature just for fun. YDBGo handles the type conversions.
	m2 := conn.MustImport("Sub: string[10] sub^arithmetic(int64, uint64)")
	// Test Sub() again but now with a different Go type signature
	result = m2.Call("Sub", int64(5), uint64(11)).(string)
	fmt.Printf("5 - 11 with negative sign removed is: %s\n", result[1:])

	// Output:
	//
	// Double (5 + 11) is: 32
	// Half (5 - 11) is: -3
	// 5.5 + 1.2 is: 6.7
	//
	// Double (5 + 11) is: 32
	// Half (5 - 11) is: -3
	// 5.5 + 1.2 is: 6.7
	//
	// Result = test:107
	// s = test:
	// n+1 = 108
	//
	// 5 - 11 with negative sign removed is: 6
}
