package yottadb_test

import (
	"fmt"
	"lang.yottadb.com/go/yottadb"
)

// Example_basic demonstrates the most basic features of YottaDB:
// setting a value, getting a value, iterating through values,
// and deleting a value.
//
// It does this using methods from the easy API; if performance is a concern, considering using
// methods from the simple API (those methods on KeyT, BufferT, and BufferArrayT).
func Example_easyAPI() {
	// Set global node ["^hello", "world"] to "Go World"
	err := yottadb.SetValE(yottadb.NOTTP, "Go World", "^hello", []string{"world"})
	if err != nil {
		panic(err)
	}

	// Retrieve the value that was set
	r, err := yottadb.ValE(yottadb.NOTTP, "^hello", []string{"world"})
	if err != nil {
		panic(err)
	}
	if r != "Go World" {
		panic("Value not what was expected; did someone else set something?")
	}

	// Set a few more nodes so we can iterate through them
	err = yottadb.SetValE(yottadb.NOTTP, "Go Middle Earth", "^hello", []string{"shire"})
	if err != nil {
		panic(err)
	}
	err = yottadb.SetValE(yottadb.NOTTP, "Go Westeros", "^hello", []string{"Winterfell"})
	if err != nil {
		panic(err)
	}

	var cur_sub = ""
	for true {
		cur_sub, err = yottadb.SubNextE(yottadb.NOTTP, "^hello", []string{cur_sub})
		if err != nil {
			error_code := yottadb.ErrorCode(err)
			if error_code == yottadb.YDB_ERR_NODEEND {
				break
			} else {
				panic(err)
			}
		}
		fmt.Printf("%s ", cur_sub)
	}
	/* Output: Winterfell shire world*/
}
