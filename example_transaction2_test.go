package yottadb_test

import (
	"fmt"
	"lang.yottadb.com/go/yottadb"
)

// Example demonstrating how to do transactions in Go
func Example_transactionProcessing2() {
	// Allocate a key to set our value equal too
	var buffertary1 yottadb.BufferTArray
	var tptoken uint64
	var err error

	// The tptoken argument to many functions is either a value passed into the
	//  callback routine for TP, or yottadb.NOTTP if not in a transaction
	tptoken = yottadb.NOTTP

	// Restore all YDB local buffers on a TP-restart
	defer buffertary1.Free()
	buffertary1.Alloc(1, 32)
	err = buffertary1.SetValStrLit(tptoken, 0, "*")
	if err != nil {
		panic(err)
	}
	err = buffertary1.TpST2(tptoken, func(tptoken uint64) int {
		fmt.Printf("Hello from MyGoCallBack!\n")
		return 0
	}, "TEST")
	if err != nil {
		panic(err)
	}

	/* Output: Hello from MyGoCallBack!
	 */
}
