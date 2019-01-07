package yottadb_test

import (
	"lang.yottadb.com/go/yottadb"
	"lang.yottadb.com/go/yottadb/internal/test_helpers"
)

const dummyVarToForceGoDocToShowComments = 42

// NOTE: lines starting with //# should have the //# removed in real code
//  Due to the way Go runs example/test code, it is impossible to demonstrate
//  exactly how this would work in application

// The callback routine needs some C code generated for it; this can be
//  done using the gengluecode utility YottaDB provides. To install it, run
//      go get lang.yottadb.com/go/gengluecode/
//# //go:generate gengluecode -pkg yottadb_test -func MyGoCallBack

//# //export MyGoCallBack
//# func MyGoCallBack(tptoken uint64, errstr *BufferT, tpfnarg unsafe.Pointer, errptr unsafe.Pointer) int {
//#     var errstr yottadb.BufferT
//#     errstr.BufferTFromPtr(errptr)
//#	// This violates TP transactions, but useful for demonstration
//#	fmt.Printf("Hello from MyGoCallBack!\n")
//#	return 0
//# }

// Example demonstrating how to do transactions in Go
func Example_transactionProcessing() {
	// Allocate a key to set our value equal too
	var buffertary1 yottadb.BufferTArray
	var errstr yottadb.BufferT
	var tptoken uint64
	var err error

	// The tptoken argument to many functions is either a value passed into the
	//  callback routine for TP, or yottadb.NOTTP if not in a transaction
	tptoken = yottadb.NOTTP

	// Restore all YDB local buffers on a TP-restart
	defer buffertary1.Free()
	buffertary1.Alloc(1, 32)
	errstr.Alloc(64)
	defer errstr.Free()
	
	err = buffertary1.SetValStrLit(tptoken, &errstr, 0, "*")
	if err != nil {
		panic(err)
	}
	//# /*
	err = buffertary1.TpST(tptoken, &errstr, test_helpers.GetMyGoCallBackCgo(), nil, "TEST")
	//# */
	//# err = buffertary1.TpST(tptoken, nil, GetMyGoCallBackCgo(), nil, "TEST")
	if err != nil {
		panic(err)
	}

	/* Output: Hello from MyGoCallBack!
	 */
}
