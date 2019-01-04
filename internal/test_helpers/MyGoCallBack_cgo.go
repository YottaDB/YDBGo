package test_helpers

import "unsafe"

/*
#include <libyottadb.h>
#include <inttypes.h>
int MyGoCallBack(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm);
int MyGoCallBack_cgo(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm) {
    return MyGoCallBack(tptoken, errstr, tpfnparm);
}
*/
import "C"

func GetMyGoCallBackCgo() unsafe.Pointer {
	return C.MyGoCallBack_cgo
}
