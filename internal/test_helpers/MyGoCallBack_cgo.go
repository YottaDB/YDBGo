package test_helpers

import "unsafe"

/*
#include <inttypes.h>
int MyGoCallBack(uint64_t tptoken, void *tpfnparm);
int MyGoCallBack_cgo(uint64_t tptoken, void *tpfnparm) {
    return MyGoCallBack(tptoken, tpfnparm);
}
*/
import "C"

func GetMyGoCallBackCgo() unsafe.Pointer {
	return C.MyGoCallBack_cgo
}
