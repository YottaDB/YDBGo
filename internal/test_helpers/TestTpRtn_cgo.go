package test_helpers

import "unsafe"

/*
// The gateway function
#include <inttypes.h>
int TestTpRtn(unsigned long long tptoken, void *tpfnparm);
int TestTpRtn_cgo(uint64_t tptoken, void *tpfnparm)
{
	return TestTpRtn(tptoken, tpfnparm);
}
*/
import "C"

func TpRtn_cgo() unsafe.Pointer {
	return C.TestTpRtn_cgo
}
