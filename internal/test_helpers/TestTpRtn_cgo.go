package test_helpers

import "unsafe"

/*
// The gateway function
#include <inttypes.h>
int TestTpRtn(unsigned long long tptoken, ydb_buffer_t *errstr, void *tpfnparm);
int TestTpRtn_cgo(uint64_t tptoken, ydb_buffer_t *errstr, void *tpfnparm)
{
	return TestTpRtn(tptoken, errstr, tpfnparm);
}
*/
import "C"

func TpRtn_cgo() unsafe.Pointer {
	return C.TestTpRtn_cgo
}
