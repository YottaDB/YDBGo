package yottadb

// #include <stdlib.h>
// #include <string.h>
// #include "libyottadb.h"
// #include "libydberrors.h"
// int ydbTpStWrapper(uint64_t, void *);
// int ydb_tp_st_wrapper_cgo(uint64_t tptoken, void *tpfnparm) {
//   return ydbTpStWrapper(tptoken, tpfnparm);
// }
import "C"
