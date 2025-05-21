//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2020-2025 YottaDB LLC and/or its subsidiaries.
// All rights reserved.
//
//	This source code contains the intellectual property
//	of its copyright holder(s), and is made available
//	under a license.  If you do not know the terms of
//	the license, please stop and do not read further.
//
//////////////////////////////////////////////////////////////////

package yottadb

/* extern void signalExitCallback(int signal);

// C wrapper to call the Go function signalExitCallback.
// It is necessary to use a C wrapper since CGo will not let a pointer to a Go function be passed to ydb_main_lang_init().
// This function must not be in init.go or we we get duplicate entry point compilation issues.
//
void ydb_signal_exit_callback(int signal) {
  signalExitCallback(signal);
}
*/
import "C"
