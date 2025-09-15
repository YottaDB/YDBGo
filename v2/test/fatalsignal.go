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

package main

import (
	"fmt"
	"syscall"
	"time"

	"lang.yottadb.com/go/yottadb/v2"
)

import "C"

// main checks that a fatal signal exits and shuts down cleanly.
// This forces a database shutdown so it should be run stand-alone, not with other tests.
// Note: requires an external helper program to provide -run FatalSignal -fatalsignal flags and to check that stdout
// says "shutdownSignalGoroutines: Channel closings complete" (cf. shutdownSignalGoroutines)
func main() {
	//defer yottadb.Shutdown(yottadb.MustInit())
	yottadb.MustInit()

	defer func() {
		recovered := recover()
		if recovered != nil {
			fmt.Printf("Recovered from: %s\n", recovered)
		} else {
			fmt.Printf("No panic occurred\n")
		}
	}()
	yottadb.DebugMode.Store(2) // switch on output of completion message in shutdownSignalGoroutines()
	//syscall.Kill(syscall.Getpid(), syscall.SIGINT) // Send ourselves a SIGINT
	yottadb.SignalExitCallback(syscall.SIGINT)
	time.Sleep(time.Second) // allow time for signal to be heard
}
