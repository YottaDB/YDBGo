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

package yottadb_test

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"

	"lang.yottadb.com/go/yottadb/v2"
)

// ---- Examples

// Example that hooks SIGINT and SIGKILL but only passes SIGKILL on to YottaDB:
func ExampleSignalNotify() {
	ch := make(chan os.Signal, 1) // Create signal notify and signal ack channels
	yottadb.SignalNotify(ch, syscall.SIGCONT, syscall.SIGINT)

	// A flag just for this test to check when a signal has been handled
	signals := atomic.Int64{}

	go func(ch chan os.Signal) {
		for {
			sig := <-ch

			fmt.Printf("\nSignal %d (%s) received.\n", sig, sig)
			if sig == syscall.SIGINT {
				fmt.Printf("Not passing signal to YottaDB to avoid exit.\n")
			}
			if sig == syscall.SIGCONT {
				fmt.Printf("Passing signal to YottaDB now.\n")
				yottadb.NotifyYDB(sig)
				fmt.Printf("Returned from YottaDB signal handler.\n")
			}
			signals.Add(1)
		}
	}(ch)

	// send SIGCONT to myself
	syscall.Kill(os.Getpid(), syscall.SIGCONT)
	// ensure the first signal gets processed first so the message order is correct in the output below
	for signals.Load() < 1 {
	}

	// send SIGINT to myself to simulate Ctrl-C
	syscall.Kill(os.Getpid(), syscall.SIGINT)
	// allow time for the previous message to flush before output is checked
	for signals.Load() < 2 {
	}

	// Output:
	//
	// Signal 18 (continued) received.
	// Passing signal to YottaDB now.
	// Returned from YottaDB signal handler.
	//
	// Signal 2 (interrupt) received.
	// Not passing signal to YottaDB to avoid exit.
}
