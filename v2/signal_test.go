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

package yottadb

import (
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

// TestSigSegv tests ability to catch a SIGSEGV and do cleanup rather than exit immediately leaving a core file.
// This test is derived from a test case in the YDBGo repository: YottaDB/Lang/YDBGo#25 (comment 258495195) by @zapkub
func TestSigSegv(t *testing.T) {
	conn := NewConn()
	conn.Node("avariable").Set("a value") // dummy Set

	// Access invalid address -1 to test this because accessing nil always produces a panic regardless of SetPanicOnFault
	var p uintptr
	// The following *Sizeof(p) is needed to avoid alignment errors when trying to access badPointer when `test -race` is used
	// The 2* is to avoid "straddles multiple allocations" error when accessing the last aligned memory address during `test -race`
	var badPointer *string = (*string)(unsafe.Add(unsafe.Pointer(nil), -2*int(unsafe.Sizeof(p))))

	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true)) // No core - just a panic please
	assert.Panics(t, func() { fmt.Println(*badPointer) })
}

var testSignals []os.Signal

func init() {
	for _, sig := range YDBSignals {
		switch sig {
		case syscall.SIGTSTP, syscall.SIGTTIN, syscall.SIGTTOU:
			// Skip signals which cause hangs and are therefore not supported by YDBGo
			continue
		}
		testSignals = append(testSignals, sig)
	}
}

// TestSignalNotify tests that all YDB signals are correctly received under load per issue YDBGo#34.
func TestSignalReset(t *testing.T) {
	// Create channels we use for notification/acknowledgement for our two handlers (expected and unexpected)
	sig := os.Signal(syscall.SIGCONT)
	ch := make(chan os.Signal, 2)
	SignalNotify(ch, sig)
	defer SignalReset(sig)

	// Check that we've hooked the signal by sending one
	syscall.Kill(syscall.Getpid(), sig.(syscall.Signal))
	received := false
	for !received {
		select {
		case <-time.After(MaxSigShutdownWait):
			panic("signal not received before timeout")
		case got := <-ch:
			received = got == sig
		}
	}

	// Check that SignalReset prevents more notifications from coming in
	SignalReset(sig)
	syscall.Kill(syscall.Getpid(), sig.(syscall.Signal))
	timeout := false
	for !timeout {
		select {
		case <-time.After(10 * time.Millisecond):
			timeout = true
		case got := <-ch:
			if got == sig {
				panic("signal received after SignalReset() turned it off")
			}
		}
	}
}

// TestSignalNotify tests that all YDB signals are correctly received under load per issue YDBGo#34.
func TestSignalNotify(t *testing.T) {
	// Check that an non-YDB signal panics
	assert.Panics(t, func() { SignalNotify(make(chan os.Signal), syscall.SIGPIPE) })

	// Wait group so we know when all goroutines are started or finished and when the signal is received/processed
	var starting, running sync.WaitGroup
	// indicate it is time to shut down workerBee goroutines
	var allDone atomic.Bool

	// workerBee function increments the value of global given ^a(i). Used as one of a number of "worker bee" goroutines.
	workerBee := func(i int) {
		defer running.Done()
		starting.Done() // Tell main we are running
		// Start up a processing loop
		conn := NewConn()
		n := conn.Node("^a", i)
		for !allDone.Load() {
			// Do update inside a transaction
			conn.TransactionFast([]string{}, func() {
				n.Incr(1)
			})
		}
	}

	// Start up a few goroutines that will be doing some work when the signal comes in
	const goroutines int = 4
	for i := range goroutines {
		starting.Add(1) // Add worker to list we wait for to startup
		running.Add(1)  // Used to test when finished
		go workerBee(i)
	}
	if testing.Verbose() {
		fmt.Println(" Starting workers")
	}
	starting.Wait() // Wait for all workers to be up and running

	// Test non-fatal signals, passing on the signal to YDB
	testSignal(syscall.SIGCONT, true)
	testSignal(syscall.SIGHUP, true)
	testSignal(syscall.SIGUSR1, true)
	// Now test the rest (fatal signals), not passing on the signal to YDB (because that exits the test)
	for _, sig := range testSignals {
		// Skip signals which often occur naturally during the test because they would produce a incorrect-trigger failure
		if sig == syscall.SIGALRM || sig == syscall.SIGURG {
			continue
		}
		testSignal(sig, false)
	}

	// Stop workers
	if testing.Verbose() {
		fmt.Println(" Stopping workers")
	}
	allDone.Store(true) // Indicate it is time to shut everything down
	running.Wait()      // Wait for any worker not yet complete
}

// testSignal hooks all signals, then sends the given signal sig, and checks to see that it receives *only* that signal.
//   - tellYDB determines whether to call NotifyYDB for the given signal. Switch it off for fatal signals or the text will exit
func testSignal(sig os.Signal, tellYDB bool) {
	// Create channels we use for notification/acknowledgement for our two handlers (expected and unexpected)
	ch := make(chan os.Signal, 2)
	// Set all YDB signals to go to notify ch
	SignalNotify(ch, testSignals...)
	defer SignalReset(testSignals...)

	// Now actually send the signal
	if testing.Verbose() {
		fmt.Printf(" Testing signal %d (%s): ", sig, sig)
	}
	syscall.Kill(syscall.Getpid(), sig.(syscall.Signal))

	select {
	case <-time.After(MaxSigShutdownWait):
		panic("signal not received before timeout")
	case got := <-ch:
		if got != sig {
			panic(fmt.Errorf("received signal %d (%s) when expecting %d (%s)", got, got, sig, sig))
		}
		if testing.Verbose() {
			fmt.Print("received")
		}
		if tellYDB {
			if testing.Verbose() {
				fmt.Printf("; calling YottaDB handler\n")
			}
			NotifyYDB(got)
		} else {
			if testing.Verbose() {
				fmt.Println()
			}
		}
	}
}

// TestSyslogEntry checks that we can write an INFO-level message to syslog.
// Verification that the message is actually in syslog must be done by an external program.
func TestSyslogEntry(t *testing.T) {
	syslogEntry("Test of syslog functionality")
}
