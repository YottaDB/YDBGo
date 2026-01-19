//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2025-2026 YottaDB LLC and/or its subsidiaries.
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
	conn := SetupTest(t)
	conn.Node("avariable").Set("a value") // dummy Set

	// Access invalid address -1 to test this because accessing nil always produces a panic regardless of SetPanicOnFault
	var p uintptr
	// The following *Sizeof(p) is needed to avoid alignment errors when trying to access badPointer when `test -race` is used
	// The 2* is to avoid "straddles multiple allocations" error when accessing the last aligned memory address during `test -race`
	var badPointer *string = (*string)(unsafe.Add(unsafe.Pointer(nil), -2*int(unsafe.Sizeof(p))))

	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true)) // No core - just a panic please
	assert.Panics(t, func() { fmt.Println(*badPointer) })
}

var testSignals []os.Signal = YDBSignals

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
	// Check that a non-YDB signal panics
	assert.Panics(t, func() { SignalNotify(make(chan os.Signal), syscall.SIGPIPE) })
	assert.Panics(t, func() { NotifyYDB(syscall.SIGPIPE) })

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
			conn.TransactionFast(nil, func() {
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
	// Now test all signals but without passing them on to YDB because they are designed to cause either exit or hang, which would stop our tests.
	for _, sig := range testSignals {
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
	timeout := time.After(MaxSigShutdownWait)
	for {
		select {
		case <-timeout:
			panic("signal not received before timeout")
		case got := <-ch:
			if got != sig {
				// Skip signals which often occur naturally during the test because they would produce a incorrect-trigger failure
				if got == syscall.SIGALRM || got == syscall.SIGURG {
					continue
				}
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
			return
		}
	}
}

// TestSyslogEntry checks that we can write an INFO-level message to syslog.
// Verification that the message is actually in syslog must be done by an external program.
// Note: requires an external helper program to run the test with flags: -run TestSyslog -syslog
func TestSyslogEntry(t *testing.T) {
	if !testSyslog {
		return
	}
	syslogEntry("Test of syslog functionality")
}

// TestFatal checks that a fatal signal exits and shuts down cleanly.
// This forces a database shutdown so it should be run stand-alone, not with other tests.
// Note: requires an external helper program to provide flags: -run TestFatal -fataltest=fake, etc
// and to check that stdout says "shutdownSignalGoroutines: Channel closings complete"
// (cf. shutdownSignalGoroutines)
// Set -fataltest to:
//   - "real" to send the signal with syscall.Kill
//   - "fake" to call the SignalExitCallback directly
//   - "shutdownpanic" to test that path
//
// Only the "fake" form saves coverage data in the coverage file.
// The "real" form doesn't because YottaDB calls os.Exit() before Go saves the coverage data.
// This means you have to call both forms: one to test coverage, and one to test that the signal actually works
func TestFatalTest(t *testing.T) {
	if fatalTest == "none" {
		return
	}

	DebugMode.Store(2) // turn on output of completion message in shutdownSignalGoroutines()

	recoverer := func() {
		err := recover()
		if err != nil {
			fmt.Printf("Recovered from: %s\n", err)
			// This is purely to cover code path where NotifyYDB receives a CALLINAFTERXIT error from YDB.
			// It is expected to do nothing except shut down the signal goroutines (again).
			NotifyYDB(syscall.SIGCONT)
		} else {
			fmt.Printf("No panic occurred2\n")
		}
	}
	defer recoverer()

	switch fatalTest {
	case "real":
		syscall.Kill(syscall.Getpid(), syscall.SIGINT) // Send ourselves a SIGINT
		time.Sleep(time.Second)                        // Give signal time be picked up by goroutine
	case "fake":
		// Only the "fake" form saves coverage data in the coverage file because in the "real" form YDB exits before Go saves the coverage data.
		ydbSigPanicCalled.Store(true) // fake this, too
		SignalExitCallback(syscall.SIGINT)
	case "goroutine":
		ch := make(chan os.Signal, 1) // Create signal notify and signal ack channels
		SignalNotify(ch, syscall.SIGQUIT)
		go func() {
			defer recoverer()
			defer ShutdownOnPanic()
			for {
				sig := <-ch
				fmt.Printf("\nSignal %d (%s) received.\n", sig, sig)
				NotifyYDB(sig)
			}
		}()
		syscall.Kill(syscall.Getpid(), syscall.SIGQUIT) // Send ourselves a SIGINT
		time.Sleep(time.Second)                         // Give signal time be picked up by goroutine
	case "shutdownpanic":
		ydbSigPanicCalled.Store(true)    // fake this to test its code path
		assert.True(t, SignalWasFatal()) // take the opportunity of a fatal signal condition to coverage-test this function
		MaxPanicExitWait = 10 * time.Millisecond
		fallthrough
	case "shutdownpanic2":
		MaxNormalExitWait = 10 * time.Millisecond
		go func() {
			defer recoverer()
			defer ShutdownOnPanic()
			wgexit.Add(1) // kludgy way to force code coverage of shutdown MaxNormalExitWait timeout path
			panic("test panic")
		}()
		time.Sleep(100 * time.Millisecond) // Give goroutine time to finish up
	}
	fmt.Printf("No panic occurred1\n")
}
