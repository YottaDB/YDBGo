//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2020 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// #include "libyottadb.h"
// extern void *ydb_get_gowrapper_panic_callback_funcvp(void);
import "C"

// signalsHandled is the count of the signals the wrapper gets notified of and passes on to YottaDB. This matches with
// the number of signals (and thus also the number of goroutines launched running waitForAndProcessSignal() as well as
// being the dimension of the ydbShutdownChannel array below that has one slot for each signal handled.
const signalsHandled int = 18

var wgSigInit sync.WaitGroup                    // Used to make sure signals are setup before initializeYottaDB() exits
var wgSigShutdown sync.WaitGroup                // Used to wait for all signal threads to shutdown
var ydbInitMutex sync.Mutex                     // Mutex for access to initialization
var ydbShutdownMutex sync.Mutex                 // Mutex doing wrapper's part of engine shutdown (close signal goroutines)
var ydbShutdownChannel [signalsHandled]chan int // Array of channels used to shutdown signal handling threads
var ydbShutdown bool                            // True when the YDB engine has been shutdown

// shutdownSignalGoroutines is a function to stop the signal handling goroutines used to tell the YDB engine what signals
// have occurred. No signals are recognized by the Go wrapper or YottaDB once this is done. All signal handling reverts to
// Go standard handling.
func shutdownSignalGoroutines() {
	printEntry("shutdownSignalGoroutines")
	ydbShutdownMutex.Lock()
	if ydbShutdown {
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: shutdownSignalGoroutines: already run - bypassing")
		}
		ydbShutdownMutex.Unlock()
		return // Nothing left to do
	}
	for i := 0; signalsHandled > i; i++ {
		close(ydbShutdownChannel[i]) // Will wakeup signal routine and make it exit
	}
	ydbShutdown = true
	// Wait for the signal goroutines to exit but with a timeout
	done := make(chan struct{})
	go func() {
		wgSigShutdown.Wait()
		close(done)
	}()
	select {
	case <-done: // All signal monitoring goroutines are shutdown!
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: shutdownSignalGoroutines: All signal goroutines successfully closed")
		}
	case <-time.After(time.Duration(MaximumCloseWait) * time.Second):
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: shutdownSignalGoroutines: Timeout! Some signal threads did not shutdown")
		}
	}
	// All signal routines should be finished or otherwise occupied
	ydbShutdownMutex.Unlock()
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDB: shutdownSignalGoroutines: Channel closings complete")
	}
}

// YDBWrapperPanic is a function called from C code. The C code routine address is passed to YottaDB via the ydb_main_lang_init()
// call in the below initializeYottaDB() call and is called by YottaDB when it has completed processing a deferred fatal signal
// and needs to exit in a "Go-ish" manner. The parameter determines the type of panic that gets raised.
//export YDBWrapperPanic
func YDBWrapperPanic(sigNum C.int) {
	var sig syscall.Signal

	printEntry("YDBWrapperPanic()")
	atomic.StoreUint32(&ydbSigPanicCalled, 1) // Need "atomic" usage to avoid read/write DATA RACE issues
	shutdownSignalGoroutines()                // Close the goroutines down with their signal notification channels
	sig = syscall.Signal(sigNum)              // Convert numeric signal number to Signal type for use in panic() messagee
	panic(fmt.Sprintf("YDB: Fatal signal %d (%v) occurred", sig, sig))
}

// initializeYottaDB is a function to initialize the YottaDB engine. This is an atypical method of doing simple API
// initialization as usually, we just make the needed calls and initialization is automatically run. But the Go
// wrapper needs to do its initialization differently due to the need to setup signal handling differently. Usually,
// YottaDB sets up its signal handling but to work well with Go, Go itself needs to do the signal handling and forward
// it as needed to the YottaDB engine.
func initializeYottaDB() {
	printEntry("initializeYottaDB()")
	ydbInitMutex.Lock()
	// Verify we need to be initialized
	if 1 == ydbInitialized {
		ydbInitMutex.Unlock() // Already initialized - nothing to see here
		return
	}
	// Drive initialization of the YottaDB engine/runtime
	rc := C.ydb_main_lang_init(C.YDB_MAIN_LANG_GO, C.ydb_get_gowrapper_panic_callback_funcvp())
	if YDB_OK != rc {
		panic(fmt.Sprintf("YDB: YottaDB initialization failed with return code %d", rc))
	}
	// Start up a goroutine for each signal we want to be notified of. This is so that if one signal is in process,
	// we can still catch a different signal and deliver it appropriately (probably to the same thread). For each signal,
	// bump our wait group counter so we don't proceed until all of these goroutines are initialized. If any signals are
	// added or removed here, adjust signalsHandled constant in yottadb.go.
	wgSigInit.Add(1)     // Indicate signals not yet initialized
	wgSigShutdown.Add(1) // Indicate this new goroutine is not yet rundown
	go waitForAndProcessSignal(syscall.SIGABRT, 0)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGALRM, 1)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGBUS, 2)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGCONT, 3)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGFPE, 4)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGILL, 5)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGINT, 6)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGIO, 7)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGIOT, 8)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGQUIT, 9)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGSEGV, 10)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGTERM, 11)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGTRAP, 12)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGTSTP, 13)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGTTIN, 14)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGTTOU, 15)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGURG, 16)
	wgSigInit.Add(1)
	wgSigShutdown.Add(1)
	go waitForAndProcessSignal(syscall.SIGUSR1, 17)
	// Now wait for the goroutine to initialize and get signals all set up. When that is done, we can return
	wgSigInit.Wait()
	atomic.StoreUint32(&ydbInitialized, 1) // YottaDB wrapper is now initialized
	ydbInitMutex.Unlock()
}

// waitForAndProcessSignal is used as a goroutine to wait for a signal notification and send it to YottaDB's signal dispatcher.
func waitForAndProcessSignal(sig syscall.Signal, shutdownChannelIndx int) {
	// This Go routine needs its own errstr buffer as it continues running until ydb_exit() is called
	var errstr BufferT
	var cbuft *C.ydb_buffer_t
	var allDone bool
	var shutdownChannel *chan int
	var sigStatus string

	shutdownChannel = &ydbShutdownChannel[shutdownChannelIndx]
	errstr.Alloc(YDB_MAX_ERRORMSG) // Initialize error string to hold errors
	// We only need one of each type of signal so buffer depth is 1, but let it queue one additional
	sigchan := make(chan os.Signal, 2)
	// Only one message need be passed on shutdown channel so no buffering
	*shutdownChannel = make(chan int)
	// Tell Go to pass this signal to our channel
	signal.Notify(sigchan, sig)
	wgSigInit.Done() // Signal parent goroutine that we have completed initializing signal handling
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Signals now initialized")
	}
	// Although we typically refer to individual signals with their syscall.Signal type, we do need to have them
	// in their numeric form too to pass to C when we want to dispatch their YottaDB handler. Unfortnuately,
	// switching between designations is a bit ridiculous.
	csignum := fmt.Sprintf("%d", sig)
	signum, err := strconv.Atoi(csignum)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error translating signal to numeric: %v", csignum))
	}
	cbuft = errstr.getCPtr() // Get pointer to ydb_buffer_t embedded in errstr's BufferT we need to pass to C
	allDone = false
	for !allDone {
		select {
		case <-sigchan:
			// Wait for signal notification
		case <-*shutdownChannel:
			allDone = true // Done by close of channel
		}
		if allDone {
			break // Got a shutdown request - fall out!
		}
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Go sighandler - sending signal", signum,
				"to dispatcher")
		}
		// Note this call to ydb_sig_dispatch() does not pass a tptoken. The reason for this is that inside this
		// routine the tptoken at the time the signal actually pops in unknown. The ydb_sig_dispatch() routine
		// itself does not need the tptoken nor does anything it calls but we do still need the error buffer in
		// case an error occurs that we need to return.
		rc := C.ydb_sig_dispatch(cbuft, C.int(signum)) // Engine was initialized earlier so no need to re-init
		switch rc {
		case YDB_OK: // Signal handling complete
		case YDB_DEFER_HANDLER: // Signal was deferred for some reason
			// If CALLINAFTERXIT (positive or negative version) we're done - exit goroutine
		case YDB_ERR_CALLINAFTERXIT, -YDB_ERR_CALLINAFTERXIT:
			allDone = true
			break
		default: // Some sort of error occurred during signal handling
			if YDB_OK != rc {
				errstring, err := errstr.ValStr(NOTTP, nil)
				if "" == errstring {
					if nil != err {
						errstring = fmt.Sprintf("%s", err)
					} else {
						errstring = "(unknown error)"
					}
				}
				panic(fmt.Sprintf("YDB: Failure from ydb_sig_dispatch() (rc = %d): %s", rc, errstring))
			}
		}
		if allDone {
			break
		}
		// It is probable that the fatal signals (SIGSEGV/SIGBUS/SIGILL/etc) won't return from the above handling
		// and will end up calling YDBWrapperPanic() defined above but allow them to come through here too and be
		// handled appropriately.
		if dbgSigHandling {
			sigStatus = "complete"
		}
		switch sig {
		case syscall.SIGALRM, syscall.SIGCONT, syscall.SIGIO, syscall.SIGIOT, syscall.SIGURG:
			// No post processing but SIGALRM is most common signal so check for it first
		case syscall.SIGTSTP, syscall.SIGTTIN, syscall.SIGTTOU, syscall.SIGUSR1:
			// No post processing here either
		case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			// These are the deferrable signals - see if need deferring
			if YDB_DEFER_HANDLER == rc {
				if dbgSigHandling {
					sigStatus = "deferred"
				}
			}
		}
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Signal handling for signal", signum, sigStatus)
		}
	}
	signal.Stop(sigchan)      // No more signal notification for this signal channel
	runtime.KeepAlive(errstr) // Keep errstr (and its internal C buffer) around till sure we are done with it
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "goroutine-sighandler: Goroutine for signal index", shutdownChannelIndx, "exiting..")
	}
	wgSigShutdown.Done()
}
