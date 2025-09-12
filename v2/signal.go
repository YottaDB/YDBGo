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

// Refer YottaDB-required signals to YottaDB and allow user to be notified of them if desired

package yottadb

import (
	"fmt"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// #include "libyottadb.h"
// extern void *ydb_signal_exit_callback(void);
import "C"

// Shutdown globals
var ydbSigPanicCalled atomic.Bool          // True when our exit is panic driven due to a signal
var ydbShutdownCheck = make(chan struct{}) // Flag that a channel has been shut down. Needs no buffering since we use blocking writes
var shutdownSigGoroutines bool             // Flag that we have completed shutdownSignalGoroutines()
var shutdownSigGoroutinesMutex sync.Mutex  // Serialize access to shutdownSignalGoroutines()

// YDBSignals lists all the signals that YottaDB requires to be notified of.
var YDBSignals = []os.Signal{
	syscall.SIGABRT,
	syscall.SIGALRM,
	syscall.SIGBUS,
	syscall.SIGCONT,
	syscall.SIGFPE,
	syscall.SIGHUP,
	syscall.SIGILL,
	syscall.SIGINT,
	// syscall.SIGIO - this is a duplicate of SIGURG
	// syscall.SIGIOT - this is a duplicate of SIGABRT
	syscall.SIGQUIT,
	syscall.SIGSEGV,
	syscall.SIGTERM,
	syscall.SIGTRAP,
	syscall.SIGTSTP,
	syscall.SIGTTIN,
	syscall.SIGTTOU,
	syscall.SIGURG,
	syscall.SIGUSR1,
}

// sigInfo holds info for each signal that YDBGo handles and defers to YottaDB.
type sigInfo struct {
	updating     sync.Mutex     // Indicate this struct is being modified
	signal       os.Signal      // signal number for this entry
	notifyChan   chan os.Signal // user-supplied channel notify of incoming signal
	shutdownNow  chan struct{}  // Channel used to shutdown signal handling goroutine
	shutdownDone atomic.Bool    // indicate that goroutine shutdown is complete
	servicing    atomic.Bool    // indicate signal handler is active
	_conn        *Conn          // not a full Conn; just acts as a place to store errstr for use by this signal's NotifyYDB()
}

// init populates ydbSignalMap -- must occur before starting signal handler goroutines in Init()
var ydbSignalMap sync.Map // stores data for signals that the wrapper handles and passes to YottaDB.
func init() {
	for _, sig := range YDBSignals {
		info := sigInfo{sync.Mutex{}, sig, nil, make(chan struct{}, 1), atomic.Bool{}, atomic.Bool{}, _newConn()}
		ydbSignalMap.Store(sig, &info)
	}
}

// printEntry is a function to print the entry point of the function, when entered, if the printEPHdrs flag is enabled.
func printEntry(funcName string) {
	if debugMode.Load() >= 1 {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			log.Println("Entered ", funcName, " from ", file, " at line ", line)
		} else {
			log.Println("Entered ", funcName)
		}
	}
}

// syslogEntry records the given message in the syslog. Since these are rare or one-time per process type errors
// that get recorded here, we open a new syslog handle each time to reduce complexity of access across goroutines.
func syslogEntry(logMsg string) {
	syslogr, err := syslog.New(syslog.LOG_INFO+syslog.LOG_USER, "[YottaDB-Go-Wrapper]")
	if err != nil {
		panic(errorf(ydberr.Syslog, "syslog.New() failed unexpectedly with error: %s", err))
	}
	err = syslogr.Info(logMsg)
	if err != nil {
		panic(errorf(ydberr.Syslog, "syslogr.Info() failed unexpectedly with error: %s", err))
	}
	err = syslogr.Close()
	if err != nil {
		panic(errorf(ydberr.Syslog, "syslogr.Close() failed unexpectedly with error: %s", err))
	}
}

// lookupYDBSignal returns a pointer to the sigInfo entry related to signal sig.
func lookupYDBSignal(sig os.Signal) *sigInfo {
	value, ok := ydbSignalMap.Load(sig)
	if !ok {
		panic(errorf(ydberr.SignalUnsupported, "The specified signal %d (%v) is not a YottaDB signal so is unsupported for signal notification", sig, sig))
	}
	info := value.(*sigInfo)
	return info
}

// validateYDBSignal verifies that the specified signal is valid for SignalNotify()/SignalReset()
func validateYDBSignal(sig os.Signal) *sigInfo {
	// Verify the supplied signal is one that we support with this function. This list contains all of the signals
	// that the wrapper traps in Init() except those signals that cause problems if handlers other than
	// YottaDB's handler is driven (SIGTSTP, SIGTTIN, etc).
	// It is up to the user to know which signals are duplicates of others so if separate handlers
	// are set for say SIGABRT and SIGIOT, whichever handler was set last is the one that gets both signals
	// (because both constants are the the same signal).

	info := lookupYDBSignal(sig)
	if sig == syscall.SIGTSTP || sig == syscall.SIGTTIN || sig == syscall.SIGTTOU {
		panic(errorf(ydberr.SignalUnsupported, "handling signal %d (%v) hangs, so handling it is not supported", sig, sig))
	}
	return info
}

// SignalNotify relays incoming signals to notifyChan specifically for signals used by YottaDB.
// If SignalNotify is used on a specific signal, the user is then responsible to call [NotifyYDB]() at the start or end
// of their own handler to allow YottaDB to process the signal. The user can revert behaviour to the YDBGo default
// with [SignalReset](), after which YDBGo will once again call [NotifyYDB]() itself.
//   - Users may opt to use the standard library's [Signal.Notify]() instead of this function to be notified of signals, but
//     this will notify them in parallel with YottaDB. However, they must not call Signal.Stop() (see below).
//   - Do not call [Signal.Stop](), [Signal.Ignore]() or [Signal.Reset]() for any of the YottaDB-specific signals
//     unless you understand that it will prevent [NotifyYDB]() from being called, and will affect YottaDB timers
//     or other functionality.
//   - Using SignalNotify to capture SIGSEGV is unreliable. Instead, see standard library function [debug.SetPanicOnFault](true)
//
// YottaDB-specific signals are listed in the source in [YDBSignals].
//
// See [YottaDB signals].
//
// [YottaDB signals]: https://docs.yottadb.com/MultiLangProgGuide/programmingnotes.html#signals
func SignalNotify(notifyChan chan os.Signal, signals ...os.Signal) {
	// Do-nothing hack purely to prevent goimport from removing runtime/debug from imports since it's required for the docstring above
	debug.SetPanicOnFault(debug.SetPanicOnFault(false))

	// Although this routine itself does not interact with the YottaDB runtime, use of this routine has an expectation that
	// the runtime is going to handle signals so let's make sure it is initialized.
	initCheck()
	for _, sig := range signals {
		info := validateYDBSignal(sig)
		info.updating.Lock()
		info.notifyChan = notifyChan
		info.updating.Unlock()
	}
}

// SignalReset stops notifying the user of the given signals and reverts to default YDBGo signal behaviour which simply calls [NotifyYDB].
// No error is raised if the signal did not already have a notification request in effect.
func SignalReset(signals ...os.Signal) {
	for _, sig := range signals {
		info := lookupYDBSignal(sig)
		info.updating.Lock()
		info.notifyChan = nil
		info.updating.Unlock()
	}
}

// NotifyYDB calls the YottaDB signal handler for sig.
// Return as a boolean whether YottaDB returned error code ydberr.CALLINAFTERXIT.
// If YottaDB deferred handling of the signal, return false; otherwise return true.
// Panic on YottaDB errors.
func NotifyYDB(sig os.Signal) bool {
	value, ok := ydbSignalMap.Load(sig)
	if !ok {
		panic(errorf(ydberr.SignalUnsupported, "goroutine-sighandler: called NotifyYDB with a non-YottaDB signal %d (%v)", sig, sig))
	}
	info := value.(*sigInfo)
	_conn := info._conn

	// Flag that YDB is servicing this signal
	info.servicing.Store(true)
	defer info.servicing.Store(false)

	if debugMode.Load() >= 2 && sig != syscall.SIGURG {
		// SIGURG happens almost continually, so don't report it.
		log.Printf("goroutine-sighandler: calling YottaDB signal handler for signal %d (%v)\n", sig, sig)
	}
	signum := C.int(sig.(syscall.Signal)) // have to type-assert before converting to an int
	// Note this call to ydb_sig_dispatch() does not pass a tptoken. The reason for this is that inside
	// this routine the tptoken at the time the signal actually occurs is unknown. The ydb_sig_dispatch()
	// routine itself does not need the tptoken nor does anything it calls but we do still need an
	// error buffer in case an error occurs that we need to return.
	rc := C.ydb_sig_dispatch(&_conn.cconn.errstr, signum)
	// Handle errors so user doesn't have to
	switch rc {
	case YDB_OK:
		// Signal handling complete
	case YDB_DEFER_HANDLER:
		// Signal was deferred for some reason
		// Not an error, but the fact is logged
		if debugMode.Load() >= 2 {
			log.Printf("goroutine-sighandler: YottaDB deferred signal %d (%v)\n", sig, sig)
		}
		return false
	case ydberr.CALLINAFTERXIT:
		// If CALLINAFTERXIT, we're done - exit goroutine
		shutdownSignalGoroutine(info)
	default: // Some sort of error occurred during signal handling
		err := _conn.lastError(rc)
		panic(newError(ydberr.SignalHandling, fmt.Sprintf("goroutine_sighandler: error from ydb_sig_dispatch() of signal %d (%v)", sig, sig), err))
	}
	return true
}

// FatalSignalPanic returns whether the currently unwinding panic was caused by a fatal signal like Ctrl-C.
// May be used in a deferred function like [QuitAfterFatalSignal] to check whether a fatal signal caused the current exit procedure.
func SignalWasFatal() bool {
	return ydbSigPanicCalled.Load()
}

// QuitAfterFatalSignal may be deferred by goroutines to prevent goroutine errors after Ctrl-C is pressed.
// When Ctrl-C is pressed the signal is (by default) passed to YottaDB which shuts down the database.
// If goroutines are still running and access the database, they will panic with code ydberr.CALLINAFTERXIT.
// To silence these many panics and have each goroutine simply exit gracefully, defer QuitAfterFatalSignal()
// at the start of each goroutine. Then you will get just one panic from the Ctrl-C signal interrupt rather
// than one CALLINAFTERXIT panic per goroutine.
//
// Deferring this function will hide CALLINAFTERXIT panics when caused by YottaDB receiving a fatal signal.
// If you don't wish to completely hiding these errors, you could make your own version of this function that logs them.
//
// See: [Shutdown], [SignalNotify], [SignalWasFatal]
func QuitAfterFatalSignal() {
	if err := recover(); err != nil {
		if err, ok := err.(error); !ok {
			panic(err)
		}
		if ErrorIs(err.(error), ydberr.CALLINAFTERXIT) {
			runtime.Goexit() // Silently and gracefully exit the goroutine
		} else {
			panic(err)
		}
	}
}

// handleSignal is used as a goroutine for each YottaDB-specific signal (listed in YDBSignals).
// It calls NotifyYDB() unless a user has requested notification of that signal using SignalNotify(),
// in which case it will notify the user who must call NotifyYDB().
// info specifies the signal to be handled by this particular goroutine.
func handleSignal(info *sigInfo) {
	sig := info.signal

	// We only need one of each type of signal so buffer depth is 1, but let it queue one additional signal.
	sigchan := make(chan os.Signal, 2)
	// Create fresh channel for shutdown monitoring.
	info.shutdownNow = make(chan struct{}, 1) // Need to buffer only 1 element since shutdownSignalGoroutine() is non-blocking
	// Tell Go to pass this signal to our channel.
	signal.Notify(sigchan, sig)
	if debugMode.Load() >= 2 {
		log.Printf("goroutine-sighandler: Signal handler initialized for %d (%v)\n", sig, sig)
	}
	wgSigInit.Done() // Signal parent goroutine that we have completed initializing signal handling
	allDone := false
	// Process incoming signals until we get told to stop on channel info.shutdownNow
	for !allDone {
		select {
		case <-sigchan:
			// Wait for signal notification
		case <-info.shutdownNow:
			allDone = true // Done if channel has data
		}
		if allDone {
			break // Got a shutdown request - fall out!
		}

		// See if user asked to be notified of this signal
		info := lookupYDBSignal(sig)
		// Note that for fatal signals, the YDB handler probably won't return as it will (usually) exit,
		// but some fatal signals can be deferred under the right conditions (holding crit, interrupts-disabled-state, etc).
		info.updating.Lock()
		notifyChan := info.notifyChan
		info.updating.Unlock()
		if notifyChan != nil {
			// Notify user code via the supplied channel
			if debugMode.Load() >= 2 {
				log.Printf("goroutine-sighandler: notifying user-specified channel of signal %d (%v)\n", sig, sig)
			}
			// Send to channel without blocking (same as Signal.Notify)
			select {
			case notifyChan <- sig: // notify channel of signal, sending it a function to use to notify YDB
			default:
			}
		} else {
			// otherwise just run YDB handler function ourselves since user didn't hook this signal
			NotifyYDB(sig)
		}
	}
	signal.Stop(sigchan) // No more signal notification for this signal channel
	if debugMode.Load() >= 2 {
		log.Printf("goroutine-sighandler: exiting goroutine for signal %d (%v)\n", sig, sig)
	}
	info.shutdownDone.Store(true)  // Indicate this channel is closed
	ydbShutdownCheck <- struct{}{} // Notify shutdownSignalGoroutines that it needs to check if all channels closed now
}

// shutdownSignalGoroutine tells the routine for the signal specified by info to shutdown.
// This is Non-blocking.
func shutdownSignalGoroutine(info *sigInfo) {
	// Perform non-blocking send
	select {
	// Wake up signal goroutine and make it exit
	case info.shutdownNow <- struct{}{}:
	default:
	}
}

// shutdownSignalGoroutines is a function to stop the signal handling goroutines used to tell the YDB engine what signals
// have occurred. No signals are recognized by the Go wrapper or YottaDB once this is done. All signal handling reverts to
// Go standard handling.
func shutdownSignalGoroutines() {
	printEntry("shutdownSignalGoroutines")
	shutdownSigGoroutinesMutex.Lock()
	if shutdownSigGoroutines { // Nothing to do if already done
		shutdownSigGoroutinesMutex.Unlock()
		if debugMode.Load() >= 2 {
			log.Println("shutdownSignalGoroutines: Bypass shutdownSignalGoroutines as it has already run")
		}
		return
	}
	// Send shutdown signal to each goroutine
	for _, sig := range YDBSignals {
		value, _ := ydbSignalMap.Load(sig)
		shutdownSignalGoroutine(value.(*sigInfo))
	}
	// Wait for the signal goroutines to exit but with a timeout
	doneChan := make(chan struct{}) // Zero-length is OK because we signal it by closing it.
	go func() {
		// Loop handling channel notifications as goroutines shutdown. If we are currently handling a fatal signal
		// like a SIGQUIT, that channel is active but is busy so will not respond to a shutdown request. For this
		// reason, we treat active goroutines the same as successfully shutdown goroutines so we don't delay
		// shutdown. No need to wait for something that is not likely to occur (The YottaDB handlers for fatal signals
		// drive a process-ending panic and never return).
		for {
			<-ydbShutdownCheck // A goroutine finished - check if all are shutdown or otherwise busy
			done := true
			for _, sig := range YDBSignals {
				value, _ := ydbSignalMap.Load(sig)
				sigData := value.(*sigInfo)
				shutdownDone := sigData.shutdownDone.Load()
				signalActive := sigData.servicing.Load()
				if !shutdownDone && !signalActive {
					// A goroutine is not shutdown and not active so to wait for more
					// goroutine(s) to complete, break out of this scan loop
					done = false
					break
				}
			}
			if done {
				close(doneChan) // Notify select loop below that this is complete
				return
			}
		}
	}()
	select {
	case <-doneChan: // All signal monitoring goroutines are shutdown or are busy!
		if debugMode.Load() >= 2 {
			log.Println("shutdownSignalGoroutines: All signal goroutines successfully closed or active")
		}
	case <-time.After(MaxSigShutdownWait):
		// Notify syslog that this timeout happened
		if debugMode.Load() >= 2 {
			log.Println("shutdownSignalGoroutines: Timeout! Some signal goroutines did not shutdown")
		}
		syslogEntry("Shutdown of signal goroutines timed out")
	}
	shutdownSigGoroutines = true
	shutdownSigGoroutinesMutex.Unlock()
	// All signal routines should be finished or otherwise occupied
	if debugMode.Load() >= 2 {
		log.Println("shutdownSignalGoroutines: Channel closings complete")
	}
}

// signalExitCallback is called from C by YottaDB to perform an exit when YottaDB gets a fatal signal.
// Its purpose is to make sure defers get called before exit, which it does by calling panic.
// This function is passed to YottaDB during init by ydb_main_lang_init().
// YDB calls this exit handler after rundown (equivalent to ydb_exit()).
// The sigNum parameter is reported in the panic message.
//
//export signalExitCallback
func signalExitCallback(sigNum C.int) {
	printEntry("YDBWrapperPanic()")
	ydbSigPanicCalled.Store(true) // Need "atomic" usage to avoid read/write DATA RACE issues
	shutdownSignalGoroutines()    // Close the goroutines down with their signal notification channels
	sig := syscall.Signal(sigNum) // Convert numeric signal number to Signal type for use in panic() message
	panic(errorf(ydberr.SignalFatal, "Fatal signal %d (%v) occurred", sig, sig))
}
