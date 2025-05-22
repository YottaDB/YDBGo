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

import (
	"fmt"
	"log/syslog"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// #include "libyottadb.h"
// extern void *ydb_signal_exit_callback(void);
import "C"

type ydbHndlrWhen int

const (
	beforeYDBHandler ydbHndlrWhen = iota + 1
	afterYDBHandler
)

// sigInfo holds info for each signal that YDBGo handles and defers to YottaDB.
type sigInfo struct {
	signal       os.Signal     // signal number for this entry
	shutdownNow  chan struct{} // Channel used to shutdown signal handling goroutine
	shutdownDone atomic.Bool   // indicate that goroutine shutdown is complete
	servicing    atomic.Bool   // indicate signal handler is active
}

// ydbSignals lists signals that the wrapper handles and passes to YottaDB.
var ydbSignals = []sigInfo{
	sigInfo{syscall.SIGABRT, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGALRM, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGBUS, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGCONT, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGFPE, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGHUP, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGILL, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGINT, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	// syscall.SIGIO - this is a duplicate of SIGURG
	// syscall.SIGIOT - this is a duplicate of SIGABRT
	sigInfo{syscall.SIGQUIT, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGSEGV, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGTERM, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGTRAP, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGTSTP, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGTTIN, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGTTOU, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGURG, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
	sigInfo{syscall.SIGUSR1, make(chan struct{}), atomic.Bool{}, atomic.Bool{}},
}

// This is a map value element used to look up user signal notification channel and flags (one handler per signal)
type sigNotificationEntry struct {
	notifyChan chan os.Signal // Channel for user to be notified of the signal number being received.
	ackChan    chan struct{}  // Channel used to acknowledge that processing for a signal has completed.
	notifyWhen YDBHandlerFlag // When/if YDB handler is driven in relation to user notification
}

var sigNotificationMap sync.Map

// Exit globals
var wgexit sync.WaitGroup

var wgSigInit sync.WaitGroup               // Used to make sure signals are setup before initializeYottaDB() exits
var inInit sync.Mutex                      // Mutex for access to init and exit
var ydbShutdownCheck = make(chan struct{}) // Channel used to check if all signal routines have been shutdown
var shutdownSigGoroutines bool             // Flag that we have completed shutdownSignalGoroutines()
var shutdownSigGoroutinesMutex sync.Mutex  // Serialize access to shutdownSignalGoroutines()

// printEntry is a function to print the entry point of the function, when entered, if the printEPHdrs flag is enabled.
func printEntry(funcName string) {
	if dbgPrintEPHdrs {
		_, file, line, ok := runtime.Caller(2)
		if ok {
			fmt.Println("Entered ", funcName, " from ", file, " at line ", line)
		} else {
			fmt.Println("Entered ", funcName)
		}
	}
}

// syslogEntry records the given message in the syslog. Since these are rare or one-time per process type errors
// that get recorded here, we open a new syslog handle each time to reduce complexity of access across goroutines.
func syslogEntry(logMsg string) {
	syslogr, err := syslog.New(syslog.LOG_INFO+syslog.LOG_USER, "[YottaDB-Go-Wrapper]")
	if err != nil {
		panic(fmt.Sprintf("syslog.New() failed unexpectedly with error: %s", err))
	}
	err = syslogr.Info(logMsg)
	if err != nil {
		panic(fmt.Sprintf("syslogr.Info() failed unexpectedly with error: %s", err))
	}
	err = syslogr.Close()
	if err != nil {
		panic(fmt.Sprintf("syslogr.Close() failed unexpectedly with error: %s", err))
	}
}

// selectString returns the first string parm if the expression is true and the second if it is false
func selectString(boolVal bool, trueString, falseString string) string {
	if boolVal {
		return trueString
	}
	return falseString
}

// validateNotifySignal verifies that the specified signal is valid for SignalNotify()/SignalReset()
func validateNotifySignal(sig os.Signal) {
	// Verify the supplied signal is one that we support with this function. This list contains all of the signals
	// that the wrapper traps in the initializeYottaDB() except those signals that cause
	// problems if handlers other than YottaDB's handler is driven (SIGTSTP, SIGTTIN, etc).
	// It is up to the user to know which signals are duplicates of others so if separate handlers
	// are set for say SIGABRT and SIGIOT, whichever handler was set last is the one that gets both signals
	// (because both constants are the the same signal).

	unsupported := false
	for i := range ydbSignals {
		entry := &ydbSignals[i]
		if sig == entry.signal {
			unsupported = true
		}
	}
	if sig == syscall.SIGTSTP || sig == syscall.SIGTTIN || sig == syscall.SIGTTOU {
		// Trying to handle these signals just hangs, so don't support them
		unsupported = false
	}
	if unsupported {
		panic(fmt.Sprintf("YDBGo: The specified signal (%v) is not supported for signal notification", sig))
	}
}

// SignalNotify is a function to request notification, on notifyChan, that signal has occured.
// When the signal occurs the signal's number will be sent to notifyChan so that the user can handle the signal.
// The signal number is sent so that the same channel may be used to handle more than one signal.
// After the user's goroutine handles the signal it must send any boolean to ackChan
func SignalNotify(sig os.Signal, notifyChan chan os.Signal, ackChan chan struct{}, notifyWhen YDBHandlerFlag) {
	// Although this routine itself does not interact with the YottaDB runtime, use of this routine has an expectation that
	// the runtime is going to handle signals so make sure it is initialized.
	initCheck()
	validateNotifySignal(sig)
	sigNotificationMap.Store(sig, sigNotificationEntry{notifyChan, ackChan, notifyWhen})
}

// SignalReset removes a notification request for the given signals.
// No error is raised if the signal did not already have a notification request in effect.
func SignalReset(signals ...os.Signal) {
	for _, sig := range signals {
		sigNotificationMap.Delete(sig)
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
		if dbgSigHandling {
			fmt.Println("YDBGo: shutdownSignalGoroutines: Bypass shutdownSignalGoroutines as it has already run")
		}
		return
	}
	for i := range ydbSignals {
		close(ydbSignals[i].shutdownNow) // Will wakeup signal goroutine and make it exit
	}
	// Wait for the signal goroutines to exit but with a timeout
	doneChan := make(chan struct{})
	go func() {
		// Loop handling channel notifications as goroutines shutdown. If we are currently handling a fatal signal
		// like a SIGQUIT, that channel is active but is busy so will not respond to a shutdown request. For this
		// reason, we treat active goroutines the same as successfully shutdown goroutines so we don't delay
		// shutdown. No need to wait for something that is likely to not occur (The YottaDB handlers for fatal signals
		// drive a process-ending panic and never return).
		for {
			<-ydbShutdownCheck // A goroutine finished - check if all are shutdown or otherwise busy
			done := true
			for i := range ydbSignals {
				shutdownDone := ydbSignals[i].shutdownDone.Load()
				signalActive := ydbSignals[i].servicing.Load()
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
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDBGo: shutdownSignalGoroutines: All signal goroutines successfully closed or active")
		}
	case <-time.After(MaxSigShutdownWait):
		// Notify syslog that this timeout happened
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDBGo: shutdownSignalGoroutines: Timeout! Some signal goroutines did not shutdown")
		}
		errstr := getWrapperErrorMsg(ydberr.SIGGORTNTIMEOUT)
		syslogEntry(errstr)
	}
	shutdownSigGoroutines = true
	shutdownSigGoroutinesMutex.Unlock()
	// All signal routines should be finished or otherwise occupied
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDBGo: shutdownSignalGoroutines: Channel closings complete")
	}
}

// signalExitCallback is called from C by YottaDB to perform an exit when YottaDB gets a fatal signal.
// Its purpose make sure defers get called before exit, which it does by calling panic.
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
	panic(fmt.Sprintf("YDBGo: Fatal signal %d (%v) occurred", sig, sig))
}

var initCount atomic.Int64 // Increment when Init() called and decrement when Shutdown() called; shutdown when it reaches 0

// initializeYottaDB initializes the YottaDB engine if necessary.
// This is an atypical method of doing simple API initialization compared to
// other language APIs, where you can just make any API call, and initialization is automatic.
// But the Go wrapper needs to do its initialization differently to setup signal handling differently.
// Usually, YottaDB sets up its signal handling, but to work well with Go, Go itself needs to do the
// signal handling and forward it as needed to the YottaDB engine.
func initializeYottaDB() {
	var releaseMajorStr, releaseMinorStr string

	inInit.Lock()
	defer inInit.Unlock()     // Release lock when we leave this routine
	if initCount.Add(1) > 1 { // Must increment this before calling NewConn() below or it will fail initCheck()
		return // already initialized
	}

	// Init YottaDB engine/runtime
	// In Go, instead of ydb_init(), use ydb_main_lang_init() which lets us pass an exit handler for YDB to call on fatal signals.
	// We must make the exit handler panic to ensure defers get called before exit.
	// YDB calls the exit handler after rundown (equivalent to ydb_exit()).
	printEntry("initializeYottaDB()")
	rc := C.ydb_main_lang_init(C.YDB_MAIN_LANG_GO, C.ydb_signal_exit_callback)
	if rc != YDB_OK {
		panic(fmt.Sprintf("YDBGo: YottaDB initialization failed with return code %d", rc))
	}

	releaseInfoString := NewConn().Node("$ZYRELEASE").Get()

	// The returned output should have the YottaDB version as the 2nd token in the form rX.YY[Y] where:
	//   - 'r' is a fixed character
	//   - X is a numeric digit specifying the major version number
	//   - YY[Y] are basically the remaining digits and specify the minor release number.
	releaseInfoTokens := strings.Fields(releaseInfoString)
	releaseNumberStr := releaseInfoTokens[1] // Fetch second token
	if releaseNumberStr[:1] != "r" {         // Better start with 'r'
		panic(fmt.Sprintf("YDBGo: expected $ZYRELEASE to start with 'r' but it returned: %s", releaseInfoString))
	}
	releaseNumberStr = releaseNumberStr[1:]          // Remove starting 'r' in the release number
	dotIndex := strings.Index(releaseNumberStr, ".") // Look for the decimal point that separates major/minor values
	if dotIndex >= 0 {                               // Decimal point found
		releaseMajorStr = string(releaseNumberStr[:dotIndex])
		releaseMinorStr = string(releaseNumberStr[dotIndex+1:])
	} else {
		releaseMajorStr = releaseNumberStr // Isolate the major version number
		releaseMinorStr = "00"
	}
	// Note it is possible for either the major or minor release values to have a single letter suffix that is primarily
	// for use in a development environment (no production releases have character suffixes). If we get an error, try
	// removing a char off the end and retry.
	// The possibility of major release having a letter suffix prevents the use of fmt.Scanf()
	_, err := strconv.Atoi(releaseMajorStr)
	if err != nil {
		releaseMajorStr = releaseMajorStr[:len(releaseMajorStr)-1]
		_, err = strconv.Atoi(releaseMajorStr)
		if err != nil {
			panic(fmt.Sprintf("YDBGo: Failure trying to convert $ZYRELEASE major release number to integer: %s", err))
		}
	}
	_, err = strconv.Atoi(releaseMinorStr)
	if err != nil { // Strip off last char and try again
		releaseMinorStr = releaseMinorStr[:len(releaseMinorStr)-1]
		_, err = strconv.Atoi(releaseMinorStr)
		if err != nil {
			panic(fmt.Sprintf("YDBGo: Failure trying to convert $ZYRELEASE minor release number to integer: %s", err))
		}
	}
	// Verify we are running with the minimum YottaDB version or later
	runningYDBRelease, err := strconv.ParseFloat(releaseMajorStr+"."+releaseMinorStr, 64)
	if err != nil {
		panic(err) // shouldn't happen due to check above
	}
	minYDBRelease, err := strconv.ParseFloat(MinYDBRelease[1:], 64)
	if err != nil {
		panic("source code constant MinYDBRelease is not formatted correctly as rX.YY")
	}
	if minYDBRelease > runningYDBRelease {
		panic(fmt.Sprintf("YDBGo: Not running with at least minimum YottaDB release. Needed: %s  Have: r%s.%s",
			MinYDBRelease, releaseMajorStr, releaseMinorStr))
	}
	// Start up a goroutine for each signal we want to be notified of. This is so that if one signal is in process,
	// we can still catch a different signal and deliver it appropriately (probably to the same goroutine). For each signal,
	// bump our wait group counter so we don't proceed until all of these goroutines are initialized.
	// If you need to handle any more or fewer signals, alter ydbSignals at the top of this module.
	for i := range ydbSignals {
		wgSigInit.Add(1) // Indicate this signal goroutine is not yet initialized
		go waitForAndProcessSignal(&ydbSignals[i])
	}
	// Now wait for the goroutine to initialize and get signals all set up. When that is done, we can return
	wgSigInit.Wait()
}

// notifyUserSignalChannel calls a user signal routine associated with a given signal
func notifyUserSignalChannel(sig os.Signal, notification sigNotificationEntry, whenCalled ydbHndlrWhen) {
	// Notify user code via the supplied channel
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDBGo: goroutine-sighandler: Sending 'true' to notify",
			"channel", selectString(whenCalled == beforeYDBHandler, "(a)", "(b)"))
	}
	// Purge the acknowledgement channel of any content by reading the contents if any
	for len(notification.ackChan) > 0 {
		if dbgSigHandling {
			fmt.Println("YDBGo: goroutine-sighandler: Flush loop read",
				selectString(whenCalled == beforeYDBHandler, "(a)", "(b)"))
		}
		waitForSignalAckWTimeout(notification.ackChan,
			selectString(whenCalled == beforeYDBHandler, "(flush before)", "(flush after)"))
	}
	notification.notifyChan <- sig // Notify receiver which signal has been seen
	if notification.notifyWhen != NotifyAsyncYDBSigHandler {
		// Wait for acknowledgement that their handling is complete
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDBGo: goroutine-sighandler: Waiting for notify acknowledgement",
				selectString(whenCalled == beforeYDBHandler, "(a)", "(b)"))
		}
		waitForSignalAckWTimeout(notification.ackChan,
			selectString(whenCalled == beforeYDBHandler, "(wait before)", "(wait after)"))
	}
}

// waitForSignalAckWTimeout is used to wait for a signal with a timeout value of MaximumSignalAckWait
func waitForSignalAckWTimeout(ackChan chan struct{}, whatAck string) {
	select { // Wait for an acknowledgement but put a timer on it
	case <-ackChan:
	case <-time.After(MaxSigAckWait):
		syslogEntry(strings.Replace(getWrapperErrorMsg(ydberr.SIGACKTIMEOUT), "!AD", whatAck, 1))
	}
}

// waitForAndProcessSignal is used as a goroutine to wait for a signal notification and send it to YottaDB's signal dispatcher.
// This routine will also call a user handler for the signal if one has been set up using NotifySignal().
// The `signalIndex` parameter is an index into our various tables that store signal info.
func waitForAndProcessSignal(info *sigInfo) {
	sig := info.signal

	// We only need one of each type of signal so buffer depth is 1, but let it queue one additional
	sigchan := make(chan os.Signal, 2)
	// Create fresh channel for shutdown monitoring. Only one message need be passed on shutdown channel so no buffering.
	info.shutdownNow = make(chan struct{})
	// Tell Go to pass this signal to our channel
	signal.Notify(sigchan, sig)
	if dbgSigHandling {
		fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: Signal handler initialized for %v\n", sig)
	}
	wgSigInit.Done()  // Signal parent goroutine that we have completed initializing signal handling
	conn := NewConn() // an easy way to get an error buffer for the current goroutine
	allDone := false
	for !allDone {
		select {
		case <-sigchan:
			// Wait for signal notification
		case <-info.shutdownNow:
			allDone = true // Done by close of channel
		}
		if allDone {
			break // Got a shutdown request - fall out!
		}

		func() { // Inline function to provide scope for defer
			var rc C.int
			rc = YDB_OK // In case ydb_sig_dispatch() is bypassed by not running a handler
			info.servicing.Store(true)
			defer info.servicing.Store(false)

			// Define a simple function to notify YDB of the signal
			notifyYDB := func() {
				if dbgSigHandling && (sig != syscall.SIGURG) {
					// SIGURG happens almost continually, so don't report it.
					fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: notifying YottaDB signal dispatcher of signal %d (%v)", sig, sig)
				}
				signum := C.int(sig.(syscall.Signal)) // have to type assert before converting to an int
				// Note this call to ydb_sig_dispatch() does not pass a tptoken. The reason for this is that inside
				// this routine the tptoken at the time the signal actually occurs is unknown. The ydb_sig_dispatch()
				// routine itself does not need the tptoken nor does anything it calls but we do still need an
				// error buffer in case an error occurs that we need to return.
				rc = C.ydb_sig_dispatch(&conn.cconn.errstr, signum)
				// Handle errors so user doesn't have to
				switch rc {
				case YDB_OK:
					// Signal handling complete
				case YDB_DEFER_HANDLER:
					// Signal was deferred for some reason
					// Not an error, but the fact is logged below if dbgSigHandling is true
				case ydberr.CALLINAFTERXIT, -ydberr.CALLINAFTERXIT:
					// If CALLINAFTERXIT (positive or negative version) we're done - exit goroutine
					allDone = true
				default: // Some sort of error occurred during signal handling
					panic(fmt.Errorf("YDBGo: Error from ydb_sig_dispatch() of signal %d (%v): %w", sig, sig, conn.lastError(rc)))
				}
			}

			// See if we have a notification request for this signal
			value, entryFound := sigNotificationMap.Load(sig)
			// For fatal signals, the YDB handler probably won't return as it will (usually) throw a panic
			// but some fatal signals can be deferred under the right conditions (holding crit,
			// interrupts-disabled-state, etc).
			var notification sigNotificationEntry
			if entryFound {
				notification = value.(sigNotificationEntry)
				switch notification.notifyWhen {
				case NotifyBeforeYDBSigHandler, NotifyAsyncYDBSigHandler, NotifyInsteadOfYDBSigHandler:
					// Notify user code via the supplied channel specifying that this message is occurring BEFORE
					// the YDB handler has been driven.
					notifyUserSignalChannel(sig, notification, beforeYDBHandler)
				}
			}
			// Now notify the YDB runtime of this signal unless the handler is not supposed to run
			if !entryFound || (notification.notifyWhen != NotifyInsteadOfYDBSigHandler) {
				notifyYDB()
			}
			// Drive user notification if requested
			if entryFound && (notification.notifyWhen == NotifyAfterYDBSigHandler) {
				notifyUserSignalChannel(sig, notification, afterYDBHandler)
			}
			if dbgSigHandling {
				sigStatus := "complete"
				if rc == YDB_DEFER_HANDLER {
					sigStatus = "deferred"
				}
				if sig != syscall.SIGURG {
					// SIGURG happens almost continually, so don't report it.
					fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: Signal handling for signal %d (%v) %s", sig, sig, sigStatus)
				}
			}
		}()
	}
	signal.Stop(sigchan) // No more signal notification for this signal channel
	if dbgSigHandling {
		fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: exiting goroutine for signal %d (%v)", sig, sig)
	}
	info.shutdownDone.Store(true)  // Indicate this channel is closed
	ydbShutdownCheck <- struct{}{} // Notify shutdownSignalGoroutines that it needs to check if all channels closed now
}

// initCheck Panics if Init() has not been called
func initCheck() {
	if initCount.Load() == 0 {
		panic("YDBGo: Init() must be called first")
	}
}

// DB is the type returned by Init() which must be passed to Shutdown().
// It is used as a clue to the user that they must not forget to Shutdown()
type DB struct{}

// Init YottaDB access and return a handle that must be Shutdown before exit.
//   - Be sure to read the cautions at [Shutdown].
//   - Users should `defer yottadb.Shutdown(yottadb.Init())` from their main routine before using any other database function.
//   - Init returns a value of type DB that must be passed to the [Shutdown] function.
//
// Although Init could be made to be called automatically, this more explicit approach clarifies that Shutdown() MUST be
// called before process exit. Init may be called multiple times (e.g. by different goroutines) but [Shutdown]() must be
// called exactly once for each time Init() was called. See [Shutdown] for more detail on the fallout from incorrect usage.
func Init() *DB {
	initializeYottaDB()
	return &DB{}
}

// Shutdown invokes YottaDB's rundown function ydb_exit() to shut down the database properly.
// It MUST be called prior to process termination by any application that modifies the database.
// This is necessary particularly in Go because Go does not call the C atexit() handler (unless building with certain test options),
// so YottaDB itself cannot automatically ensure correct rundown of the database.
//
// If Shutdown() is not called prior to process termination, steps must be taken to ensure database integrity as documented in [Database Integrity]
// and unreleased locks may cause small subsequent delays (see [relevant LKE documentation]).
//
// Recommended way to call [Init]() is for your main routine to defer yottadb.Shutdown(yottadb.Init()) early in the main routine's initialization, and then
// for the main routine to confirm that all goroutines have stopped or have completely finished accessing the database before returning.
//
// Cautions:
//   - If goroutines that access the database are spawned, it is the main routine's responsibility to ensure that all such goroutines have
//     finished using the database before it calls yottadb.Shutdown(). Calling Shutdown() before they are done will cause problems.
//   - Avoid Go's os.Exit() function because it bypasses any defers (it is a low-level OS call).
//   - Care must be taken with any signal notifications (see [Go Using Signals]) to prevent them from causing premature exit.
//   - Note that Go *will* run defers on panic, but not on fatal signals such as SIGSEGV.
//
// Shutdown() must be called exactly once for each time [Init]() was called.
//
// [Database Integrity]: https://docs.yottadb.com/MultiLangProgGuide/goprogram.html#database-integrity
// [relevant LKE documentation]: https://docs.yottadb.com/AdminOpsGuide/mlocks.html#introduction
// [Go Using Signals]: https://docs.yottadb.com/MultiLangProgGuide/goprogram.html#go-using-signals
//
// [exceptions]: https://github.com/golang/go/issues/20713#issuecomment-1518197679
func Shutdown(handle *DB) error {
	// use the same mutex as Init because we don't want either to run simultaneously
	inInit.Lock()         // One goroutine at a time through here else we can get DATA-RACE warnings accessing wgexit wait group
	defer inInit.Unlock() // Release lock when we leave this routine
	if initCount.Load() == 0 {
		panic("YDBGo: Shutdown() called more times than Init()")
	}
	if initCount.Add(-1) > 0 {
		// Don't shutdown if some other goroutine is still using the dbase
		return nil
	}

	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDBGo: Exit(): YDB Engine shutdown started")
	}
	// When we run ydb_exit(), set up a timer that will pop if ydb_exit() gets stuck in a deadlock or whatever. We could
	// be running after some fatal error has occurred so things could potentially be fairly screwed up and ydb_exit() may
	// not be able to get the lock. We'll give it the given amount of time to finish before we give up and just exit.
	exitdone := make(chan struct{})
	wgexit.Add(1)
	go func() {
		_ = C.ydb_exit()
		wgexit.Done()
	}()
	wgexit.Add(1) // And run our signal goroutine cleanup in parallel
	go func() {
		shutdownSignalGoroutines()
		wgexit.Done()
	}()
	// And now, set up our channel notification for when those both ydb_exit() and signal goroutine shutdown finish
	go func() {
		wgexit.Wait()
		close(exitdone)
	}()

	// Wait for either ydb_exit to complete or the timeout to expire but how long we wait depends on how we are ending.
	// If a signal drove a panic, we have a much shorter wait as it is highly likely the YDB engine lock is held and
	// ydb_exit() won't be able to grab it causing a hang. The timeout is to prevent the hang from becoming permanent.
	// This is not a real issue because the signal handler would have driven the exit handler to clean things up already.
	// On the other hand, if this is a normal exit, we need to be able to wait a reasonably long time in case there is
	// a significant amount of data to flush.
	exitWait := MaxNormalExitWait
	if ydbSigPanicCalled.Load() {
		exitWait = MaxPanicExitWait
	}
	var errstr string
	var errNum int
	select {
	case <-exitdone:
		// We don't really care at this point what the return code is as we're just trying to run things down the
		// best we can as this is the end of using the YottaDB engine in this process.
	case <-time.After(exitWait):
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDBGo: Shutdown(): Wait for ydb_exit() expired")
		}
		errstr = getWrapperErrorMsg(ydberr.DBRNDWNBYPASS)
		errNum = ydberr.DBRNDWNBYPASS
		if !ydbSigPanicCalled.Load() {
			// If we panic'd due to a signal, we definitely have run the exit handler as it runs before the panic is
			// driven so we can bypass this message in that case.
			syslogEntry(errstr)
		}
	}
	if errstr != "" {
		return newError(errNum, errstr)
	}
	return nil
}
