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

// ---- Signal Handling

// sigInfo holds info for each signal that YDBGo handles and defers to YottaDB.
type sigInfo struct {
	signal       os.Signal      // signal number for this entry
	notifyChan   chan os.Signal // user-supplied channel notify of incoming signal
	shutdownNow  chan struct{}  // Channel used to shutdown signal handling goroutine
	shutdownDone atomic.Bool    // indicate that goroutine shutdown is complete
	servicing    atomic.Bool    // indicate signal handler is active
	conn         *Conn          // just acts as a place to store errstr for use by this signal's NotifyYDB()
}

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

// ydbSignalMap stores data for signals that the wrapper handles and passes to YottaDB.
// It is populated by InitializeYottaDB()
var ydbSignalMap sync.Map
var ydbSigPanicCalled atomic.Bool // True when our exit is panic driven due to a signal

// Init and Exit globals
var wgexit sync.WaitGroup
var inInit sync.Mutex                      // Mutex for access to init AND exit
var wgSigInit sync.WaitGroup               // Used to make sure signals are setup before initializeYottaDB() exits
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

// lookupYDBSignal returns a pointer to the sigInfo entry related to signal sig.
func lookupYDBSignal(sig os.Signal) *sigInfo {
	initCheck() // Make sure ydbSignalMap is populated
	value, ok := ydbSignalMap.Load(sig)
	if !ok {
		panic(fmt.Errorf("YDBGo: The specified signal %d (%v) is not a YottaDB signal so is unsupported for signal notification", sig, sig))
	}
	info := value.(*sigInfo)
	return info
}

// validateYDBSignal verifies that the specified signal is valid for SignalNotify()/SignalReset()
func validateYDBSignal(sig os.Signal) *sigInfo {
	// Verify the supplied signal is one that we support with this function. This list contains all of the signals
	// that the wrapper traps in the initializeYottaDB() except those signals that cause
	// problems if handlers other than YottaDB's handler is driven (SIGTSTP, SIGTTIN, etc).
	// It is up to the user to know which signals are duplicates of others so if separate handlers
	// are set for say SIGABRT and SIGIOT, whichever handler was set last is the one that gets both signals
	// (because both constants are the the same signal).

	info := lookupYDBSignal(sig)
	if sig == syscall.SIGTSTP || sig == syscall.SIGTTIN || sig == syscall.SIGTTOU {
		panic(fmt.Errorf("YDBGo: handling signal %d (%v) hangs, so handling it is not supported", sig, sig))
	}
	return info
}

// SignalNotify relays incoming signals to notifyChan specifically for signals used by YottaDB.
// After calling SignalNotify, the user is then responsible to call [NotifyYDB]() at the start or end of their own handler
// to allow YottaDB to process the signal. The user can revert behaviour to the YDBGo default
// with [SignalReset](), after which YDBGo will once again call [NotifyYDB]().
//   - Users may opt to use the standard library's [Signal.Notify]() instead of this function to be notified of signals, but
//
// this will notify them in parallel with YottaDB. However, they must not call Signal.Stop() (see below).
//   - Do not call [Signal.Stop](), [Signal.Ignore]() or [Signal.Reset]() for any of the YottaDB-specific signals
//
// unless you understand that it will prevent [NotifyYDB]() from being called, and will affect YottaDB timers
// or other functionality.
//
// YottaDB-specific signals are listed in the source in [YDBSignals].
//
// See [YottaDB signals].
// [YottaDB signals information]: https://docs.yottadb.com/MultiLangProgGuide/programmingnotes.html#signals
func SignalNotify(notifyChan chan os.Signal, signals ...os.Signal) {
	// Although this routine itself does not interact with the YottaDB runtime, use of this routine has an expectation that
	// the runtime is going to handle signals so let's make sure it is initialized.
	initCheck()
	for _, sig := range signals {
		info := validateYDBSignal(sig)
		info.notifyChan = notifyChan
	}
}

// SignalReset stops notifying the user of the given signals and reverts to default YDBGo signal behaviour which simply calls [NotifyYDB].
// No error is raised if the signal did not already have a notification request in effect.
func SignalReset(signals ...os.Signal) {
	for _, sig := range signals {
		info := lookupYDBSignal(sig)
		info.notifyChan = nil
	}
}

// NotifyYDB calls the YottaDB signal handler for sig.
// Return whether YottaDB returned a CALLINAFTEREXIT error.
// If YottaDB deferred handling of the signal, return false; otherwise return true.
// Panic on YottaDB errors.
func NotifyYDB(sig os.Signal) bool {
	value, ok := ydbSignalMap.Load(sig)
	if !ok {
		panic(fmt.Errorf("YDBGo: goroutine-sighandler: called NotifyYDB with a non-YottaDB signal %d (%v)", sig, sig))
	}
	info := value.(*sigInfo)
	conn := info.conn

	// Flag that YDB is servicing this signal
	info.servicing.Store(true)
	defer info.servicing.Store(false)

	if dbgSigHandling && (sig != syscall.SIGURG) {
		// SIGURG happens almost continually, so don't report it.
		fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: calling YottaDB signal handler for signal %d (%v)\n", sig, sig)
	}
	signum := C.int(sig.(syscall.Signal)) // have to type-assert before converting to an int
	// Note this call to ydb_sig_dispatch() does not pass a tptoken. The reason for this is that inside
	// this routine the tptoken at the time the signal actually occurs is unknown. The ydb_sig_dispatch()
	// routine itself does not need the tptoken nor does anything it calls but we do still need an
	// error buffer in case an error occurs that we need to return.
	rc := C.ydb_sig_dispatch(&conn.cconn.errstr, signum)
	// Handle errors so user doesn't have to
	switch rc {
	case YDB_OK:
		// Signal handling complete
	case YDB_DEFER_HANDLER:
		// Signal was deferred for some reason
		// Not an error, but the fact is logged
		fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: YottaDB deferred signal %d (%v)\n", sig, sig)
		return false
	case ydberr.CALLINAFTERXIT, -ydberr.CALLINAFTERXIT:
		// If CALLINAFTERXIT (positive or negative version) we're done - exit goroutine
		shutdownSignalGoroutine(info)
	default: // Some sort of error occurred during signal handling
		panic(fmt.Errorf("YDBGo: goroutine_sighandler: error from ydb_sig_dispatch() of signal %d (%v): %w", sig, sig, conn.lastError(rc)))
	}
	return true
}

// handleSignal is used as a goroutine to process all YottaDB-specific signals (listed in YDBSignals).
// It calls NotifyYDB() unless a user has requested notification of that signal using SignalNotify(),
// in which case it will notify the user who must call NotifyYDB().
// info specifies the signal to be processed.
func handleSignal(info *sigInfo) {
	sig := info.signal

	// We only need one of each type of signal so buffer depth is 1, but let it queue one additional signal.
	sigchan := make(chan os.Signal, 2)
	// Create fresh channel for shutdown monitoring.
	info.shutdownNow = make(chan struct{})
	// Tell Go to pass this signal to our channel.
	signal.Notify(sigchan, sig)
	if dbgSigHandling {
		fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: Signal handler initialized for %d (%v)\n", sig, sig)
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
		if info.notifyChan != nil {
			// Notify user code via the supplied channel
			if dbgSigHandling {
				fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: notifying user-specified channel of signal %d (%v)\n", sig, sig)
			}
			// Send to channel without blocking (same as Signal.Notify)
			select {
			case info.notifyChan <- sig: // notify channel of signal, sending it a function to use to notify YDB
			default:
			}
		} else {
			// otherwise just run YDB handler function ourselves since user didn't hook this signal
			NotifyYDB(sig)
		}
	}
	signal.Stop(sigchan) // No more signal notification for this signal channel
	if dbgSigHandling {
		fmt.Fprintf(os.Stderr, "YDBGo: goroutine-sighandler: exiting goroutine for signal %d (%v)\n", sig, sig)
	}
	info.shutdownDone.Store(true)  // Indicate this channel is closed
	ydbShutdownCheck <- struct{}{} // Notify shutdownSignalGoroutines that it needs to check if all channels closed now
}

// sutdownSignalGoroutine tells routine for signal sig to shutdown.
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
		if dbgSigHandling {
			fmt.Println("YDBGo: shutdownSignalGoroutines: Bypass shutdownSignalGoroutines as it has already run")
		}
		return
	}
	// Send shutdown signal to each goroutine
	for _, sig := range YDBSignals {
		value, _ := ydbSignalMap.Load(sig)
		shutdownSignalGoroutine(value.(*sigInfo))
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

// ---- Init and Exit

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
	// If you need to handle any more or fewer signals, alter YDBSignals at the top of this module.
	for _, sig := range YDBSignals {
		// Populate ydbSignalMap
		info := sigInfo{sig, nil, make(chan struct{}), atomic.Bool{}, atomic.Bool{}, NewConn()}
		ydbSignalMap.Store(sig, &info)
		wgSigInit.Add(1) // Indicate this signal goroutine is not yet initialized
		go handleSignal(&info)
	}
	// Now wait for the goroutine to initialize and get signals all set up. When that is done, we can return
	wgSigInit.Wait()
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
