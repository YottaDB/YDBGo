//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2020-2022 YottaDB LLC and/or its subsidiaries.	//
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// #include "libyottadb.h"
// extern void *ydb_get_gowrapper_panic_callback_funcvp(void);
import "C"

// Table of signals that the wrapper handles and passes to YottaDB
var ydbSignalList = []syscall.Signal{
	// Signal           Index in array
	syscall.SIGABRT, // [0]
	syscall.SIGALRM, // [1]
	syscall.SIGBUS,  // [2]
	syscall.SIGCONT, // [3]
	syscall.SIGFPE,  // [4]
	syscall.SIGHUP,  // [5]
	syscall.SIGILL,  // [6]
	syscall.SIGINT,  // [7]
	// syscall.SIGIO - this is a duplicate of SIGURG
	// syscall.SIGIOT - this is a duplicate of SIGABRT
	syscall.SIGQUIT, // [8]
	syscall.SIGSEGV, // [9]
	syscall.SIGTERM, // [10]
	syscall.SIGTRAP, // [11]
	syscall.SIGTSTP, // [12]
	syscall.SIGTTIN, // [13]
	syscall.SIGTTOU, // [14]
	syscall.SIGURG,  // [15]
	syscall.SIGUSR1, // [16]
}

// signalsHandled is the count of the signals the wrapper gets notified of and passes on to YottaDB. This matches with
// the number of signals (and thus also the number of goroutines launched running waitForAndProcessSignal() as well as
// being the dimension of the ydbShutdownChannel array below that has one slot for each signal handled.
var signalsHandled int = len(ydbSignalList)

// This is a map value element used to look up user signal notification channel and flags (one handler per signal)
type sigNotificationMapEntry struct {
	notifyChan chan bool      // Channel for user to be notified of a signal being received.
	ackChan    chan bool      // Channel used to acknowledge that processing for a signal has completed.
	notifyWhen YDBHandlerFlag // When/if YDB handler is driven in relation to user notification
}

var sigNotificationMap map[syscall.Signal]sigNotificationMapEntry

var wgSigInit sync.WaitGroup           // Used to make sure signals are setup before initializeYottaDB() exits
var wgSigShutdown sync.WaitGroup       // Used to wait for all signal threads to shutdown
var ydbInitMutex sync.Mutex            // Mutex for access to initialization
var ydbShutdownChannel []chan int      // Array of channels used to shutdown signal handling goroutines
var ydbShutdownCheck chan int          // Channel used to check if all signal routines have been shutdown
var shutdownSigGortns bool             // We have been through shutdownSignalGoroutines()
var shutdownSigGortnsMutex sync.Mutex  // Serialize access to shutdownSignalGoroutines()
var sigNotificationMapMutex sync.Mutex // Mutex for access to user sig handler map
var ydbSignalActive []uint32           // Array of indicators indicating indexed signal handlers are active
var ydbShutdownComplete []uint32       // Array of flags that indexed signal goroutine shutdown is complete

// validateNotifySignal verifies that the specified signal is valid for RegisterSignalNotify()/UnregisterSignalNotify()
func validateNotifySignal(sig syscall.Signal) error {
	// Verify the supplied signal is one that we support with this function. Note this list contains all of the signals
	// that the wrapper traps as seen below in the initializeYottaDB() function except those signals that cause
	// problems if handlers other than YottaDB's handler is driven (e.g. SIGTSTP, SIGTTIN, etc). It is up to the user
	// to know which signals are duplicates of others so if separate handlers are set for say SIGABRT and SIGIOT, whichever
	// handler was set last is the one that gets both signals as they are the same signal.
	switch sig { // Validate the signal chosen
	case syscall.SIGABRT: // Same as SIGIOT
	case syscall.SIGALRM:
	case syscall.SIGBUS:
	case syscall.SIGCONT:
	case syscall.SIGFPE:
	case syscall.SIGHUP:
	case syscall.SIGILL:
	case syscall.SIGINT:
	// case syscall.SIGIO: // Same as SIGPOLL & SIGURG
	// case syscall.SIGIOT: // Same as SIGABRT (both can't exist as cases in a switch)
	case syscall.SIGQUIT:
	case syscall.SIGSEGV:
	case syscall.SIGTERM:
	case syscall.SIGTRAP:
	// case syscall.SIGTSTP: // Trying to handle this signal just hangs
	// case syscall.SIGTTIN: // Trying to handle this signal just hangs
	// case syscall.SIGTTOU: // Trying to handle this signal just hangs
	case syscall.SIGURG: // Same as SIGPOLL and SIGIO - happens almost constantly so be careful if used
	case syscall.SIGUSR1:
	default:
		panic(fmt.Sprintf("YDB: The specified signal (%v) is not supported for signal notification by yottadb."+
			"[Un]RegisterSignalNotify()", sig))
	}
	// Note no error is currently returned but will when YDB#790 is complete.
	return nil
}

// RegisterSignalNotify is a function to request notification of a signal occurring on a supplied channel. Additionally,
// the user should respond to the same channel when they are done. To make sure this happens, the first step in the
// routine listening for the signal should be a defer statement that sends an acknowledgement back that handling is
// complete.
func RegisterSignalNotify(sig syscall.Signal, notifyChan, ackChan chan bool, notifyWhen YDBHandlerFlag) error {
	// Although this routine itself does not interact with the YottaDB runtime, use of this routine has an expectation that
	// the runtime is going to handle signals so make sure it gets initialized.
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		initializeYottaDB()
	}
	err := validateNotifySignal(sig)
	if nil != err {
		panic(fmt.Sprintf("YDB: %v", err))
	}
	sigNotificationMapMutex.Lock()
	if nil == sigNotificationMap {
		sigNotificationMap = make(map[syscall.Signal]sigNotificationMapEntry)
	}
	sigNotificationMap[sig] = sigNotificationMapEntry{notifyChan, ackChan, notifyWhen}
	sigNotificationMapMutex.Unlock()
	// Note there is no error return right now but one will be added for YDB#790 in the future so defining it now.
	return nil
}

// UnRegisterSignalNotify removes a notification request for the given signal. No error is raised if the signal did not already
// have a notification request in effect.
func UnRegisterSignalNotify(sig syscall.Signal) error {
	err := validateNotifySignal(sig)
	if nil != err {
		panic(fmt.Sprintf("YDB: %v", err))
	}
	sigNotificationMapMutex.Lock()
	delete(sigNotificationMap, sig)
	sigNotificationMapMutex.Unlock()
	// There is no error to return yet but there will be when YDB#790 is complee
	return nil
}

// shutdownSignalGoroutines is a function to stop the signal handling goroutines used to tell the YDB engine what signals
// have occurred. No signals are recognized by the Go wrapper or YottaDB once this is done. All signal handling reverts to
// Go standard handling.
func shutdownSignalGoroutines() {
	printEntry("shutdownSignalGoroutines")
	shutdownSigGortnsMutex.Lock()
	if shutdownSigGortns { // Nothing to do if already done
		shutdownSigGortnsMutex.Unlock()
		if dbgSigHandling {
			fmt.Println("YDB: shutdownSignalGoroutines: Bypass shutdownSignalGoroutines as it has already run")
		}
		return
	}
	for i := 0; signalsHandled > i; i++ {
		close(ydbShutdownChannel[i]) // Will wakeup signal goroutine and make it exit
	}
	// Wait for the signal goroutines to exit but with a timeout
	done := make(chan struct{})
	go func() {
		// Loop handling channel blips as goroutines shutdown. Take into account any signals that are in-process also
		// as those routines will be unable to shutdown so no use waiting for them until timeout. Note, if this loop
		// is unable to return, this goroutine will die when the process dies.
		sigRtnScanStopped := false
		for !sigRtnScanStopped {
			select {
			case <-ydbShutdownCheck: // A goroutine finished - check if all are shutdown or otherwise busy
				var i int
				for i = 0; signalsHandled > i; i++ {
					lclShutdownComplete := (1 == atomic.LoadUint32(&ydbShutdownComplete[i]))
					lclSignalActive := (1 == atomic.LoadUint32(&ydbSignalActive[i]))
					if !lclShutdownComplete && !lclSignalActive {
						// A goroutine is not shutdown and is not active - need to wait for more
						// goroutine(s) to complete so break out of this scan loop
						break
					}
				}
				if signalsHandled == i { // We made it all the way through the loop satisfactorily
					close(done)              // Notify select loop below that this is complete
					sigRtnScanStopped = true // Escape the sig gortn scan loop
				}
			}
		}
	}()
	select {
	case <-done: // All signal monitoring goroutines are shutdown!
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: shutdownSignalGoroutines: All signal goroutines successfully closed or active")
		}
	case <-time.After(time.Duration(MaximumSigShutDownWait) * time.Second):
		// Note, if these goroutines don't shutdown, it is not considered significant enough to warrant a warning to
		// the syslog but here is where we would add one if that opinion changes.
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: shutdownSignalGoroutines: Timeout! Some signal threads did not shutdown")
		}
	}
	shutdownSigGortns = true
	shutdownSigGortnsMutex.Unlock()
	// All signal routines should be finished or otherwise occupied
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
	var errStr BufferT
	var err error
	var releaseNumberStr, releaseMajorStr, releaseMinorStr string

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
	// Make a call to see what YottaDB version we are dealing with to verify this version of the wrapper works
	// with this version of YottaDB. Note this operation sets the flag inInit for the duration of the call to
	// ValE()/ValST() so it does not report a nested SimpleAPI call.
	atomic.StoreUint32(&inInit, 1)
	defer func() { atomic.StoreUint32(&inInit, 0) }() // Turn the flag back off when we leave if we haven't already done so
	defer errStr.Free()
	errStr.Alloc(YDB_MAX_ERRORMSG)
	releaseInfoString, err := ValE(NOTTP, &errStr, "$ZYRELEASE", []string{})
	if nil != err {
		panic(fmt.Sprintf("YDB: YottaDB fetch of $ZYRELEASE failed wih return code %s", err))
	}
	atomic.StoreUint32(&inInit, 0)
	// The returned output should have the YottaDB version as the 2nd token in the form rxyy[y] where:
	//   - 'r' is a fixed character
	//   - x is a numeric digit specifying the major version number
	//   - yy[y] are basically the remaining digits and specify the minor release number.
	releaseInfoTokens := strings.Fields(releaseInfoString)
	releaseNumberStr = releaseInfoTokens[1] // Fetch second token
	if "r" != releaseNumberStr[:1] {        // Better start with 'r'
		panic(fmt.Sprintf("YDB: Unexpected output from fetch of $ZYRELEASE: %s", releaseInfoString))
	}
	releaseNumberStr = releaseNumberStr[1:]          // Remove starting 'r' in the release number
	dotIndex := strings.Index(releaseNumberStr, ".") // Look for the decimal point that separates major/minor values
	if 0 <= dotIndex {                               // Decimal point found
		releaseMajorStr = string(releaseNumberStr[:dotIndex])
		releaseMinorStr = string(releaseNumberStr[dotIndex+1:])
	} else {
		releaseMajorStr = releaseNumberStr // Isolate the major version number
		releaseMinorStr = "00"
	}
	runningYDBReleaseMajor, err := strconv.Atoi(releaseMajorStr)
	if nil != err {
		panic(fmt.Sprintf("YDB: Failure trying to convert major release to int: %s", err))
	}
	// Note it is possible for the minor version number to have a letter suffix so if our conversion fails, strip
	// a char off the end and retry.
	runningYDBReleaseMinor, err := strconv.Atoi(releaseMinorStr)
	if nil != err { // Strip off last char and try again
		releaseMinorStr = releaseMinorStr[1 : len(releaseMinorStr)-1]
		runningYDBReleaseMinor, err = strconv.Atoi(releaseMinorStr)
		if nil != err {
			panic(fmt.Sprintf("YDB: Failure trying to convert minor release to int: %s", err))
		}
	}
	// Verify we are running with the minimum YottaDB version or later
	if (MinimumYDBReleaseMajor > runningYDBReleaseMajor) ||
		((MinimumYDBReleaseMajor == runningYDBReleaseMajor) && (MinimumYDBReleaseMinor > runningYDBReleaseMinor)) {
		panic(fmt.Sprintf("YDB: Not running with at least minimum YottaDB release. Needed: %s  Have: r%d.%d",
			MinimumYDBRelease, runningYDBReleaseMajor, runningYDBReleaseMinor))
	}
	// Create the shutdown channel array now that we know (at run time) how many we need. See the ydbSignalList defined
	// above for the list of signals. Also create the ydbSignalActive array (same size and index).
	ydbShutdownChannel = make([]chan int, signalsHandled) // Array of channels for each signal go routine for shutdown notification
	ydbShutdownCheck = make(chan int)                     // Channel used to notify to check if goroutine shutdown is complete
	ydbSignalActive = make([]uint32, signalsHandled)      // Array of flags that signal handling is active
	ydbShutdownComplete = make([]uint32, signalsHandled)  // Array of flags that goroutine shutdown is complete
	// Start up a goroutine for each signal we want to be notified of. This is so that if one signal is in process,
	// we can still catch a different signal and deliver it appropriately (probably to the same thread). For each signal,
	// bump our wait group counter so we don't proceed until all of these goroutines are initialized. Add or remove any
	// signals to be handled from the ydbSignalsList array up top of this module.
	for indx := 0; indx < signalsHandled; indx++ {
		wgSigInit.Add(1) // Indicate this signal goroutine is not yet initialized
		go waitForAndProcessSignal(indx)
	}
	// Now wait for the goroutine to initialize and get signals all set up. When that is done, we can return
	wgSigInit.Wait()
	atomic.StoreUint32(&ydbInitialized, 1) // YottaDB wrapper is now initialized
	ydbInitMutex.Unlock()
}

// waitForSignalAckWTimeout is used to wait for a signal with a timeout value of MaximumSignalAckWait
func waitForSignalAckWTimeout(ackChan chan bool, whatAck string) {
	select { // Wait for an acknowledgement but put a timer on it
	case _ = <-ackChan:
	case <-time.After(time.Duration(MaximumSigAckWait) * time.Second):
		syslogEntry("YDB-W-SIGACKTIMEOUT Signal completion acknowledgement timeout:" + whatAck)
	}
}

// waitForAndProcessSignal is used as a goroutine to wait for a signal notification and send it to YottaDB's signal dispatcher.
// This routine will also drive a user handler for the subset of signals we allow to have user handlers if one is defined.
func waitForAndProcessSignal(shutdownChannelIndx int) {
	// This Go routine needs its own errstr buffer as it continues running until ydb_exit() is called
	var errstr BufferT
	var cbuft *C.ydb_buffer_t
	var allDone bool
	var shutdownChannel *chan int
	var sigStatus string
	var sig syscall.Signal
	var rc C.int

	sig = ydbSignalList[shutdownChannelIndx]
	shutdownChannel = &ydbShutdownChannel[shutdownChannelIndx]
	errstr.Alloc(YDB_MAX_ERRORMSG) // Initialize error string to hold errors
	// We only need one of each type of signal so buffer depth is 1, but let it queue one additional
	sigchan := make(chan os.Signal, 2)
	// Only one message need be passed on shutdown channel so no buffering
	*shutdownChannel = make(chan int)
	// Tell Go to pass this signal to our channel
	signal.Notify(sigchan, sig)
	if dbgSigHandling {
		fmt.Fprintf(os.Stderr, "YDB: goroutine-sighandler: Signal handler initialized for %v\n", sig)
	}
	wgSigInit.Done() // Signal parent goroutine that we have completed initializing signal handling
	// Although we typically refer to individual signals with their syscall.Signal type, we do need to have them
	// in their numeric form too to pass to C when we want to dispatch their YottaDB handler. Unfortnuately,
	// switching between designations is a bit silly.
	csignum := fmt.Sprintf("%d", sig)
	signum, err := strconv.Atoi(csignum)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error translating signal to numeric: %v", csignum))
	}
	cbuft = errstr.getCPtr() // Get pointer to ydb_buffer_t embedded in errstr's BufferT we need to pass to C
	allDone = false
	for !allDone {
		select {
		case _ = <-sigchan:
			// Wait for signal notification
		case _ = <-*shutdownChannel:
			allDone = true // Done by close of channel
		}
		if allDone {
			break // Got a shutdown request - fall out!
		}
		func() { // Inline function to provide scope for defer
			rc = 0 // In case ydb_sig_dispatch() is bypassed by not running a handler
			atomic.StoreUint32(&ydbSignalActive[shutdownChannelIndx], 1)
			defer func() {
				atomic.StoreUint32(&ydbSignalActive[shutdownChannelIndx], 0) // Shut flag back off when we are done
			}()
			// See if we have a notification request for this signal
			sigNotificationMapMutex.Lock()
			sigHndlrEntry, sigHndlrEntryFound := sigNotificationMap[sig]
			sigNotificationMapMutex.Unlock()
			// If we want to do our user notification before the YDB handler runs, in parallel with the YDB handler,
			// or if we don't want to run the YDB handler at all, drive the notification here. The only other case is
			// doing the notification after the YDB handler is driven but we'll do that later. For fatal signals, we
			// probably won't return from the YDB handler as it will (usually) throw a panic but some fatal signals can
			// be deferred under the right conditions (holding crit, interrupts-disabled-state, etc).
			if sigHndlrEntryFound {
				switch sigHndlrEntry.notifyWhen {
				case NotifyBeforeYDBSigHandler:
					fallthrough
				case NotifyAsyncYDBSigHandler:
					fallthrough
				case NotifyInsteadOfYDBSigHandler:
					// Notify user code via the supplied channel
					if dbgSigHandling {
						fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Sending 'true' to notify",
							"channel (a)")
					}
					// Purge the acknowledgement channel of any content by reading the contents if any
					for 0 < len(sigHndlrEntry.ackChan) {
						if dbgSigHandling {
							fmt.Println("YDB: goroutine-sighandler: Flush loop read (a)")
						}
						waitForSignalAckWTimeout(sigHndlrEntry.ackChan, "(flush before)")
					}
					sigHndlrEntry.notifyChan <- true // Notify receiver the signal has been seen
					if NotifyAsyncYDBSigHandler != sigHndlrEntry.notifyWhen {
						// Wait for acknowledgement that their handling is complete
						if dbgSigHandling {
							fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Waiting for notify",
								"acknowledgement (a)")
						}
						waitForSignalAckWTimeout(sigHndlrEntry.ackChan, "(wait before)")
					}
				case NotifyAfterYDBSigHandler:
				}
			}
			// Now notify the YDB runtime of this signal unless the handler is not supposed to run
			if !sigHndlrEntryFound || (NotifyInsteadOfYDBSigHandler != sigHndlrEntry.notifyWhen) {
				// Note this call to ydb_sig_dispatch() does not pass a tptoken. The reason for this is that inside
				// this routine the tptoken at the time the signal actually pops in unknown. The ydb_sig_dispatch()
				// routine itself does not need the tptoken nor does anything it calls but we do still need the
				// error buffer in case an error occurs that we need to return.
				if dbgSigHandling && (syscall.SIGURG != sig) {
					// Note our passage if not SIGURG (happens almost continually so be silent)
					fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Go sighandler - sending notification",
						"for signal", signum, " to the YottaDB signal dispatcher")
				}
				rc = C.ydb_sig_dispatch(cbuft, C.int(signum))
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
						panic(fmt.Sprintf("YDB: Failure from ydb_sig_dispatch() (rc = %d): %s", rc,
							errstring))
					}
				}
			}
			// Drive user notification if requested
			if sigHndlrEntryFound && (NotifyAfterYDBSigHandler == sigHndlrEntry.notifyWhen) {
				if dbgSigHandling {
					fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Sending 'true' to notify",
						"channel (b)")
				}
				// Purge the ackknowledgement channel of any content by reading the contents if any
				for 0 < len(sigHndlrEntry.ackChan) {
					if dbgSigHandling {
						fmt.Println("YDB: goroutine-sighandler: Flush loop read (b)")
					}
					waitForSignalAckWTimeout(sigHndlrEntry.ackChan, "(flush after)")
				}
				sigHndlrEntry.notifyChan <- true
				// Wait for acknowledgement that their handling is complete
				if dbgSigHandling {
					fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Waiting for notify",
						"acknowledgement (b)")
				}
				waitForSignalAckWTimeout(sigHndlrEntry.ackChan, "(wait after)")
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
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
				// These are the deferrable signals - see if need deferring
				if YDB_DEFER_HANDLER == rc {
					if dbgSigHandling {
						sigStatus = "deferred"
					}
				}
			}
			if dbgSigHandling && (syscall.SIGURG != sig) {
				// Note our passage if not SIGURG (SIGURG happens almost continually so be silent)
				fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Signal handling for signal", signum, sigStatus)
			}
		}()
	}
	signal.Stop(sigchan)      // No more signal notification for this signal channel
	runtime.KeepAlive(errstr) // Keep errstr (and its internal C buffer) around till sure we are done with it
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDB: goroutine-sighandler: Goroutine for signal index", shutdownChannelIndx, "exiting..")
	}
	atomic.StoreUint32(&ydbShutdownComplete[shutdownChannelIndx], 1) // Indicate this channel is closed

	ydbShutdownCheck <- 0 // Notify shutdownSignalGoroutines that it needs to check if all channels closed now
}
