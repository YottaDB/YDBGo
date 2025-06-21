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

// Initialize and Shutdown YottaDB

package yottadb

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

// #include "libyottadb.h"
// extern void *ydb_signal_exit_callback(void);
import "C"

// DB is the type returned by Init() which must be passed to Shutdown().
// It is used as a clue to the user that they must not forget to Shutdown()
type DB struct {
	YDBRelease float64 // release version of the installed YottaDB
}

var internalDB = &DB{} // make this a single instance per app because we can: so that e.g. mcall can check YDBRelease without having access to a particular DB instance

// Init and Exit globals
var wgexit sync.WaitGroup
var inInit sync.Mutex        // Mutex for access to init AND exit
var wgSigInit sync.WaitGroup // Used to make sure signals are setup before Init() exits
var initCount atomic.Int64   // Increment when Init() called and decrement when Shutdown() called; shutdown when it reaches 0

// Init initializes the YottaDB engine and set up signal handling.
// Init may be called multiple times (e.g. by different goroutines) but [Shutdown]() must be called exactly once
// for each time Init() was called. See [Shutdown] for more detail on the fallout from incorrect usage.
// Although Init could have been made to happen automatically, this more explicit approach clarifies that
// Shutdown() MUST be called before process exit.
//   - Be sure to read the cautions at [Shutdown].
//   - Init returns a value of type DB that must be passed to the [Shutdown] function.
//   - Returns YDBError with Code=ydberr.Init on failure, which will also wrap any errors from YottaDB in the error chain.
//
// Users should defer [Shutdown] from their main routine before using other database functions; for example:
//
//	db, err := yottadb.Init()
//	if err != nil {
//	  panic(err)
//	}
//	defer yottadb.Shutdown(db)
//	  ... user code to use the database ...
func Init() (*DB, error) {
	// This is an atypical method of doing simple API initialization compared to
	// other language APIs, where you can just make any API call, and initialization is automatic.
	// But the Go wrapper needs to do its initialization differently to setup signal handling differently.
	// Usually, YottaDB sets up its signal handling, but to work well with Go, Go itself needs to do the
	// signal handling and forward it as needed to the YottaDB engine.

	inInit.Lock()
	defer inInit.Unlock()     // Release lock when we leave this routine
	if initCount.Add(1) > 1 { // Must increment this before calling NewConn() below or it will fail initCheck()
		return internalDB, nil // already initialized
	}

	// Init YottaDB engine/runtime
	// In Go, instead of ydb_init(), use ydb_main_lang_init() which lets us pass an exit handler for YDB to call on fatal signals.
	// We must make the exit handler panic to ensure defers get called before exit.
	// YDB calls the exit handler after rundown (equivalent to ydb_exit()).
	printEntry("YottaDB Init()")

	conn := NewConn()
	status := C.ydb_main_lang_init(C.YDB_MAIN_LANG_GO, C.ydb_signal_exit_callback)
	if status != YDB_OK {
		dberror := newYDBError(int(status), conn.recoverMessage(status))
		return nil, newYDBError(ydberr.Init, "YottaDB initialization failed", dberror)
	}

	var releaseMajorStr, releaseMinorStr string
	releaseInfoString := conn.Node("$ZYRELEASE").Get()

	// The returned output should have the YottaDB version as the 2nd token in the form rX.YY[Y] where:
	//   - 'r' is a fixed character
	//   - X is a numeric digit specifying the major version number
	//   - YY[Y] are basically the remaining digits and specify the minor release number.
	releaseInfoTokens := strings.Fields(releaseInfoString)
	releaseNumberStr := releaseInfoTokens[1] // Fetch second token
	if releaseNumberStr[:1] != "r" {         // Better start with 'r'
		return nil, newYDBError(ydberr.Init, fmt.Sprintf("expected YottaDB version $ZYRELEASE value to start with 'r' but it returned: %s", releaseInfoString))
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
			return nil, newYDBError(ydberr.Init, fmt.Sprintf("Failure trying to convert YottaDB version $ZYRELEASE (%s) major release number to integer", releaseInfoString))
		}
	}
	_, err = strconv.Atoi(releaseMinorStr)
	if err != nil { // Strip off last char and try again
		releaseMinorStr = releaseMinorStr[:len(releaseMinorStr)-1]
		_, err = strconv.Atoi(releaseMinorStr)
		if err != nil {
			return nil, newYDBError(ydberr.Init, fmt.Sprintf("Failure trying to convert YottaDB version $ZYRELEASE (%s) minor release number to integer", releaseInfoString))
		}
	}
	// Verify we are running with the minimum YottaDB version or later
	runningYDBRelease, err := strconv.ParseFloat(releaseMajorStr+"."+releaseMinorStr, 64)
	if err != nil {
		panic(newYDBError(ydberr.Init, err.Error(), err)) // shouldn't happen due to check above
	}
	minYDBRelease, err := strconv.ParseFloat(MinYDBRelease[1:], 64)
	if err != nil {
		panic(newYDBError(ydberr.Init, fmt.Sprintf("source code constant MinYDBRelease (%s) is not formatted correctly as rX.YY", MinYDBRelease)))
	}
	if minYDBRelease > runningYDBRelease {
		return nil, newYDBError(ydberr.Init, fmt.Sprintf("Not running with at least minimum YottaDB release. Needed: %s  Have: r%s.%s",
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
	internalDB = &DB{runningYDBRelease}
	return internalDB, nil
}

// initCheck Panics if Init() has not been called
func initCheck() {
	if initCount.Load() == 0 {
		panic(newYDBError(ydberr.Init, "Init() must be called first"))
	}
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
// Shutdown() must be called exactly once for each time [Init]() was called, and shutdown will not occur until the last time.
// Return ShutdownBypassError if it has to wait longer than MaxNormalExitWait for signal handling goroutines to exit.
// No other errors are returned. Panics if Shutdown is called more than Init.
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
		panic(newYDBError(ydberr.Shutdown, "Shutdown() called more times than Init()"))
	}
	if initCount.Add(-1) > 0 {
		// Don't shutdown if some other goroutine is still using the dbase
		return nil
	}

	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "Exit(): YDB Engine shutdown started")
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
	select {
	case <-exitdone:
		// We don't really care at this point what the return code is as we're just trying to run things down the
		// best we can as this is the end of using the YottaDB engine in this process.
	case <-time.After(exitWait):
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "Shutdown(): Wait for ydb_exit() expired")
		}
		errstr = "YottaDB database rundown may have been bypassed due to timeout - run MUPIP JOURNAL ROLLBACK BACKWARD / MUPIP JOURNAL RECOVER BACKWARD / MUPIP RUNDOWN"
		if !ydbSigPanicCalled.Load() {
			// If we panic'd due to a signal, we definitely have run the exit handler as it runs before the panic is
			// driven so we can bypass this message in that case.
			syslogEntry(errstr)
		}
	}
	if errstr != "" {
		return newYDBError(ydberr.ShutdownIncomplete, errstr)
	}
	return nil
}
