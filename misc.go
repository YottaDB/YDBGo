//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2020 YottaDB LLC and/or its subsidiaries.	//
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
	"log/syslog"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// #include "libyottadb.h"
import "C"

var wgexit sync.WaitGroup

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Miscellaneous functions
//
////////////////////////////////////////////////////////////////////////////////////////////////////

// max is a function to provide max integer value between two given values.
func max(x int, y int) int {
	if x >= y {
		return x
	}
	return y
}

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

// initkey is a function to initialize a provided key with the provided varname and subscript array in string form.
func initkey(tptoken uint64, errstr *BufferT, dbkey *KeyT, varname string, subary []string) {
	var maxsublen, sublen, i uint32
	var err error

	subcnt := uint32(len(subary))
	maxsublen = 0
	for i = 0; i < subcnt; i++ {
		// Find maximum length of subscript so know how much to allocate
		sublen = uint32(len(subary[i]))
		if sublen > maxsublen {
			maxsublen = sublen
		}
	}
	dbkey.Alloc(uint32(len(varname)), subcnt, maxsublen)
	dbkey.Varnm.SetValStr(tptoken, errstr, varname)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
	}
	// Load subscripts into KeyT (if any)
	for i = 0; i < subcnt; i++ {
		err = dbkey.Subary.SetValStr(tptoken, errstr, i, subary[i])
		if nil != err {
			panic(fmt.Sprintf("YDB: Unexpected error with SetValStr(): %s", err))
		}
	}
	err = dbkey.Subary.SetElemUsed(tptoken, errstr, subcnt)
	if nil != err {
		panic(fmt.Sprintf("YDB: Unexpected error with SetUsed(): %s", err))
	}
}

// allocMem is a function to allocate memory optionally initializing it in various ways. This can be a future
// point where storage management sanity code can be added.
func allocMem(size C.size_t) unsafe.Pointer {
	// This initial call must be to calloc() to get initialized (cleared) storage. We cannot allocate it and then
	// do another call to initialize it as that means uninitialized memory is traversing the cgo boundary which
	// is what triggers the cgo bug mentioned in the cgo docs (https://golang.org/cmd/cgo/#hdr-Passing_pointers).
	mem := C.calloc(1, size)
	if dbgInitMalloc && (0x00 != dbgInitMallocChar) { // Want to initialize to something other than nulls
		_ = C.memset(mem, dbgInitMallocChar, size)
	}
	return mem
}

// freeMem is a function to return memory allocated with allocMem() or C.calloc().
func freeMem(mem unsafe.Pointer, size C.size_t) {
	if dbgInitFree {
		_ = C.memset(mem, dbgInitFreeChar, size)
	}
	C.free(mem)
}

// errorFormat is a function to replace the FAO codes in YDB error messages with meaningful data. This is normally
// handled by YDB itself but when this Go wrapper raises the same errors, no substitution is done. This routine can
// provide that substitution. It takes set of FAO-code and value pairs performing those substitutions on the error
// message in the order specified. Care must be taken to specify them in the order they appear in the message or
// unexpected substitutions may occur.
func errorFormat(errmsg string, subparms ...string) string {
	if 0 != (uint32(len(subparms)) & 1) {
		panic("YDB: Odd number of substitution parms - invalid FAO code and substitution value pairing")
	}
	for i := 0; i < len(subparms); i = i + 2 {
		errmsg = strings.Replace(errmsg, subparms[i], subparms[i+1], 1)
	}
	return errmsg
}

// formatINVSTRLEN is a function to do the fetching and formatting of the INVSTRLEN error with both of its
// substitutable parms filled in.
func formatINVSTRLEN(tptoken uint64, errstr *BufferT, lenalloc, lenused C.uint) string {
	errmsg, err := MessageT(tptoken, errstr, (int)(YDB_ERR_INVSTRLEN))
	if nil != err {
		panic(fmt.Sprintf("YDB: Error fetching INVSTRLEN: %s", err))
	}
	errmsg = errorFormat(errmsg, "!UL", fmt.Sprintf("%d", lenused), "!UL", fmt.Sprintf("%d", lenalloc)) // Substitute parms
	return errmsg
}

// IsLittleEndian is a function to determine endianness. Exposed in case anyone else wants to know.
func IsLittleEndian() bool {
	var bittest = 0x01

	if 0x01 == *(*byte)(unsafe.Pointer(&bittest)) {
		return true
	}
	return false
}

// Exit is a function to drive YDB's exit handler in case of panic or other non-normal shutdown that bypasses
// atexit() that would normally drive the exit handler.
func Exit() {
	if 1 != atomic.LoadUint32(&ydbInitialized) {
		return // If not (never) initialized, nothing to do
	}
	if dbgSigHandling {
		fmt.Fprintln(os.Stderr, "YDB: Exit(): YDB Engine shutdown started")
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
	exitWait := MaximumNormalExitWait
	if 0 != atomic.LoadUint32(&ydbSigPanicCalled) { // Need "atomic" usage to avoid read/write DATA RACE issues
		exitWait = MaximumPanicExitWait
	}
	select {
	case _ = <-exitdone:
		// We don't really care at this point what the return code is as we're just trying to run things down the
		// best we can as this is the end of using the YottaDB engine in this process.
	case <-time.After(time.Duration(exitWait) * time.Second):
		if dbgSigHandling {
			fmt.Fprintln(os.Stderr, "YDB: Exit(): Wait for ydb_exit() expired")
		}
		if 0 == atomic.LoadUint32(&ydbSigPanicCalled) { // Need "atomic" usage to avoid read/write DATA RACE issues
			// If we panic'd due to a signal, we definitely have run the exit handler as it runs before the panic is
			// driven so we can bypass this message in that case.
			syslogr, err := syslog.New(syslog.LOG_INFO+syslog.LOG_USER, "[YottaDB-Go-Wrapper]")
			if nil != err {
				panic(fmt.Sprintf("syslog.NewLogger() failed unexpectedly with error: %s", err))
			}
			err = syslogr.Info("YDB-W-DBRNDWNBYPASS YottaDB database rundown may have been bypassed due to timeout " +
				"- run MUPIP JOURNAL ROLLBACK BACKWARD / MUPIP JOURNAL RECOVER BACKWARD / MUPIP RUNDOWN")
			if nil != err {
				panic(fmt.Sprintf("syslogr.Info() failed unexpectedly with error: %s", err))
			}

		}
	}
	// Note - the temptation here is to unset ydbInitialized but things work better if we do not do that (we don't have
	// multiple goroutines trying to re-initialize the engine) so we bypass/ re-doing the initialization call later and
	// just go straight to getting the CALLINAFTERXIT error when an actual call is attempted. We now handle CALLINAFTERXIT
	// in the places it matters.
}
