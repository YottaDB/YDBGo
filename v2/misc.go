//////////////////////////////////////////////////////////////////
//
// Copyright (c) 2018-2025 YottaDB LLC and/or its subsidiaries.
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
	"runtime"
	"sync"
	"unsafe"
)

// #include "libyottadb.h"
import "C"

var wgexit sync.WaitGroup
var mtxInExit sync.Mutex
var exitRun bool

////////////////////////////////////////////////////////////////////////////////////////////////////
//
// Miscellaneous functions
//
////////////////////////////////////////////////////////////////////////////////////////////////////

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
// that get recorded here, we open a new syslog handle each time to reduce complexity of single threading access
// across goroutines.
func syslogEntry(logMsg string) {
	syslogr, err := syslog.New(syslog.LOG_INFO+syslog.LOG_USER, "[YottaDB-Go-Wrapper]")
	if nil != err {
		panic(fmt.Sprintf("syslog.New() failed unexpectedly with error: %s", err))
	}
	err = syslogr.Info(logMsg)
	if nil != err {
		panic(fmt.Sprintf("syslogr.Info() failed unexpectedly with error: %s", err))
	}
	err = syslogr.Close()
	if nil != err {
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

// IsLittleEndian is a function to determine endianness. Exposed in case anyone else wants to know.
func IsLittleEndian() bool {
	var bittest = 0x01

	return *(*byte)(unsafe.Pointer(&bittest)) != 0
}
