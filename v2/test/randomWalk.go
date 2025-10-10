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

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"lang.yottadb.com/go/yottadb/v2"
)

// Run for a specified duration calling random features of the YottaDB Go Easy API.
//
// Creates DRIVER_THREADS goroutines, where each rusn runProc() in a loop until the timeout expires.
// Each runProc() has an equal chance of performing one of the ACTION enums listed below
// The goal is to run until timeout with no panics.

const (
	ACT_SET        = iota // Setting a global/local
	ACT_SET2              // Ditto. Doubles the likelihood of this action
	ACT_GET               // Getting a global/local
	ACT_HAS               // Test global/local for whether it has data or subtree
	ACT_KILL              // Kill or clear a global/local and its tree
	ACT_KILLEXCEPT        // Kill all locals except the specified list of strings
	ACT_NEXTSUB           // Walking through a global/local
	ACT_NEXTNODE          // Walking through a node tree
	ACT_INCR              // Incrementing a global/local
	ACT_LOCK              // Settings locks on a global/local
	ACT_TRANSACT          // Create a transaction nesting a function that performs this action list to a depth of MAX_DEPTH
	ACT_SPAWN             // Create THREADS_TO_MAKE new goroutines to run a function that performs this action list to a depth of MAX_DEPTH
	ACT_COUNT             // Number of actions defined above
)

// These are set below in init() and should be treated as constants for the rest of the test
var THREADS_TO_MAKE int
var DRIVER_THREADS int
var MAX_THREADS int
var MAX_DEPTH int

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
	THREADS_TO_MAKE = rand.Intn(17) + 4 //[4,20]
	DRIVER_THREADS = rand.Intn(9) + 2   //[2,10]
	MAX_THREADS = 1000
	MAX_DEPTH = rand.Intn(19) + 2 //[2,20]
}

var routineCount atomic.Int32

// Generate a node with a randomly global or local varname and randomly 1-5 subscripts to be used in runProc()
func genNode(conn *yottadb.Conn) *yottadb.Node {
	// randomly select one of  4 varnames
	names := []string{"^MyGlobal1", "^MyGlobal2", "MyLocal1xx", "MyLocal2xx"}
	varname := names[rand.Intn(len(names))]
	var subs []any
	for i := range rand.Intn(5) {
		subs = append(subs, fmt.Sprintf("sub%d", i))
	}
	return conn.Node(varname, subs...)
}

// runProc randomly attempts various YDBGo functions
func runProc(conn *yottadb.Conn, depth int) {
	node := genNode(conn)
	action := rand.Intn(ACT_COUNT)
	//fmt.Printf("%d %s\n", action, node)
	switch action {
	case ACT_SET:
		fallthrough
	case ACT_SET2:
		node.Set("MySecretValue")
	case ACT_GET:
		node.Get()
	case ACT_HAS:
		// randomly select one of 6 functions to run
		funcs := []func() bool{node.HasValue, node.HasValueOnly, node.HasTree, node.HasTreeOnly, node.HasBoth, node.HasNone}
		f := funcs[rand.Intn(len(funcs))]
		f()
	case ACT_KILL:
		if rand.Intn(2) == 0 {
			node.Kill()
		} else {
			node.Clear()
		}
	case ACT_KILLEXCEPT:
		conn.KillLocalsExcept()
	case ACT_NEXTSUB:
		if rand.Intn(2) == 0 {
			node.Next()
		} else {
			node.Prev()
		}
	case ACT_NEXTNODE:
		if rand.Intn(2) == 0 {
			node.TreeNext()
		} else {
			node.TreePrev()
		}
	case ACT_INCR:
		node.Incr(rand.Float64()*10 - 5)
	case ACT_LOCK:
		// Avoid %YDB-E-TPLOCK from trying to use conn.Lock() inside a transaction to release locks created outside the transaction
		if depth == 0 {
			conn.Lock(0, node) // release all nodes and then try only once to lock this node
		}
		node.Lock(0)  // lock this node (again)
		node.Unlock() // unlock this node (once)
		// Avoid %YDB-E-TPLOCK from trying to use conn.Lock() inside a transaction to release locks created outside the transaction
		if depth == 0 {
			conn.Lock(0) // unlock all nodes
		}
	case ACT_TRANSACT:
		if depth > MAX_DEPTH {
			break
		}
		// Half the time do runProc() inside a transaction; otherwise create THREADS_TO_MAKE new goroutines.
		// But suppress the second option if we've already got at least MAX_THREADS goroutines
		if rand.Intn(2) == 0 || int(routineCount.Load()) >= MAX_THREADS {
			conn.TransactionFast([]string{}, func() {
				for range rand.Intn(20) {
					runProc(conn, depth+1)
				}
			})
		}
	case ACT_SPAWN:
		if depth > MAX_DEPTH {
			break
		}
		var wg sync.WaitGroup
		for range THREADS_TO_MAKE {
			wg.Add(1)
			go func() {
				defer yottadb.ShutdownOnPanic()
				defer wg.Done()
				routineCount.Add(1)
				defer routineCount.Add(-1)
				conn := conn.CloneConn()
				runProc(conn, depth+1)
			}()
		}
		wg.Wait()
	}
}

// goid extracts the goroutine ID from the stack trace.
// This may be used for debug printing if there are problems.
func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := bytes.Fields(buf[:n])[1]
	id, err := strconv.Atoi(string(idField))
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func main() {
	var Verbose = flag.Bool("verbose", false, "Display extra progress information")
	var Timeout = flag.Float64("timeout", -1.0, "Test time in seconds (default randomly between 15 and 120 seconds)")
	flag.Parse()
	if !*Verbose {
		log.SetOutput(io.Discard)
	}
	if *Timeout < 0.0 {
		*Timeout = float64(rand.Intn(106) + 15) //[15,120] test timeout in seconds for the driver threads to stop
	}
	timeout := time.Duration(*Timeout * float64(time.Second))

	defer yottadb.Shutdown(yottadb.MustInit())

	var waitGroup sync.WaitGroup
	waitGroup.Add(DRIVER_THREADS)
	var stop atomic.Bool // set true to stop all jobs -- use atomic to ensure volatility
	for range DRIVER_THREADS {
		go func() {
			defer yottadb.ShutdownOnPanic()
			routineCount.Add(1)
			defer routineCount.Add(-1)
			// Create new connection and node objects for this goroutine
			conn := yottadb.NewConn()

			for !stop.Load() { // loop until test timeout then break
				depth := 0
				runProc(conn, depth)
			}
			waitGroup.Done()
		}()
	}

	// Wait for timeout
	log.Printf("Waiting %ds", timeout/time.Second)
	time.Sleep(timeout)
	stop.Store(true)
	log.Printf("Waiting for goroutines to stop")
	waitGroup.Wait()
	log.Println("Done")
}
