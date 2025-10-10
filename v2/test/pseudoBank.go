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

// pseudoBank creates 1000 fake bank accounts and transfers an amount two accounts as fast as possible for 2 minutes.
// Two random accounts are newly selected for each transfer.
// Ten concurrent jobs are started, each doing this same thing.
// Each job represents a different 'user' named with the single letter 'A' to 'J'.
// Each transfer appends to list of account transactions in ^ZHIST and appends to a log of account transactions in ^ZTRNLOG.
// After running, pseudoBank you can run `ydb -r pseudoBankDisp` to count log entries and show how many transactions occurred.

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"lang.yottadb.com/go/yottadb/v2"
)

const tranAmt = 1          // amount to transfer each time
const startCid = 100001    // start of account number range
const accountNeeded = 1000 // number of accounts to create and use
const concurrent = 10      // number of concurrent jobs to start

var ydbEpoch = time.Date(1840, time.December, 31, 0, 0, 0, 0, time.UTC)

type Nodes struct {
	conn     *yottadb.Conn // database connection object
	accounts *yottadb.Node // account strings
	history  *yottadb.Node // list of transaction history
	log      *yottadb.Node // transaction log
}

func main() {
	var Verbose = flag.Bool("verbose", false, "Display extra progress information")
	var Timeout = flag.Float64("timeout", 120.0, "Test time in seconds (default 120 seconds)")
	flag.Parse()
	if !*Verbose {
		log.SetOutput(io.Discard)
	}
	timeout := time.Duration(*Timeout * float64(time.Second))

	defer yottadb.Shutdown(yottadb.MustInit())
	log.Println("init data")
	initData(accountNeeded)

	var waitGroup sync.WaitGroup
	waitGroup.Add(concurrent)
	var stop atomic.Bool // set true to stop all jobs -- use atomic to ensure volatility
	for guid := range concurrent {
		user := string('A' + guid)
		// Start job
		go func() {
			defer yottadb.ShutdownOnPanic()
			defer waitGroup.Done()
			// Create new connection and node objects for this goroutine
			conn := yottadb.NewConn()
			data := &Nodes{conn, conn.Node("^ZACN"), conn.Node("^ZHIST"), conn.Node("^ZTRNLOG")}

			for t := 0; !stop.Load(); t++ {
				ref := guid + (t * concurrent) // make a big reference number
				from := startCid + rand.Intn(accountNeeded)
				to := startCid + rand.Intn(accountNeeded-1)
				if from == to {
					to += 1
				}
				data.postTransfer(ref, from, to, tranAmt, user)
			}
		}()
	}
	// Wait for timeout
	log.Printf("Waiting %ds\n", timeout/time.Second)
	time.Sleep(timeout)
	stop.Store(true)
	log.Printf("Waiting for goroutines to stop\n")
	waitGroup.Wait()
	log.Println("Done")
}

func initData(accountNeeded int) {
	conn := yottadb.NewConn()
	accounts := conn.Node("^ZACN")
	history := conn.Node("^ZHIST")
	for i := range accountNeeded {
		// Each 'account' string contains "transaction_counter | balance"
		cid := startCid + i
		accounts.Index(cid).Set("1|10000000")
		history.Index(cid).Set("Account Create||10000000")
	}
}

// Horolog produces the same output at Node("$HOROLOG").Get() but faster because it doesn't have to call YDB
func Horolog() string {
	now := time.Now()
	_, offset := now.Zone()
	duration := time.Now().UTC().Sub(ydbEpoch) + time.Duration(offset)*time.Second
	days := duration.Hours() / 24.0
	intDays := int(days)
	return fmt.Sprintf("%d,%d", intDays, int((days-float64(intDays))*24*60*60))
}

func (data *Nodes) postTransfer(ref, from, to, amount int, user string) {
	data.conn.TransactionFast([]string{}, func() {
		// update from account
		fromSequence, fromBalance := data.getAccount(from)
		fromSequence, fromBalance = fromSequence+1, fromBalance-amount
		data.putAccount(from, fromSequence, fromBalance)
		// update to account
		toSequence, toBalance := data.getAccount(to)
		toSequence, toBalance = toSequence+1, toBalance+amount
		data.putAccount(to, toSequence, toBalance)
		// update transaction history and log
		data.history.Index(from, fromSequence).Set(fmt.Sprintf("Transfer to %d|-%d|%d|%d", to, amount, fromBalance, ref))
		data.history.Index(to, toSequence).Set(fmt.Sprintf("Transfer to %d|%d|%d|%d", to, amount, toBalance, ref))
		datetime := "12345,67890"
		data.log.Index(ref, 1).Set(fmt.Sprintf("Transfer to %d|%s|%d|-%d|%d|%s", to, datetime, from, amount, fromBalance, user))
		data.log.Index(ref, 1).Set(fmt.Sprintf("Transfer from %d|%s|%d|%d|%d|%s", from, datetime, to, amount, toBalance, user))
	})
}

func (data *Nodes) getAccount(account int) (int, int) {
	fields := strings.Split(data.accounts.Index(account).Get(), "|")
	if len(fields) < 2 {
		panic("Account string corrupted in database")
	}
	sequence, err := strconv.Atoi(fields[0])
	if err != nil {
		panic(err)
	}
	balance, err := strconv.Atoi(fields[1])
	if err != nil {
		panic(err)
	}
	return sequence, balance
}

func (data *Nodes) putAccount(account, sequence, balance int) {
	value := fmt.Sprintf("%d|%d", sequence, balance)
	data.accounts.Index(account).Set(value)
}
