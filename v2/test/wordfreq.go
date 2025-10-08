//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

// workfreq.go: Count and report word frequencies for http://www.cs.duke.edu/csed/code/code2007/

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"lang.yottadb.com/go/yottadb/v2"
)

// Main routine (wordfreq) - Golang flavor
func main() {
	defer yottadb.Shutdown(yottadb.MustInit())
	conn := yottadb.NewConn()

	// Randomize whether to use local or global vars depending on process id (even=global; odd=local).
	words := conn.Node("words")
	index := conn.Node("index")
	if (os.Getpid() & 1) == 0 {
		words = conn.Node("^words")
		index = conn.Node("^index")
		// Remove any previous results first
		words.Kill()
		index.Kill()
	}

	// Create a reader for stdin
	readin := bufio.NewReader(os.Stdin)
	// Loop through each line in the input file (via stdin) breaking the line into space delimited words
	for {
		line, err := readin.ReadString('\n')
		if nil != err {
			if io.EOF == err {
				break
			}
			panic(fmt.Sprintf("ReadString failure: %s", err))
		}

		// Lower case the string and break line up using Fields method that also eliminates white space
		line = strings.ToLower(line)
		fields := strings.Fields(line)

		// Loop over each word (whitespace delineated) in the input line and increment the counter for it in "fields" array
		for _, word := range fields {
			if word == "" {
				panic("Zero-length word found: this should not happen")
			}
			words.Index(word).Incr(1)
		}
	}

	// Loop through each word and create the index glvn with the frequency count as the first subscript to sort them into
	// least frequent to most frequent order (typical numeric order).
	for node, word := range words.Children() {
		// Fetch the count for this word and SET index(count,word)=""
		count := node.Get()
		index.Index(count, word).Set("")
	}
	//  Loop through [^]indexvar array in reverse to print most common words and their counts first.
	for node, count := range index.ChildrenBackward() {
		for _, word := range node.Children() {
			fmt.Printf("%s\t%s\n", count, word)
		}
	}
}
