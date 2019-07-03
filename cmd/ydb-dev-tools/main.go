//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2019 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

/* ydb-dev-tools provides a variety of functions for Go programmers who use YottaDB.
 * This main file gets compiled to a binary that is installed to the users $GOPATH/bin
 * directory when go get is run.
 *
 * Currently, this tool only generates glue code for TP callbacks, but future enhancements
 * could include functionality to generate wrapping code for M call-ins, or other misc.
 * tasks
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"text/template"
)

// GlueCodeParameters encapsulates parameters to pass to the GlueCode template
type GlueCodeParameters struct {
	PackageName  string
	FunctionName string
}

// Subcommand provides additional actions on this ydb-dev-tools command
type Subcommand struct {
	name   string
	help   string
	flags  *flag.FlagSet
	handle func()
}

// As we add additional commnads, this will have to be broken up into submodules much like
//  the "go" command does (https://github.com/golang/go/blob/master/src/cmd/go)
var routineName, packageName string
var programFlags map[string]Subcommand

// Usage prints the intended usage of this utility, along with the help for each subcommand
func Usage() {
	fmt.Printf("Usage: %s [subcommand] [subcommand options]\n\n", os.Args[0])
	for k, v := range programFlags {
		fmt.Printf("Subcommand: %s -- %s\n", k, v.help)
		v.flags.Usage()
	}
}

// handleGenerate handles the logic for the generate command
func handleGenerate() {
	myFlags := programFlags["generate"].flags
	myFlags.Parse(os.Args[2:])
	if (routineName == "") || (packageName == "") {
		fmt.Printf("Missing argument; please give -func and -pkg\n")
		Usage()
		os.Exit(2)
	}
	params := GlueCodeParameters{packageName, routineName}
	tmpl, err := template.New("output").Parse(fileTemplate)
	if err != nil {
		panic(err)
	}
	f, err := os.Create(routineName + "_cgo.go")
	if err != nil {
		panic(err)
	}
	err = tmpl.Execute(f, params)
	if err != nil {
		panic(err)
	}
}

// Parses arguments and calls appropriate subcommands; currently, the only subcommand is generate.
func main() {

	programFlags = make(map[string]Subcommand)

	myFlags := flag.NewFlagSet("generate", flag.ExitOnError)
	programFlags["generate"] = Subcommand{
		"generate",
		"generates C glue code for TP functions",
		myFlags,
		handleGenerate,
	}
	myFlags.StringVar(&routineName, "func", "", "the function name we are generating a template for")
	myFlags.StringVar(&packageName, "pkg", "", "the package name we are generating a template for")

	if 2 >= len(os.Args) {
		Usage()
		os.Exit(2)
	}

	action := os.Args[1]

	val, ok := programFlags[action]
	if ok {
		val.handle()
	} else {
		Usage()
		os.Exit(2)
	}
}
