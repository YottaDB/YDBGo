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

package yottadb_test

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func setupLogger(test_dir string, verbose bool) (*log.Logger, *os.File) {
	test_log_file := filepath.Join(test_dir, "output.log")
	f, err := os.OpenFile(test_log_file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	multi := io.MultiWriter(f)
	if verbose {
		multi = io.MultiWriter(multi, os.Stdout)
	}
	logger := log.New(multi, "YDBGo:", log.Lshortfile)
	return logger, f
}

func createDatabase() (string, bool, *log.Logger, *os.File) {
	// "tst_working_dir" env var is not defined. This means an outside the test system invocation.
	// So create temporary database. We do this to avoid "go test" invocation from polluting any existing
	// database of user.

	// Get a temporary directory to put the database in
	test_dir, err := ioutil.TempDir("", "ydbgo")
	if err != nil {
		log.Fatal(err)
	}

	// Setup the log file, print to stdout if needed
	verbose := false
	for _, b := range os.Args {
		if b == "-test.v=true" || b == "-test.v" {
			verbose = true
		}
	}
	log, f := setupLogger(test_dir, verbose)

	// Setup environment variables
	log.Printf("Test dirctory is %s", test_dir)
	ydb_gbldir := filepath.Join(test_dir, "mumps.gld")
	ydb_datfile := filepath.Join(test_dir, "mumps.dat")
	os.Setenv("ydb_gbldir", ydb_gbldir)
	ydb_dist := os.Getenv("ydb_dist")
	if ydb_dist == "" {
		log.Fatal("ydb_dist not set")
	}
	mumps_exe := filepath.Join(ydb_dist, "mumps")
	mupip_exe := filepath.Join(ydb_dist, "mupip")

	// Create global directory
	cmd := exec.Command(mumps_exe, "-run", "^GDE",
		"change -seg DEFAULT -file="+ydb_datfile)
	output, err := cmd.CombinedOutput()
	log.Printf("%s\n", output)
	if err != nil {
		log.Fatal(err)
	}

	// Create database itself
	cmd = exec.Command(mupip_exe, "create")
	output, err = cmd.CombinedOutput()
	log.Printf("%s\n", output)
	if err != nil {
		log.Fatal(err)
	}
	return test_dir, verbose, log, f
}

func cleanupDatabase(retCode int, verbose bool, log *log.Logger, f *os.File, test_dir string) {
	// Cleanup the temp directory; we leave it if we are in verbose mode
	//  or the test failed
	if 0 == retCode && false == verbose {
		log.Printf("Cleaning up test directory")
		f.Close()
		os.RemoveAll(test_dir)
	}
}

func TestMain(m *testing.M) {
	var verbose bool
	var test_dir string
	var f *os.File
	var log *log.Logger
	// Determine if this is an invocation of "go test" from the YDBTest repo (YottaDB test system).
	// If so, skip temporary database setup as test system sets up databases with random parameters
	// (qdbrundown, replication etc.) and will get more coverage using that database than this on-the-fly database.
	_, is_ydbtest_invocation := os.LookupEnv("tst_working_dir")
	if false == is_ydbtest_invocation {
		test_dir, verbose, log, f = createDatabase()
	}
	// Run the tests
	retCode := m.Run()
	// Cleanup database if needed
	if false == is_ydbtest_invocation {
		cleanupDatabase(retCode, verbose, log, f, test_dir)
	}
	os.Exit(retCode)
}
