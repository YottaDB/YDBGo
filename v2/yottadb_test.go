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

package yottadb

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	v1 "lang.yottadb.com/go/yottadb"
)

// ---- Tests and Examples

// Tests the example given in the package doc at the top of yottadb.go
func ExampleNewConn() {
	defer Shutdown(Init())
	conn := NewConn()
	n := conn.Node("person", "name")
	n.Child("first").Set("Joe")
	n.Child("last").Set("Bloggs")
	for x := range n.Children() {
		fmt.Printf("%s = %s\n", x, Quote(x.Get()))
	}
	n.Kill()
	// Output:
	// person("name","first") = "Joe"
	// person("name","last") = "Bloggs"
}

// ---- Utility functions for tests

var randstrArray = make([]string, 0, 1000000) // Array of random strings for use in testing
var randstrIndex = 0

// initRandstr prepares a list of many random strings.
// Note that tests that use this may not currently run in parallel.
// This would be fixed if randstrIndex were goroutine-local, but Go frowns upon goroutine-local state.
func initRandstr() {
	if len(randstrArray) > 0 {
		return // early return if already filled randstrArray
	}
	rnd := rand.New(rand.NewChaCha8([32]byte{}))
	for range cap(randstrArray) {
		s := fmt.Sprintf("%x", rnd.Uint32())
		randstrArray = append(randstrArray, s)
	}
}

// Randstr fetches a random string from our cache of pre-calculated random strings.
func Randstr() string {
	randstrIndex = (randstrIndex + 1) % len(randstrArray)
	return randstrArray[randstrIndex]
}

// RandstrReset restarts the sequence of pseudo-random strings from the beginning.
func RandstrReset() {
	randstrIndex = 0
}

// multi returns multiple parameters as a single slice of interfaces.
// Useful, for example, in asserting test validity of functions that return both a value and an error.
func multi(v ...any) []any {
	return v
}

// lockExists return whether a lock exists using YottaDB's LKE utility.
func lockExists(lockpath string) bool {
	const debug = false // set true to print output of LKE command
	var outbuff bytes.Buffer

	// Run LKE and scan result
	cmd := exec.Command(os.Getenv("ydb_dist")+"/lke", "show", "-all", "-wait")
	cmd.Stdout = &outbuff
	cmd.Stderr = &outbuff
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
	output := outbuff.Bytes()
	if debug {
		fmt.Printf("finding '%s' in:\n%s\n", lockpath+" Owned", string(output))
	}
	return bytes.Contains(output, []byte(lockpath+" Owned"))
}

// ---- Initialize test system

// This benchmark is purely to provide a long name that causes benchmark outputs to align.
// It calls skip which prevents it from running.
func Benchmark________________________________(b *testing.B) {
	b.Skip()
}

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

func createDatabase(test_dir string, logger *log.Logger) string {
	// Setup environment variables
	logger.Printf("Test directory is %s", test_dir)
	ydb_gbldir := filepath.Join(test_dir, "mumps.gld")
	ydb_datfile := filepath.Join(test_dir, "mumps.dat")
	os.Setenv("ydb_gbldir", ydb_gbldir)
	ydb_dist := os.Getenv("ydb_dist")
	if ydb_dist == "" {
		panic("ydb_dist must be set")
	}
	mumps_exe := filepath.Join(ydb_dist, "mumps")
	mupip_exe := filepath.Join(ydb_dist, "mupip")

	// Create global directory
	cmd := exec.Command(mumps_exe, "-run", "^GDE",
		"change -seg DEFAULT -file="+ydb_datfile)
	output, err := cmd.CombinedOutput()
	logger.Printf("%s\n", output)
	if err != nil {
		logger.Fatal(err)
	}

	// Create database itself
	cmd = exec.Command(mupip_exe, "create")
	output, err = cmd.CombinedOutput()
	logger.Printf("%s\n", output)
	if err != nil {
		logger.Fatal(err)
	}
	return test_dir
}

func cleanupDatabase(logger *log.Logger, test_dir string) {
	logger.Printf("Cleaning up test directory")
	os.RemoveAll(test_dir)
}

// _testMain is factored out of TestMain to let us defer Init() properly
// since os.Exit() must not be run in the same function as defer.
func _testMain(m *testing.M) int {
	// Get a temporary directory to put the database and logfile in
	test_dir, err := os.MkdirTemp("", "ydbgotest-")
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
	logger, logfile := setupLogger(test_dir, verbose)
	defer logfile.Close()

	// Create test database if necessary.
	// Determine if this is an invocation of "go test" from the YDBTest repo (YottaDB test system).
	// If so, skip temporary database setup as test system sets up databases with random parameters
	// (qdbrundown, replication etc.) and will get more coverage using that database than this on-the-fly database.
	_, is_ydbtest_invocation := os.LookupEnv("tst_working_dir")
	if !is_ydbtest_invocation {
		test_dir = createDatabase(test_dir, logger)
	}

	// run init/exit for both v1 and v2 code so we can compare them
	// Run v2 code last so that it sets signals to point to itself
	v1.Init()
	defer v1.Exit()
	defer Shutdown(Init())
	initRandstr()
	ret := m.Run()

	// Print result of BenchmarkDiff, if it was run
	if pathA.Load() != 0 {
		fmt.Printf("BenchmarkDiff: PathA is %.1f%% of the speed of Path B ", 100*float32(pathA.Load())/float32(pathB.Load()))
		fmt.Printf("(PathA=%d PathB=%d)\n", pathA.Load(), pathB.Load())
	}

	// Cleanup the temp directory, but leave it if we are in verbose mode or the test failed
	if !is_ydbtest_invocation && !verbose && ret == 0 {
		cleanupDatabase(logger, test_dir)
	}
	return ret
}

// TestMain is the entry point for tests and benchmarks.
func TestMain(m *testing.M) {
	code := _testMain(m)
	os.Exit(code) // os.Exit is the official way to exit a test suite
}

// SetupTest is called by each test to set up the database prior to the test.
// Returns a database connection that may be used by that test.
func SetupTest(t testing.TB) *Conn {
	tconn := NewConn()
	tconn.KillAllLocals()
	return tconn
}
