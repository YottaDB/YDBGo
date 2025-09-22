# Makefile to test YDBGo

##################################################################
#
# Copyright (c) 2025 YottaDB LLC and#or its subsidiaries.
# All rights reserved.
#
#	This source code contains the intellectual property
#	of its copyright holder(s), and is made available
#	under a license.  If you do not know the terms of
#	the license, please stop and do not read further.
#
##################################################################

tests=.
benchtime?=1s
short=-short
flags=
BENCH=

perflock?=$(shell command -v perflock >/dev/null && echo perflock)
PERFFLAGS?=-governor 90%

ifeq (,$(perflock))
  $(info Note that installing perflock makes benchmark result much more repeatable and accurate.)
else
  perflock:=$(perflock) $(PERFLAGS)
endif

COVERDIR ?= coverage

test:
	go test -run $(tests) -timeout 5s $(flags)

bench: benchmark
benchmarks: benchmark
benchmark:
	$(perflock) go test -bench $(tests) $(short) -timeout 5s -benchtime $(benchtime) $(BENCH) $(flags)

diff: benchtime=5s
diff: short=
diff: tests=BenchmarkDiff
diff: benchmark

# Check whether test results increase with each test by shown 5 of each test.
# Perflock fixes this problem but perflock doesn't work on all systems.
# So check your system with `make check`
check: BENCH=-count=6
check: benchtime=0.1s
check: benchmark

cover: coverage
coverage:
	mkdir -p $(COVERDIR)
	rm -f $(COVERDIR)/*
	@# The following commented-out lines are if we need to build a non-test executable to perform part of the test.
	@#go build -covermode=count -o test/fatalsignal test/fatalsignal.go
	@#@# Run standalone program to capture coverage of fatal signal effect
	@##GOCOVERDIR=$(COVERDIR) test/fatalsignal || true
	go test -c -covermode=count -o test/test
	@# Capture coverage for several different exit tests
	@# Run multiple times for the different tests: real=SIGINT; fake=SignalExitCallback() to capture coverage data for real; shutdownpanic test
	for i in real fake goroutine shutdownpanic shutdownpanic2; do \
		test/test -test.timeout=5s -test.gocoverdir=$(COVERDIR) -test.v -test.run TestFatal -fataltest=$$i 2>&1 \
		| grep "shutdownSignalGoroutines: Channel closings complete"; \
	done
	@# Test that YDBGo creates proper errors when it cannot Init()
	test/test -test.timeout=5s -test.gocoverdir=$(COVERDIR) -test.v -test.run TestNoInit -noinit
	@# Run the rest of the tests
	test/test -test.timeout=5s -test.gocoverdir=$(COVERDIR)
	go tool covdata textfmt -i=$(COVERDIR) -o $(COVERDIR)/coverage.cov
	go tool covdata percent -i=$(COVERDIR)
	go tool cover -html="$(COVERDIR)/coverage.cov" -o $(COVERDIR)/coverage.html

clean:
	rm -f $(COVERDIR)/* test/*.o test/fatalsignal test/test

.PHONY: test bench benchmarks benchmark check cover coverage
