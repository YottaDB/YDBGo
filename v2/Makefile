# Makefile to test YDBGo

##################################################################
#
# Copyright (c) 2025-2026 YottaDB LLC and#or its subsidiaries.
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
verbosity=/dev/null
BENCH=

perflock?=$(shell command -v perflock >/dev/null && echo perflock)
PERFFLAGS?=-governor 90%

ifeq (,$(perflock))
  $(info Note that installing perflock makes benchmark result much more repeatable and accurate.)
else
  perflock:=$(perflock) $(PERFLAGS)
endif

COVERDIR ?= coverage

# $(call run_test,<test args>,<quoted markers>,<exit status>) runs test/test and fails unless its output contains every marker.
# It also fails unless test/test exits with the given status, which defaults to 0.
# Output is captured in a variable rather than piped to grep, so Go always finishes writing its coverage counters.
# Set verbosity=/dev/stderr to see the full test output.
define run_test
	@echo "test/test $(1)"; \
	out=$$(test/test -test.timeout=5s -test.gocoverdir=$(COVERDIR) -test.v $(1) 2>&1); status=$$?; \
	printf '%s\n' "$$out" >$(verbosity); \
	[ $$status -eq $(or $(3),0) ] || { echo "test/test $(1) failed: exit status $$status, expected $(or $(3),0)"; exit 1; }; \
	for m in $(2); do \
		printf '%s\n' "$$out" | grep -qF -- "$$m" || { echo "test/test $(1) failed: missing '$$m'"; exit 1; }; \
	done
endef

# $(call fatal_test,<fataltest name>,<quoted extra markers>[,<exit status>]) runs a TestFatal case.
# Every case must report that its signal goroutines shut down, plus any markers proving that case's behavior ran.
fatal_test = $(call run_test,-test.run TestFatal -fataltest=$(1),"shutdownSignalGoroutines: Channel closings complete" $(2),$(3))

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
	go test -c -covermode=count -o test/test
	@# Capture coverage for several different exit tests. Each shuts down the database so needs its own process.
	@# The real case sends a real SIGINT but saves no coverage, since YottaDB exits first.
	@# The fake case calls signalExitCallback() directly, to capture coverage of the same exit path.
	@# The real and goroutine cases end in the fatal-signal panic, so they exit with status 2.
	$(call fatal_test,real,,2)
	$(call fatal_test,fake)
	$(call fatal_test,goroutine,,2)
	$(call fatal_test,shutdownpanic)
	$(call fatal_test,shutdownpanic2)
	$(call fatal_test,shutdownpanicerror,"Was re-paniced as:")
	$(call fatal_test,callinafterxit,"No panic occurred1")
	$(call fatal_test,sigshutdowntimeout,"shutdownSignalGoroutines: Timeout! Some signal goroutines did not shutdown")
	@# Test coverage path where runtime.Goexit is called inside a transaction
	$(call run_test,-test.run TestGoexitInTransaction -goexit,"runtime.Goexit() called inside a transaction")
	@# Test that YDBGo creates proper errors when it cannot Init()
	test/test -test.timeout=5s -test.gocoverdir=$(COVERDIR) -test.run TestNoInit -noinit
	@# Run the rest of the tests
	test/test -test.timeout=20s -test.gocoverdir=$(COVERDIR)
	go tool covdata textfmt -i=$(COVERDIR) -o $(COVERDIR)/coverage.cov
	go tool -C $(COVERDIR) cover -html=coverage.cov -o coverage.html
	@# tee below required for the pipeline to see coverage.out
	go tool covdata percent -i=$(COVERDIR) | tee $(COVERDIR)/coverage.out
	grep -oP '(?<=coverage: )[\d.]+(?=%)' $(COVERDIR)/coverage.out >$(COVERDIR)/coverage.percent
	@# awk $0+0 below converts it to a float
	badge=`awk 'END {n=$$0+0; color=n<75? "red": (n<90)? "yellow": (n<95)? "a3c51c": "4c1"; print n "%25-" color}' $(COVERDIR)/coverage.percent` && \
		wget --no-verbose https://img.shields.io/badge/coverage-$$badge -O $(COVERDIR)/coverage.svg

clean:
	rm -f $(COVERDIR)/* test/*.o test/test

.PHONY: test bench benchmarks benchmark check cover coverage
