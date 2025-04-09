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
BENCH=

perflock?=$(shell command -v perflock >/dev/null && echo perflock)
PERFFLAGS?=-governor 90%

ifeq (,$(perflock))
  $(info Note that installing perflock makes benchmark result much more repeatable and accurate.)
else
  perflock:=$(perflock) $(PERFLAGS)
endif

test:
	go test

bench: benchmark
benchmarks: benchmark
benchmark:
	$(perflock) go test -bench $(tests) $(short) -benchtime $(benchtime) $(BENCH)

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

.PHONY: test bench benchmarks benchmark check
