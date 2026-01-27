#!/bin/sh

#################################################################
#								#
# Copyright (c) 2020-2026 YottaDB LLC and/or its subsidiaries.	#
# All rights reserved.						#
#								#
#	This source code contains the intellectual property	#
#	of its copyright holder(s), and is made available	#
#	under a license.  If you do not know the terms of	#
#	the license, please stop and do not read further.	#
#								#
#################################################################

# Determines whether a file should need a copyright by its name
# Returns 0 if it needs a copyright and 1 otherwise.
# Returns 2 if an error occurs.
set -eu

if ! [ $# = 1 ]; then
	echo "usage: $0 <filename>"
	exit 2
fi

file="$1"

# Don't require deleted files to have a copyright
if ! [ -e "$file" ]; then
	exit 1
fi

skipextensions="mod ci sum md"	# List of extensions that cannot have copyrights.
	# .mod -> e.g. go.mod is an auto-generated file and
	# .ci  -> e.g. calltab.ci stores the call-in table but currently YDB repo
	#	does not have a provision for comment characters in that file).

if echo "$skipextensions" | grep -q -w "$(echo "$file" | awk -F . '{print $NF}')"; then
	exit 1
fi

# Below is a list of specific files that do not have a copyright so ignore them
skiplist="COPYING LICENSE-overview.txt v2/COPYING v2/LICENSE-overview.txt"
skiplist="$skiplist README.md error_codes.go error_codes.h v2/ydbconst.go v2/ydbconst.h v2/ydberr/errorcodes.go v2/ydberr/errorcodes.h"
skiplist="$skiplist v2/test/wordfreq_input.txt v2/test/wordfreq_output.txt"
if echo "$skiplist" | grep -q -w "$file"; then
	exit 1
fi
