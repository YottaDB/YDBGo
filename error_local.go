//////////////////////////////////////////////////////////////////
//								//
// Copyright (c) 2018-2022 YottaDB LLC and/or its subsidiaries.	//
// All rights reserved.						//
//								//
//	This source code contains the intellectual property	//
//	of its copyright holder(s), and is made available	//
//	under a license.  If you do not know the terms of	//
//	the license, please stop and do not read further.	//
//								//
//////////////////////////////////////////////////////////////////

package yottadb

// Global constants containing the error ids
const (
	ERR_STRUCTUNALLOCD  = 151027722
	ERR_INVLKNMPAIRLIST = 151027730
	ERR_DBRNDWNBYPASS   = 151027738
	ERR_SIGACKTIMEOUT   = 151027746
	ERR_SIGGORTNTIMEOUT = 151027752
)
const YDB_ERR_STRUCTUNALLOCD = -int(ERR_STRUCTUNALLOCD)
const YDB_ERR_INVLKNMPAIRLIST = -int(ERR_INVLKNMPAIRLIST)
const YDB_ERR_DBRNDWNBYPASS = -int(ERR_DBRNDWNBYPASS)
const YDB_ERR_SIGACKTIMEOUT = -int(ERR_SIGACKTIMEOUT)
const YDB_ERR_SIGGORTNTIMEOUT = -int(ERR_SIGGORTNTIMEOUT)

// ydbGoErrors is an array of error entries containing the Go-only set of errors
var ydbGoErrors = []ydbGoErrEntry{
	{ERR_STRUCTUNALLOCD, "STRUCTNUNALLOCD", "Structure not previously called with Alloc() method", "E"},
	{ERR_INVLKNMPAIRLIST, "INVLKNMPAIRLIST",
		"Invalid lockname/subscript pair list (uneven number of lockname/subscript parameters)", "E"},
	{ERR_DBRNDWNBYPASS, "DBRNDWNBYPASS",
		"YDB-W-DBRNDWNBYPASS YottaDB database rundown may have been bypassed due to timeout - run MUPIP JOURNAL ROLLBACK" +
			" BACKWARD / MUPIP JOURNAL RECOVER BACKWARD / MUPIP RUNDOWN", "E"},
	{ERR_SIGACKTIMEOUT, "SIGACKTIMEOUT",
		"Signal completion acknowledgement timeout: !AD", "E"},
	{ERR_SIGGORTNTIMEOUT, "ERR_SIGGORTNTIMEOUT", "Shutdown of signal goroutines timed out", "W"},
}
