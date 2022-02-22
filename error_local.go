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
	ERR_YDBSTRUCTNOTALLOCD = 151027722
	ERR_YDBINVLNPAIRLIST   = 151027730
	ERR_YDBDBRNDWNBYPASS   = 151027738
	ERR_YDBSIGACKTIMEOUT   = 151027746
)
const YDB_ERR_STRUCTNOTALLOCD = -int(ERR_YDBSTRUCTNOTALLOCD)
const YDB_ERR_INVLNPAIRLIST = -int(ERR_YDBINVLNPAIRLIST)
const YDB_ERR_DBRNDWNBYPASS = -int(ERR_YDBDBRNDWNBYPASS)
const YDB_ERR_SIGACKTIMEOUT = -int(ERR_YDBSIGACKTIMEOUT)

// ydbGoErrors is an array of error entries containing the Go-only set of errors
var ydbGoErrors = []ydbGoErrEntry{
	{ERR_YDBSTRUCTNOTALLOCD, "STRUCTNOTALLOCD", "Structure not previously called with Alloc() method", "E"},
	{ERR_YDBINVLNPAIRLIST, "INVLNPAIRLIST",
		"Invalid lockname/subscript pair list (uneven number of lockname/subscript parameters)", "E"},
	{ERR_YDBDBRNDWNBYPASS, "DBRNDWNBYPASS",
		"YDB-W-DBRNDWNBYPASS YottaDB database rundown may have been bypassed due to timeout - run MUPIP JOURNAL ROLLBACK" +
			" BACKWARD / MUPIP JOURNAL RECOVER BACKWARD / MUPIP RUNDOWN", "E"},
	{ERR_YDBSIGACKTIMEOUT, "SIGACKTIMEOUT",
		"Signal completion acknowledgement timeout: !AD", "E"},
}
