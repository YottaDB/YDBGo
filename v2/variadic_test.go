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
	"regexp"
	"strconv"
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func TestVpdump(t *testing.T) {
	tconn := SetupTest(t)
	//~ 	n1 := tconn.Node("var", "sub1", "sub2")
	//~ 	n2 := n1.Child("sub3", "sub4")
	//~ 	Lock()
	tconn.vpaddParam(1)
	tconn.vpaddParam64(2)

	arm32 := strconv.IntSize != 64
	arm32int := 0
	if arm32 {
		arm32int = 1
	}
	expected := fmt.Sprintf("   Total of %d elements in this variadic plist\n"+
		"   Elem 0  Value: 1 (0x1)\n"+
		"   Elem 1  Value: 2 (0x2)\n", 2+arm32int)
	if arm32 { // if we're on a 64-bit machine
		expected += "   Elem 2  Value: 0 (0x0)\n"
	}
	var b bytes.Buffer
	tconn.vpdump(&b)
	remove := regexp.MustCompile(`\(0x[0-9A-Fa-f]+\) Value:`)
	output := remove.ReplaceAllString(b.String(), " Value:")
	assert.Equal(t, expected, output)
	tconn.cconn.vplist.n = 0 // Remove test params for any subsequent use of vplist
}
