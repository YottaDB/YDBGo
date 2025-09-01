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
	"encoding/binary"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"

	assert "github.com/stretchr/testify/require"
)

// ---- Tests

func TestVpdump(t *testing.T) {
	conn := SetupTest(t)

	// Detect vpDump of a nil vplist
	assert.PanicsWithError(t, "could not dump nil vararg list", func() { conn.vpDump(os.Stdout) })

	// Detect vpAddParam before vpStart
	assert.PanicsWithError(t, "programmer forgot to call vpStart() before vpAddParam()", func() { conn.vpAddParam(1) })

	// Test isLittleEndian
	vals := []byte{0xe8, 0x03, 0xd0, 0x07, 0x12, 0x23, 0x34, 0x45}
	assert.Equal(t, binary.LittleEndian.Uint64(vals) == binary.NativeEndian.Uint64(vals), isLittleEndian())

	// Detect too many variadic parameters
	conn.vpStart()
	for i := range maxVariadicParams {
		conn.vpAddParam(uintptr(i))
	}
	assert.Panics(t, func() { conn.vpAddParam(1) })

	conn.vpStart()
	conn.vpAddParam(1)
	conn.vpAddParam64(2)

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
	conn.vpDump(&b)
	remove := regexp.MustCompile(`\(0x[0-9A-Fa-f]+\) Value:`)
	output := remove.ReplaceAllString(b.String(), " Value:")
	assert.Equal(t, expected, output)
	conn.cconn.vplist.n = 0 // Remove test params for any subsequent use of vplist
}
