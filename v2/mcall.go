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

// Handle calls to M from Go

package yottadb

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"lang.yottadb.com/go/yottadb/v2/ydberr"
)

/* #include "libyottadb.h"

// C routine to get address of ydb_cip_t() since CGo doesn't let you take the address of a variadic parameter-list function.
void *getfunc_ydb_cip_t(void) {
        return (void *)&ydb_cip_t;
}
*/
import "C"

// ---- Define type returned by Import for calling M routines

// MFunctions is returned by [Import] to represent an M-call table with some methods that allow the user to call its M routines.
type MFunctions struct {
	// Almost-private metadata for testing or specialised access to the imported call table. Public for specialised use.
	Table *CallTable
	Conn  *Conn
}

// Call calls an M routine rname with parameters args and returns string, int64 or float64.
// Since the programmer knows the return-type in advance from the M-call table, the return type may be safely
// forced to that type with an unchecked type assertion. For example:
//
//	x = m.Call("add", 1, 2).(int64)
//
// This version panics on errors. See [MFunctions.CallErr]() for a version that returns errors.
func (m *MFunctions) Call(rname string, args ...any) any {
	routine := m.getRoutine(rname)
	ret, err := m.Conn.callM(routine, args)
	if err != nil {
		panic(err)
	}
	return ret
}

// CallErr calls an M routine rname with parameters args and returns string, int64 or float64, and any error.
// This version returns any errors. See [MFunctions.Call]() for a version that panics.
func (m *MFunctions) CallErr(rname string, args ...any) (any, error) {
	routine := m.getRoutine(rname)
	ret, err := m.Conn.callM(routine, args)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// Wrap returns a function that calls an M routine rname that returns any.
// This can speed up calling M routines by avoiding the name lookup each invocation.
// This version creates functions that panic: compare [MFunctions.WrapErr] that returns errors instead.
func (m *MFunctions) Wrap(rname string) func(args ...any) any {
	routine := m.getRoutine(rname)
	return func(args ...any) any {
		ret, err := m.Conn.callM(routine, args)
		if err != nil {
			panic(err)
		}
		return ret
	}
}

// WrapRetInt produces a Go convenience function that wraps an M routine that returns int.
// This can avoid messy type assertion and speed up calling M routines by avoiding the name lookup each invocation.
// Rather than int64 it returns int as the default type in Go so that regular arithmetic can be done on the result without casting.
// This version creates functions that panic: compare [MFunctions.WrapErr] that returns errors instead.
func (m *MFunctions) WrapRetInt(rname string) func(args ...any) int {
	routine := m.getRoutine(rname)
	return func(args ...any) int {
		ret, err := m.Conn.callM(routine, args)
		if err != nil {
			panic(err)
		}
		return ret.(int)
	}
}

// WrapRetString produces a Go convenience function that wraps an M routine that returns string.
// This can avoid messy type assertion and speed up calling M routines by avoiding the name lookup each invocation.
// This version creates functions that panic: compare [MFunctions.WrapErr] that returns errors instead.
func (m *MFunctions) WrapRetString(rname string) func(args ...any) string {
	routine := m.getRoutine(rname)
	return func(args ...any) string {
		ret, err := m.Conn.callM(routine, args)
		if err != nil {
			panic(err)
		}
		return ret.(string)
	}
}

// WrapRetFloat produces a Go convenience function that wraps an M routine that returns float64.
// This can avoid messy type assertion and speed up calling M routines by avoiding the name lookup each invocation.
// This version creates functions that panic: compare [MFunctions.WrapErr] that returns errors instead.
func (m *MFunctions) WrapRetFloat(rname string) func(args ...any) float64 {
	routine := m.getRoutine(rname)
	return func(args ...any) float64 {
		ret, err := m.Conn.callM(routine, args)
		if err != nil {
			panic(err)
		}
		return ret.(float64)
	}
}

// WrapErr returns a function that calls an M routine rname that returns any.
// This can speed up calling M routines by avoiding the name lookup each invocation.
// This version creates functions that return errors: compare [MFunctions.Wrap] that panics instead.
func (m *MFunctions) WrapErr(rname string) func(args ...any) (any, error) {
	routine := m.getRoutine(rname)
	return func(args ...any) (any, error) {
		return m.Conn.callM(routine, args)
	}
}

func (m *MFunctions) getRoutine(name string) *RoutineData {
	routine, ok := m.Table.Routines[name]
	if !ok {
		panic(errorf(ydberr.MCallNotFound, "M routine '%s' not found in M-call table", name))
	}
	return routine
}

// ---- Internal types

type typeInfo struct {
	kind    reflect.Kind // for matching
	ydbType string
}

var typeMapper map[string]typeInfo = map[string]typeInfo{
	"":        {reflect.Invalid, "void"},
	"string":  {reflect.String, "ydb_buffer_t*"},
	"int":     {reflect.Int, "ydb_long_t*"}, // YottaDB (u)long type switches between 32- and 64-bit by platform, just like Go int
	"uint":    {reflect.Uint, "ydb_ulong_t*"},
	"int32":   {reflect.Int32, "ydb_int_t*"}, // YottaDB (u)int type is 32-bit only like Go int32
	"uint32":  {reflect.Uint32, "ydb_uint_t*"},
	"int64":   {reflect.Int64, "ydb_int64_t*"},
	"uint64":  {reflect.Uint64, "ydb_uint64_t*"},
	"float32": {reflect.Float32, "ydb_float_t*"},
	"float64": {reflect.Float64, "ydb_double_t*"},
}

var returnTypes map[string]struct{} = map[string]struct{}{
	"":        {},
	"string":  {},
	"int":     {},
	"int64":   {},
	"float64": {},
}

// CallTable stores internal metadata used for calling M and a table of Go functions by name loaded from the M call-in table.
type CallTable struct {
	handle   C.uintptr_t // handle used to access the call table
	Filename string
	YDBTable string // Table after pre-processing the M-call table into YDB format
	// List of M routine metadata structs: one for each routine imported.
	Routines map[string]*RoutineData
}

// typeSpec stores the specification for the return value and each parameter of an M routine call.
type typeSpec struct {
	pointer bool         // true if the parameter contains * and is thus passed by reference
	alloc   int          // preallocation size for this parameter
	typ     string       // type string
	kind    reflect.Kind // store kind of type
}

// routineCInfo is YottaDB's C descriptors for a routine.
// Note: these cannot be merged into RoutineData because a pointer to them must be passed to AddCleanup() as a single pointer.
type routineCInfo struct {
	nameDesc *C.ci_name_descriptor // descriptor for M routine with fastpath for calls after the first
}

// RoutineData stores info used to call an M routine.
type RoutineData struct {
	Name          string     // Go name for the M routine
	Entrypoint    string     // the point to be called in M code
	Types         []typeSpec // stores type specification for the routine's return value and each parameter
	preallocation int        // sum of all types.preallocation -- avoids having to calculate this in callM()
	// metadata:
	Table *CallTable   // call table used to find this routine name
	cinfo routineCInfo // stores YottaDB's C descriptors for the routine
}

var callingM sync.Mutex // Mutex for access to ydb_ci_tab_switch() so table doesn't switch again before fetching ci_name_descriptor routine info

// parseType parses Go type of form [*]type[preallocation] (e.g. *string[100]) into a typeSpec struct.
// Return typeSpec struct
func parseType(typeStr string) (*typeSpec, error) {
	typeStr = regexp.MustCompile(`\s*`).ReplaceAllString(typeStr, "") // remove spaces, e.g., between <type> and '*'
	pattern := regexp.MustCompile(`(\*?)([\w_]*)(\*?)(.*)`)
	m := pattern.FindStringSubmatch(typeStr)
	if m == nil {
		return nil, errorf(ydberr.MCallTypeUnhandled, "does not match a YottaDB call-in table type specification")
	}
	asterisk, typ, badAsterisk, allocStr := m[1], m[2], m[3], m[4]
	// Check that user didn't accidentally place * after the retType like he would in C
	if badAsterisk == "*" {
		return nil, errorf(ydberr.MCallBadAsterisk, "should not have asterisk at the end")
	}
	pointer := asterisk == "*"
	pattern = regexp.MustCompile(`\[(\d+)\]`)
	m = pattern.FindStringSubmatch(allocStr)
	preallocation := -1
	if m != nil {
		if n, ok := strconv.Atoi(m[1]); ok == nil {
			preallocation = n
		}
	}
	if preallocation == -1 && typ == "string" && pointer {
		return nil, errorf(ydberr.MCallPreallocRequired, "[preallocation] must be supplied after *string type")
	}
	if preallocation != -1 && typ != "string" {
		return nil, errorf(ydberr.MCallPreallocInvalid, "[preallocation] should not be supplied for number type")
	}
	if preallocation != -1 && !pointer {
		return nil, errorf(ydberr.MCallPreallocInvalid, "[preallocation] should not be specified for non-pointer type because it is not an output from M")
	}
	if preallocation == -1 {
		preallocation = 0 // default to zero if not supplied
	}
	if _, ok := typeMapper[typ]; !ok {
		return nil, errorf(ydberr.MCallTypeUnknown, "unknown type")
	}
	kind := typeMapper[typ].kind
	return &typeSpec{pointer, preallocation, typ, kind}, nil
}

// ParsePrototype parses one line of the ydb call-in file format.
//   - line is the text in the current line of the call-in file
//
// Return *RoutineData containing routine name and Go function that wraps the M routine, and some metadata.
// Note that the returned RoutineData.table is not filled in (nil) as this function does not know the table.
// Blank or pure comment lines return nil.
func parsePrototype(line string) (*RoutineData, error) {
	// Remove comments and ignore blank lines
	line = regexp.MustCompile(`[/]/.*`).ReplaceAllString(line, "")
	if regexp.MustCompile(`^\s*$`).MatchString(line) {
		return nil, nil // return nil if blank line
	}

	// Example prototype line: test_Run: *string[100] %Run^test(*string, int64, int64)
	// Go playground to test with a regex akin to this is at: https://go.dev/play/p/8-e53CpcagC
	// Note: this allows * before or after retType so parseType() can produce a specific error later for incorrectly placing it after retType
	pattern := regexp.MustCompile(`\s*([^:\s]+)\s*:\s*(\*?\s*?[\w_]*\s*?\*?\s*?\[?\s*?[\d]*\s*?\]?)(\s*)([^(\s]+)\s*\(([^)]*)\)`)
	m := pattern.FindStringSubmatch(line)
	if m == nil {
		return nil, errorf(ydberr.MCallInvalidPrototype, "line does not match prototype format 'Go_name: [ret_type] M_entrypoint([*]type, [*]type, ...)'")
	}
	name, retType, space, entrypoint, params := m[1], m[2], m[3], m[4], m[5]

	// Fix up case where the user specified no return type and part of retType captured part of entrypoint
	if space == "" {
		entrypoint = retType + entrypoint // anything captured in retType is part of entrypoint
		retType = ""
	}

	// Check for a valid M entrypoint. It contain only %^@+ and alphanumeric characters.
	// Here I only check valid characters. YottaDB will produce an error in case of incorrect positioning of those characters
	if !regexp.MustCompile(`[%^@+0-9a-zA-Z]+`).MatchString(entrypoint) {
		return nil, errorf(ydberr.MCallEntrypointInvalid, "entrypoint (%s) to call M must contain only alphanumeric and %%^@+ characters", entrypoint)
	}

	// Create list of types for return value and then each parameter
	_retType := retType
	if _retType != "" {
		// treat retType as if it were a pointer type since it has to receive back a value
		_retType = "*" + _retType
	}
	typ, err := parseType(_retType)
	if err != nil {
		err.(*Error).Message = fmt.Sprintf("return type (%s) %s", retType, err)
		return nil, err
	}
	if _, ok := returnTypes[typ.typ]; !ok {
		return nil, errorf(ydberr.MCallTypeUnknown, "invalid return type %s (must be string, int, int64, or float64)", retType)
	}
	if strings.Contains(retType, "*") {
		return nil, errorf(ydberr.MCallTypeMismatch, "return type (%s) must not be a pointer type", retType)
	}
	types := []typeSpec{*typ}
	// Now iterate each parameter
	for i, typeStr := range regexp.MustCompile(`[^,\)]+`).FindAllString(params, -1) {
		typ, err = parseType(typeStr)
		if err != nil {
			err.(*Error).Message = fmt.Sprintf("parameter %d (%s) %s", i+1, retType, err)
			return nil, err
		}
		if typ.typ == "" {
			return nil, errorf(ydberr.MCallTypeMissing, "parameter %d is empty but should contain a type", i+1)
		}
		types = append(types, *typ)
	}

	// Iterate types again to calculate total preallocation.
	// Avoids having to calculate this each time callM is called.
	preallocation := 0
	for _, typ := range types {
		preallocation += typ.alloc // add user's explicit string preallocation
	}

	// Make sure we aren't trying to send too many parameters.
	// The -1 is because the return value doesn't count against YDB_MAX_PARMS.
	if len(types)-1 > int(C.YDB_MAX_PARMS) {
		return nil, errorf(ydberr.MCallTooManyParameters, "number of parameters %d exceeds YottaDB maximum of %d", len(types)-1, int(C.YDB_MAX_PARMS))
	}

	// Create routine struct
	nameDesc := (*C.ci_name_descriptor)(calloc(C.sizeof_ci_name_descriptor)) // must use our calloc, not malloc: see calloc doc
	nameDesc.rtn_name.address = C.CString(name)                              // Allocates new memory (released by AddCleanup above
	nameDesc.rtn_name.length = C.ulong(len(name))
	routine := RoutineData{name, entrypoint, types, preallocation, nil, routineCInfo{nameDesc}}
	// Queue the cleanup function to free it
	runtime.AddCleanup(&routine, func(cinfo *routineCInfo) {
		// free string data in namedesc first
		C.free(unsafe.Pointer(cinfo.nameDesc.rtn_name.address))
		C.free(unsafe.Pointer(cinfo.nameDesc))
	}, &routine.cinfo)

	return &routine, nil
}

// Import loads a call-in table for use by this connection only.
// The M routines listed in the call-in 'table' (specified below) are each wrapped in a Go function which may be subsequently
// called using the returned [MFunctions.Call](name) or referenced as a Go function using [MFunctions.Wrap](name).
//
// If 'table' string contains ":" it is considered to be the call-in table specification itself; otherwise it is treated as the filename of a call-in file to be opened and read.
//
// # M-call table format specification
//
// An M-call table specifies M routines which may be called by Go.
// It may be a string or a file (typically a file with extension .mcalls) and is case-sensitive.
// The format of an M-call table is a sequence of text lines where each line contains an M routine prototype specifications as follows:
//
//	Go_name: [ret_type] M_entrypoint(type, type, ...)
//
// Elements of that line are defined as follows:
//   - Go_name may be any go string.
//   - M_entrypoint is any valid [M entry reference].
//   - ret_type may be omitted if an M return value is not supplied. Otherwise it must be *string, *int, *int64, *float64 or omitted (for void return)
//   - any spaces adjacent to commas, asterisk and square brackets (,*[]) are ignored.
//
// Zero or more parameter type specifications are allowed and must be a Go type specifier: string, int, uint, int32, uint32, int64, uint64, float32, float64,
// or a pointer version of the same in Go type format (e.g. *int).
//   - If a pointer type is selected then the parameter is passed by reference so that the M routine can modify the parameter.
//   - Any *string types and string return values must be followed by a preallocation value in square brackets (e.g. *string[100]).
//
// This allows Go to preallocate enough space for the returned string. If necessary, YottaDB will truncate returned strings so they fit.
//
// Comments begin with // and continue to the end of the line. Blank lines or pure-comment lines are ignored.
//
// [M entry reference]: https://docs.yottadb.com/ProgrammersGuide/langfeat.html#entry-references
func (conn *Conn) Import(table string) (*MFunctions, error) {
	var tbl CallTable
	cconn := conn.cconn

	// Open and read M-call table so we can preprocess it into a YottaDB-format call-in table
	prototypes := []byte(table)
	if !strings.Contains(table, ":") {
		tbl.Filename = table
		var err error
		prototypes, err = os.ReadFile(table)
		if err != nil {
			return nil, errorf(ydberr.ImportRead, "could not read call-in table file '%s': %s", table, err)
		}
	}

	// Process `prototypes` ci-table ourselves to get routine names and types.
	// Do this after ydb has processed the file to let ydb catch any errors in the table.
	var routines = make(map[string]*RoutineData)
	for i, line := range bytes.Split(prototypes, []byte{'\n'}) {
		routine, err := parsePrototype(string(line))
		if err != nil {
			err := err.(*Error)
			return nil, newError(err.Code, fmt.Sprintf("%s line %d", err, i+1), newError(ydberr.ImportParse, "")) // wrap ImportError under err
		}
		if routine == nil {
			continue
		}
		routine.Table = &tbl
		routines[routine.Name] = routine
	}
	tbl.Routines = routines

	// Create new prototype table in YottaDB-format for output to call-in file
	var b strings.Builder
	for _, routine := range routines {
		var ydbTypes []string
		for i, typ := range routine.Types {
			ydbType := typeMapper[typ.typ].ydbType
			if typ.typ == "string" && internalDB.YDBRelease < 1.36 {
				// Make ydb <1.36 always use 'ydb_string_', since it doesn't have 'ydb_buffer_t'.
				// It doesn't work as well because output length cannot be longer than input value,
				// but at least it will maintain backward compatibility for any apps that previously used YDB <1.36
				ydbType = "ydb_string_t*"
			}
			// Add * for pointer types unless the ydb type already inherently has a * (e.g. strings)
			if typ.pointer && !strings.HasSuffix(ydbType, "*") {
				ydbType = ydbType + "*"
			}
			// Add IO: or I: for all parameters (not for retval, though).
			if i > 0 {
				if typ.pointer {
					ydbType = "IO:" + ydbType
				} else {
					ydbType = "I:" + ydbType
				}
			}
			ydbTypes = append(ydbTypes, ydbType)
		}
		fmt.Fprintf(&b, "%s: %s %s(%s)\n", routine.Name, ydbTypes[0], routine.Entrypoint, strings.Join(ydbTypes[1:], ", "))
	}
	tbl.YDBTable = strings.Trim(b.String(), "\n")

	// Now create a ydb version of the call-in table without any preallocation specs (which YDB doesn't currently support)
	f, err := os.CreateTemp("", "YDBGo_callins_*.ci")
	if err != nil {
		return nil, errorf(ydberr.ImportTemp, "could not open temporary call-in table file '%s': %s", f.Name(), err)
	}
	if debugMode >= 1 { // In debug modes retain YDB-format temporary file for later inspection
		log.Printf("Temporary call-in table file is: %s\n", f.Name())
	} else {
		defer os.Remove(f.Name())
	}
	_, err = f.WriteString(tbl.YDBTable)
	f.Close()
	if err != nil {
		return nil, errorf(ydberr.ImportTemp, "could not write temporary call-in table file '%s': %s", f.Name(), err)
	}

	// Tell YottaDB to process the call-in table
	cstr := C.CString(f.Name())
	defer C.free(unsafe.Pointer(cstr))
	handle := (*C.uintptr_t)(C.malloc(C.sizeof_uintptr_t))
	defer C.free(unsafe.Pointer(handle))
	conn.prepAPI()
	status := C.ydb_ci_tab_open_t(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, cstr, handle)
	tbl.handle = *handle
	if status != YDB_OK {
		err := conn.lastError(status).(*Error)
		return nil, newError(err.Code, fmt.Sprintf("%s while processing call-in table:\n%s\n", err, tbl.YDBTable), newError(ydberr.ImportOpen, ""))
	}

	mfunctions := MFunctions{&tbl, conn}
	return &mfunctions, nil
}

const paramSize = max(C.sizeof_uintptr_t, C.sizeof_ydb_string_t, C.sizeof_ydb_buffer_t)

// paramAlloc allocates and returns parameter block if it hasn't already been allocated.
func (conn *Conn) paramAlloc() unsafe.Pointer {
	cconn := conn.cconn
	// Lazily allocate param block only if needed by callM
	if cconn.paramBlock != nil {
		return cconn.paramBlock
	}
	// Allocate enough space for every parameter to be the maximum size (a string buffer). +1 for return value.
	// Spaces for the actual strings is allocated separately (if necessary) in callM.
	size := paramSize * (C.YDB_MAX_PARMS + 1)
	cconn.paramBlock = calloc(C.size_t(size)) // must use our calloc, not malloc: see calloc doc
	// Note this gets freed by conn cleanup
	return cconn.paramBlock
}

// callM calls M routines.
//   - args are the parameters to pass to the M routine. In the current version, they are converted to
//
// strings using fmt.Sprintf("%v") but this may change in future for improved efficiency.
// Return value is nil if the routine is not defined to return anything.
func (conn *Conn) callM(routine *RoutineData, args []any) (any, error) {
	if routine == nil {
		panic(errorf(ydberr.MCallNil, "routine data passed to Conn.CallM() must not be nil"))
	}
	cconn := conn.cconn

	if len(args) != len(routine.Types)-1 {
		panic(errorf(ydberr.MCallWrongNumberOfParameters, "%d parameters supplied whereas the M-call table specifies %d", len(args), len(routine.Types)-1))
	}
	printEntry("CallTable.CallM()")
	// If we haven't already fetched the call description from YDB, do that now.
	if routine.cinfo.nameDesc.handle == nil {
		// Lock out other instances of ydb_ci_tab_switch() so table doesn't switch again before fetching ci_name_descriptor routine info.
		// Release lock once we've called routine for the first time and populated ci_name_desriptor.
		callingM.Lock()
		defer callingM.Unlock()

		// Allocate a C storage place for ydb to store handle.
		oldhandle := (*C.uintptr_t)(C.malloc(C.sizeof_uintptr_t))
		defer C.free(unsafe.Pointer(oldhandle))
		conn.prepAPI()
		status := C.ydb_ci_tab_switch_t(C.uint64_t(conn.tptoken.Load()), &cconn.errstr, routine.Table.handle, oldhandle)
		if status != YDB_OK {
			return "", conn.lastError(status)
		}
		// On return, restore table handle since we changed it. Ignore errors.
		// This is commented out because it triggers a YottaDB bug (still present in r2.01) which complains
		// if environment variable ydb_ci/CTMCI is not set even if it has already called the relevant M routine
		// and thus already has table data for it.
		//defer C.ydb_ci_tab_switch_t(cconn.tptoken, &cconn.errstr, *oldhandle, oldhandle)
	}

	// Add each parameter to the vararg list required to call ydb_cip_t()
	conn.vpStart() // restart parameter list
	conn.vpAddParam64(conn.tptoken.Load())
	conn.vpAddParam(uintptr(unsafe.Pointer(&cconn.errstr)))
	conn.vpAddParam(uintptr(unsafe.Pointer(routine.cinfo.nameDesc)))

	// Calculate how much to preallocate = routine.preallocation + size of non-preallocated strings passed this call
	preallocation := routine.preallocation
	for _, arg := range args {
		switch val := arg.(type) {
		case string:
			preallocation += len(val)
		}
	}

	// Allocate enough space for all parameters and any string preallocations.
	// Do this with a single malloc for speed, or don't do it at all if conn.value is large enough to accommodate us.
	paramBlock := conn.paramAlloc() // allocated in conn for in subsequent calls
	var prealloc unsafe.Pointer
	// Use statically-available connection space if prealloction fits within its maximum; otherwise malloc
	if routine.preallocation < YDB_MAX_STR {
		conn.ensureValueSize(routine.preallocation)
		prealloc = unsafe.Pointer(cconn.value.buf_addr)
	} else {
		prealloc = C.malloc(C.size_t(routine.preallocation)) // must use our calloc, not malloc: see calloc doc
		defer C.free(prealloc)
	}

	param := paramBlock // start param at beginning of paramBlock
	allotStr := func(alloc, length int) {
		length = min(alloc, length)
		if internalDB.YDBRelease < 1.36 {
			// Make ydb <1.36 always use 'ydb_string_', since it doesn't have 'ydb_buffer_t'.
			// It doesn't work as well because output length cannot be longer than input value,
			// but at least it will maintain backward compatibility for any apps that previously used YDB <1.36
			str := (*C.ydb_string_t)(param)
			str.length = C.ulong(length)
			str.address = (*C.char)(prealloc)
		} else {
			buf := (*C.ydb_buffer_t)(param)
			buf.len_used = C.uint(length)
			buf.len_alloc = C.uint(alloc)
			buf.buf_addr = (*C.char)(prealloc)
		}
		prealloc = unsafe.Add(prealloc, alloc)
	}

	// If there is a return value, store it first in the paramBlock space
	if routine.Types[0].typ != "" {
		typ := routine.Types[0]
		if typ.kind == reflect.String {
			allotStr(typ.alloc, typ.alloc)
		}
		conn.vpAddParam(uintptr(unsafe.Pointer(param)))
		param = unsafe.Add(param, paramSize)
	}
	// Now store each parameter into the allocated paramBlock space and load it into our variadic parameter list
	for i, typ := range routine.Types[1:] {
		pointer := false
		// typeAssert() assistant function: check arg type against supplied type.
		// This retains speed as it only uses reflect.TypeOf in the case of errors.
		typeAssert := func(val any, kind reflect.Kind) {
			if typ.kind == kind && typ.pointer == pointer {
				return
			}
			asterisk := ""
			if typ.pointer {
				asterisk = "*"
			}
			panic(errorf(ydberr.MCallTypeMismatch, "parameter %d is %s but %s%s is specified in the M-call table", i+1, reflect.TypeOf(val), asterisk, typ.typ))
		}
		switch val := args[i].(type) {
		case string:
			typeAssert(val, reflect.String)
			C.memcpy(prealloc, unsafe.Pointer(unsafe.StringData(val)), C.size_t(len(val)))
			allotStr(len(val), len(val))
		case *string:
			pointer = true
			typeAssert(val, reflect.String)
			C.memcpy(prealloc, unsafe.Pointer(unsafe.StringData(*val)), C.size_t(min(typ.alloc, len(*val))))
			allotStr(typ.alloc, len(*val))
		case int:
			typeAssert(val, reflect.Int)
			*(*C.ydb_long_t)(param) = C.ydb_long_t(val)
		case *int:
			pointer = true
			typeAssert(val, reflect.Int)
			*(*C.ydb_long_t)(param) = C.ydb_long_t(*val)
		case uint:
			typeAssert(val, reflect.Uint)
			*(*C.ydb_ulong_t)(param) = C.ydb_ulong_t(val)
		case *uint:
			pointer = true
			typeAssert(val, reflect.Uint)
			*(*C.ydb_ulong_t)(param) = C.ydb_ulong_t(*val)
		case int32:
			typeAssert(val, reflect.Int32)
			*(*C.ydb_int_t)(param) = C.ydb_int_t(val)
		case *int32:
			pointer = true
			typeAssert(val, reflect.Int32)
			*(*C.ydb_int_t)(param) = C.ydb_int_t(*val)
		case uint32:
			typeAssert(val, reflect.Uint32)
			*(*C.ydb_uint_t)(param) = C.ydb_uint_t(val)
		case *uint32:
			pointer = true
			typeAssert(val, reflect.Uint32)
			*(*C.ydb_uint_t)(param) = C.ydb_uint_t(*val)
		case int64:
			typeAssert(val, reflect.Int64)
			*(*C.ydb_int64_t)(param) = C.ydb_int64_t(val)
		case *int64:
			pointer = true
			typeAssert(val, reflect.Int64)
			*(*C.ydb_int64_t)(param) = C.ydb_int64_t(*val)
		case uint64:
			typeAssert(val, reflect.Uint64)
			*(*C.ydb_uint64_t)(param) = C.ydb_uint64_t(val)
		case *uint64:
			pointer = true
			typeAssert(val, reflect.Uint64)
			*(*C.ydb_uint64_t)(param) = C.ydb_uint64_t(*val)
		case float32:
			typeAssert(val, reflect.Float32)
			*(*C.ydb_float_t)(param) = C.ydb_float_t(val)
		case *float32:
			pointer = true
			typeAssert(val, reflect.Float32)
			*(*C.ydb_float_t)(param) = C.ydb_float_t(*val)
		case float64:
			typeAssert(val, reflect.Float64)
			*(*C.ydb_double_t)(param) = C.ydb_double_t(val)
		case *float64:
			pointer = true
			typeAssert(val, reflect.Float64)
			*(*C.ydb_double_t)(param) = C.ydb_double_t(*val)
		default:
			panic(errorf(ydberr.MCallTypeUnhandled, "unhandled type (%s) in parameter %d", reflect.TypeOf(val), i+1))
		}
		conn.vpAddParam(uintptr(unsafe.Pointer(param)))
		param = unsafe.Add(param, paramSize)
	}

	// vplist now contains the parameter list we want to send to ydb_cip_t(). But CGo doesn't permit us
	// to call or even get a function pointer to ydb_cip_t(). So call it via getfunc_ydb_cip_t().
	status := conn.vpCall(C.getfunc_ydb_cip_t()) // call ydb_cip_t()
	if status != YDB_OK {
		return nil, conn.lastError(status)
	}

	// Go through the parameters again to locate the pointer parameters and copy their values back into Go space
	param = paramBlock
	fetchStr := func() string {
		if internalDB.YDBRelease < 1.36 {
			// Make ydb <1.36 always use 'ydb_string_', since it doesn't have 'ydb_buffer_t'.
			// It doesn't work as well because output length cannot be longer than input value,
			// but at least it will maintain backward compatibility for any apps that previously used YDB <1.36
			str := (*C.ydb_string_t)(param)
			return C.GoStringN(str.address, C.int(str.length))
		} else {
			buf := (*C.ydb_buffer_t)(param)
			return C.GoStringN(buf.buf_addr, C.int(buf.len_used))
		}
	}
	// If there is a return value, fetch it first from the paramBlock space
	var retval any
	if routine.Types[0].typ != "" {
		typ := routine.Types[0]
		switch typ.kind {
		case reflect.String:
			retval = fetchStr()
		case reflect.Int:
			ptr := (*C.ydb_long_t)(param)
			retval = int(*ptr)
		case reflect.Int64:
			ptr := (*C.ydb_int64_t)(param)
			retval = int64(*ptr)
		case reflect.Float64:
			ptr := (*C.ydb_double_t)(param)
			retval = float64(*ptr)
		default:
			panic(errorf(ydberr.MCallTypeUnhandled, "unhandled type (%s) in return of return value; report bug in YDBGo", typ.typ))
		}
		param = unsafe.Add(param, paramSize)
	}
	// Now fill each pointer parameter from the paramBlock space
	for i, typ := range routine.Types[1:] {
		if typ.pointer {
			switch val := args[i].(type) {
			case *string:
				*val = fetchStr()
			case *int:
				ptr := (*C.ydb_long_t)(param)
				*val = int(*ptr)
			case *uint:
				ptr := (*C.ydb_ulong_t)(param)
				*val = uint(*ptr)
			case *int32:
				ptr := (*C.ydb_int_t)(param)
				*val = int32(*ptr)
			case *uint32:
				ptr := (*C.ydb_uint_t)(param)
				*val = uint32(*ptr)
			case *int64:
				ptr := (*C.ydb_int64_t)(param)
				*val = int64(*ptr)
			case *uint64:
				ptr := (*C.ydb_uint64_t)(param)
				*val = uint64(*ptr)
			case *float32:
				ptr := (*C.ydb_float_t)(param)
				*val = float32(*ptr)
			case *float64:
				ptr := (*C.ydb_double_t)(param)
				*val = float64(*ptr)
			case string, int, uint, int32, uint32, int64, uint64, float32, float64:
			default:
				panic(errorf(ydberr.MCallTypeUnhandled, "unhandled type (%s) in parameter %d; report bug in YDBGo", reflect.TypeOf(val), i+1))
			}
		}
		param = unsafe.Add(param, paramSize)
	}
	runtime.KeepAlive(conn) // ensure conn sticks around until we've finished copying data from it's C paramblock
	return retval, nil
}

// MustImport is the same as [Conn.Import] but panics on errors.
func (conn *Conn) MustImport(table string) *MFunctions {
	mfunctions, err := conn.Import(table)
	if err != nil {
		panic(err)
	}
	return mfunctions
}
