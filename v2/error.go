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

// ---- Error types for YottaDB errors, YDBGo Init errors, and YDBGo Signal errors

package yottadb

import (
	"errors"
	"fmt"
)

// YDBError type holds YottaDB errors including the formated $ZSTATUS message and the numeric error code.
// It implements [YDBError.Is] so you can do [errors.Is](err, yottadb.BaseError)
// Note: YottaDB error codes used with YDBError are defined in ydberr/errorcodes.go and YDBGo error codes are in ydberr/ydberr.go.
type YDBError struct {
	Message string  // The error string - generally from $ZSTATUS when available
	Code    int     // The error value (e.g. ydberr.INVSTRLEN, etc)
	chain   []error // Lists any errors wrapped by this one
}

// Error is a type method of [YDBError] to return the error message string.
func (err *YDBError) Error() string {
	return err.Message // The error code's name is already included in the message from YottaDB, so don't add the code
}

// Unwrap allows YDBError to wrap other underlying errors.
func (err *YDBError) Unwrap() []error {
	return err.chain
}

// Is lets [errors.Is]() search an error or chain of wrapped errors for a YDBError with a matching ydberr code.
// See [ErrorIs]() for a more practical way to use this capability.
// Only the error code is matched, not the message: this supports matching errors even when YottaDB messages vary.
func (err *YDBError) Is(target error) bool {
	t, ok := target.(*YDBError)
	return ok && err.Code == t.Code
}

// ErrorIs uses [errors.Is]() to search an error or chain of wrapped errors for a YDBError with a matching ydberr code.
// Only the error code is matched, not the message, to support YottaDB error messages that vary.
// For example, to test for YottaDB INVSTRLEN error:
//
//	if yottadb.ErrorIs(err, ydberr.INVSTRLEN) {
//
// is a short equivalent of:
//
//	if errors.Is(err, &YDBError{Code: ydberr.INVSTRLEN}) {
//
// It differs from a simple type test using YDBError.Code in that it searches for a match in the entire chain of wrapped errors
func ErrorIs(err error, code int) bool {
	return errors.Is(err, &YDBError{Code: code})
}

// newYDBError returns error code and message as a [yottadb.Error] error type.
// Any errors supplied in wrapErrors are wrapped in the returned error.
func newYDBError(code int, message string, wrapErrors ...error) error {
	for _, err := range wrapErrors {
		message = message + ": " + err.Error()
	}
	return &YDBError{Code: code, Message: message, chain: wrapErrors}
}

// errorf same as fmt.Errorf except that it returns error type YDBError with specified code; and doesn't handle %w.
func errorf(code int, format string, args ...any) error {
	return newYDBError(code, fmt.Sprintf(format, args...))
}
