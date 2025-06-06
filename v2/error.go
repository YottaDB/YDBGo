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
)

// Error is a base type for all errors produced by YDBGo, including various YDBGo-specific errors and also [YDBError] for YottaDB errors with code.
type BaseError struct {
	Message string // The error string - generally from $ZSTATUS when available
}

// Error is a type method of [yottadb.Error] to return the error message string.
func (err BaseError) Error() string {
	return err.Message
}

// YDBError type holds YottaDB errors including numeric error code and the formated $ZSTATUS message.
// It embeds base error type [yottadb.BaseError] so that you can test [errors.Is](err, yottadb.BaseError)
type YDBError struct {
	BaseError
	Code int // The error value (e.g. ydberr.INVSTRLEN, etc)
}

// Is lets [errors.Is]() search an error or chain of wrapped errors for a YDBError with a matching ydberr code.
// See [ErrorIs]() for a more practical way to use this capability.
// Only the error code is matched, not the message, to support YottaDB error messages that vary.
func (err YDBError) Is(target error) bool {
	t, ok := target.(YDBError)
	return ok && err.Code == t.Code
}

// ErrorIs uses [errors.Is]() to search an error or chain of wrapped errors for a YDBError with a matching ydberr code.
// Only the error code is matched, not the message, to support YottaDB error messages that vary.
// For example, to test for YottaDB INVSTRLEN error:
//
//	if ErrorIs(err, ydberr.INVSTRLEN) {
//
// is a short equivalent of:
//
//	if errors.Is(err, YDBError{BaseError{}, ydberr.INVSTRLEN}) {
//
// It differs from a simple type test using YDBError.Code in that it searches for a match in the entire chain of wrapped errors
func ErrorIs(err error, code int) bool {
	return errors.Is(err, YDBError{BaseError{}, code})
}

// newYDBError returns error code and message as a [yottadb.Error] error type.
func newYDBError(code int, message string) error {
	return YDBError{BaseError{message}, code}
}

// ---- YDBGo errors (not from YottaDB)

type InitError struct {
	BaseError
}

type ShutdownIncompleteError struct {
	BaseError
}

type ShutdownSignalsError struct {
	BaseError
}
