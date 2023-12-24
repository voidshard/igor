package errors

import (
	"fmt"
)

var (
	// ErrNoLayers is returned when no layers are specified for a job
	ErrNoLayers = fmt.Errorf("no layers specified")

	// ErrNoTasks is returned when no tasks are specified in a task creation request
	ErrNoTasks = fmt.Errorf("no tasks specified")

	// ErrNotTaskType is returned when a task type is not specified (field is always required)
	ErrNoTaskType = fmt.Errorf("no task type specified")

	// ErrParentNotFound is returned when a parent object is not found
	// Eg. task creation request that specifies a parent layer that doesn't exist
	ErrParentNotFound = fmt.Errorf("parent not found")

	// ErrETagMismatch is returned when an ETag doesn't match a specified value.
	// Usually if Igor is dealing with a set of objects we will return the number
	// of objects that were updated, implying the ETag for other object(s) didn't match
	// or were not found.
	ErrETagMismatch = fmt.Errorf("etag mismatch")

	// ErrMaxExceeded is returned when a max length is exceeded.
	// Ie. a name or args field that is too long.
	ErrMaxExceeded = fmt.Errorf("max length exceeded")

	// ErrInvalidState is returned when an object is in an invalid state, or an invalid status is specified.
	ErrInvalidState = fmt.Errorf("invalid state")

	// ErrInvalidArg is returned when a given value is invalid (a bad ID or non existent Kind, etc)
	ErrInvalidArg = fmt.Errorf("invalid arg")

	// ErrNotSupported is returned when a given operation is not supported.
	ErrNotSupported = fmt.Errorf("not supported")
)
