package errors

import (
	"fmt"
)

var (
	ErrNoLayers      = fmt.Errorf("no layers specified")
	ErrNoTasks       = fmt.Errorf("no tasks specified")
	ErrNoTaskType    = fmt.Errorf("no task type specified")
	ErrParentNoFound = fmt.Errorf("parent not found")
	ErrETagMismatch  = fmt.Errorf("etag mismatch")
	ErrMaxExceeded   = fmt.Errorf("max length exceeded")
	ErrInvalidState  = fmt.Errorf("invalid state")
	ErrInvalidArg    = fmt.Errorf("invalid arg")
	ErrNotSupported  = fmt.Errorf("not supported")
)
