package queue

import (
	"github.com/voidshard/igor/pkg/structs"
)

// Service is a cut down interface to our core.Service, that includes slimmed down and
// somewhat simplified versions of the methods the Queue is allowed to call.
type Service interface {
	// Implemented in igor/internal/core.QueueDB

	// Tasks returned by ID
	Tasks(ids []string) ([]*structs.Task, error)

	// Runs returned by ID
	Runs(ids []string) ([]*structs.Run, error)

	// SetTasksState sets the state of the given tasks to either
	// ERRORED or SKIPPED.
	//
	// If it isn't set to one of these two states, it will be set to
	// COMPLETED by default.
	SetTasksState(tasks []*structs.Task, st structs.Status) error

	// SetRunsState sets the state of the given runs to either
	// ERRORED or SKIPPED.
	//
	// If it isn't set to one of these two states, it will be set to
	// COMPLETED by default.
	SetRunsState(runs []*structs.Run, st structs.Status, msg string) error
}
