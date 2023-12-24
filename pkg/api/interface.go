package api

import (
	"github.com/voidshard/igor/pkg/structs"
)

// API represents the functions Igor servers should expose.
type API interface {
	// CreateJob creates a new job with the given layers & tasks.
	CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error)

	// CreateTasks creates new tasks on specified layers.
	CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error)

	// Pause sets the indicated objects as paused (layers or tasks)
	Pause(r []*structs.ObjectRef) (int64, error)

	// Unpause sets the indicated objects as unpaused (layers or tasks)
	Unpause(r []*structs.ObjectRef) (int64, error)

	// Skip sets the indicated tasks as skipped (layers or tasks)
	Skip(r []*structs.ObjectRef) (int64, error)

	// Kill attempts to Kill() & set errored the indicated tasks
	Kill(r []*structs.ObjectRef) (int64, error)

	// Retry attempts to Retry() the indicated tasks
	Retry(r []*structs.ObjectRef) (int64, error)

	// Jobs returns jobs matching the given query
	Jobs(q *structs.Query) ([]*structs.Job, error)

	// Layers returns layers matching the given query
	Layers(q *structs.Query) ([]*structs.Layer, error)

	// Tasks returns tasks matching the given query
	Tasks(q *structs.Query) ([]*structs.Task, error)
}

// Server is an interface for an Igor server.
//
// API servers should implement this so that we could, in theory, have a single API server
// serve Igor's API over multiple protocols (eg: http, grpc, etc).
type Server interface {
	// ServeForever starts the server & blocks until Close() is called.
	ServeForever(api API) error

	// Close & shutdown the server.
	Close() error
}
