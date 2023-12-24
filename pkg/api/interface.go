package api

import (
	"github.com/voidshard/igor/pkg/structs"
)

// API represents the functions Igor servers should expose.
type API interface {
	// Implemented in igor/internal/core.Service

	CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error)
	CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error)

	Pause(r []*structs.ToggleRequest) (int64, error)
	Unpause(r []*structs.ToggleRequest) (int64, error)
	Skip(r []*structs.ToggleRequest) (int64, error)
	Kill(r []*structs.ToggleRequest) (int64, error)

	Jobs(q *structs.Query) ([]*structs.Job, error)
	Layers(q *structs.Query) ([]*structs.Layer, error)
	Tasks(q *structs.Query) ([]*structs.Task, error)
	Runs(q *structs.Query) ([]*structs.Run, error)
}

type Server interface {
	ServeForever(api API) error
	Close() error
}
