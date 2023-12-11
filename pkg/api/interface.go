package api

import (
	"github.com/voidshard/igor/pkg/structs"
)

// API represents the functions Igor servers should expose.
type API interface {
	CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error)
	CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error)

	Pause(r []*structs.ObjectRef) (int64, error)
	Unpause(r []*structs.ObjectRef) (int64, error)
	Skip(r []*structs.ObjectRef) (int64, error)
	Kill(r []*structs.ObjectRef) (int64, error)
	Retry(r []*structs.ObjectRef) (int64, error)

	Jobs(q *structs.Query) ([]*structs.Job, error)
	Layers(q *structs.Query) ([]*structs.Layer, error)
	Tasks(q *structs.Query) ([]*structs.Task, error)
}

type Server interface {
	ServeForever(api API) error
	Close() error
}
