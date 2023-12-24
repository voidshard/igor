package database

import (
	"github.com/voidshard/igor/pkg/structs"
)

type IDTag struct {
	ID   string
	ETag string
}

type Change struct {
	Kind structs.Kind
	Old  interface{}
	New  interface{}
}

type ChangeStream interface {
	Next() (*Change, error)
	Close() error
}

type Database interface {
	InsertJob(j *structs.Job, ls []*structs.Layer, ts []*structs.Task) error
	InsertTasks(in []*structs.Task) error
	InsertRuns(in []*structs.Run) error

	SetLayersPaused(at int64, newTag string, ids []*IDTag) (int64, error)
	SetTasksPaused(at int64, newTag string, ids []*IDTag) (int64, error)

	SetJobsStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error)
	SetLayersStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error)
	SetTasksStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error)
	SetRunsStatus(status structs.Status, newTag string, ids []*IDTag, msg ...string) (int64, error)

	Jobs(q *structs.Query) ([]*structs.Job, error)
	Layers(q *structs.Query) ([]*structs.Layer, error)
	Tasks(q *structs.Query) ([]*structs.Task, error)
	Runs(q *structs.Query) ([]*structs.Run, error)

	Changes() (ChangeStream, error)

	Close() error
}
