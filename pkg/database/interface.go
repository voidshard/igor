package database

import (
	"github.com/voidshard/igor/pkg/structs"
)

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

	SetLayersPaused(at int64, newTag string, ids []*structs.ObjectRef) (int64, error)
	SetTasksPaused(at int64, newTag string, ids []*structs.ObjectRef) (int64, error)

	SetJobsStatus(status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error)
	SetLayersStatus(status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error)
	SetTasksStatus(status structs.Status, newTag string, ids []*structs.ObjectRef, msg ...string) (int64, error)
	SetTaskQueueID(taskID, etag, newEtag, queueTaskID string, newState structs.Status) (int64, error)

	Jobs(q *structs.Query) ([]*structs.Job, error)
	Layers(q *structs.Query) ([]*structs.Layer, error)
	Tasks(q *structs.Query) ([]*structs.Task, error)

	Changes() (ChangeStream, error)

	Close() error
}

type QueueDB interface {
	// Tasks returned by ID
	Tasks(ids []string) ([]*structs.Task, error)

	// SetTaskState sets the state of the given task
	//
	// Only: RUNNING, ERRORED, COMPLETED, SKIPPED are accepted
	//
	// The tasks new etag is returned, if it's set successfully
	SetTaskState(task *structs.Task, st structs.Status, msg string) (string, error)
}

func NewQueueDB(db Database) QueueDB {
	return newDefaultQDB(db)
}
