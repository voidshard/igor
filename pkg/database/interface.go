package database

import (
	"github.com/voidshard/igor/pkg/database/changes"
	"github.com/voidshard/igor/pkg/structs"
)

// Database is an interface for a database in which Igor can store it's metadata.
type Database interface {
	// InsertJob inserts a job, it's layers & tasks into the database.
	InsertJob(j *structs.Job, ls []*structs.Layer, ts []*structs.Task) error

	// InsertTasks inserts a set of tasks into the database.
	InsertTasks(in []*structs.Task) error

	// SetLayersPaused sets the paused state of the given layers
	SetLayersPaused(at int64, newTag string, ids []*structs.ObjectRef) (int64, error)

	// SetTasksPaused sets the paused state of the given tasks
	SetTasksPaused(at int64, newTag string, ids []*structs.ObjectRef) (int64, error)

	// SetJobsStatus sets the status of the given jobs
	SetJobsStatus(status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error)

	// SetLayersStatus sets the status of the given layers
	SetLayersStatus(status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error)

	// SetTasksStatus sets the status of the given tasks
	SetTasksStatus(status structs.Status, newTag string, ids []*structs.ObjectRef, msg ...string) (int64, error)

	// SetTaskQueueID sets the queue id & status of the given task
	SetTaskQueueID(taskID, etag, newEtag, queueTaskID string, newState structs.Status) error

	// Jobs returns jobs matching the given query
	Jobs(q *structs.Query) ([]*structs.Job, error)

	// Layers returns layers matching the given query
	Layers(q *structs.Query) ([]*structs.Layer, error)

	// Tasks returns tasks matching the given query
	Tasks(q *structs.Query) ([]*structs.Task, error)

	// Changes returns a stream of changes to the database (see pkg/database/changes)
	Changes() (changes.Stream, error)

	// Close & shutdown the database.
	Close() error
}

// QueueDB is a (much reduced) set of functions that a Queue is allowed to call on our Database.
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
