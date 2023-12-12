package database

import (
	"fmt"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

// defaultQDB is a simple wrapper around a Database so the Queue package can
// only call functions we allow.
type defaultQDB struct {
	db Database
}

// NewQueueDB returns a new QueueDB instance
func NewQueueDB(db Database) QueueDB {
	return &defaultQDB{db: db}
}

// SetTasksState sets the state of the given tasks to the given status.
// This func is restricted to setting a final state (errored, completed, skipped).
func (q *defaultQDB) SetTaskState(in *structs.Task, st structs.Status, msg string) (string, error) {
	if in == nil {
		return "", nil
	}
	// only allow setting selected states
	if !(st == structs.RUNNING || st == structs.ERRORED || st == structs.COMPLETED || st == structs.SKIPPED) {
		return "", fmt.Errorf("%w %s is not a permitted status (running, errored, completed, skipped)", errors.ErrInvalidState, st)
	}
	if !utils.IsValidID(in.ID) {
		return "", fmt.Errorf("%w %s is not a valid task id", errors.ErrInvalidArg, in.ID)
	}
	if !utils.IsValidID(in.ETag) {
		return "", fmt.Errorf("%w %s is not a valid task etag", errors.ErrInvalidArg, in.ETag)
	}
	etag := utils.NewRandomID()
	altered, err := q.db.SetTasksStatus(st, etag, []*structs.ObjectRef{&structs.ObjectRef{ID: in.ID, ETag: in.ETag}}, msg)
	if err != nil {
		return "", err
	}
	if altered != 1 {
		return "", fmt.Errorf("%w updated altered %d entries", errors.ErrETagMismatch, altered)
	}
	in.Status = st // we know it matches the Task in the DB if this succeeded
	in.ETag = etag
	return etag, err
}

// Tasks returns a slice of tasks matching the given ids.
func (q *defaultQDB) Tasks(id []string) ([]*structs.Task, error) {
	uniques := map[string]bool{}
	ids := []string{}
	for _, i := range id {
		if !utils.IsValidID(i) {
			return nil, fmt.Errorf("%w %s is not a valid task id", errors.ErrInvalidArg, i)
		}
		_, ok := uniques[i]
		if !ok {
			ids = append(ids, i)
			uniques[i] = true
		}
	}
	return q.db.Tasks(&structs.Query{TaskIDs: ids, Limit: len(ids)})
}
