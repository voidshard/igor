package core

import (
	"fmt"

	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

// QueueDB is a cut down interface to our core.Service; this is passed to a Queue to allow it to
// perform a subset of actions on the database.
type QueueDB struct {
	db database.Database
}

// NewQueueDB returns a new QueueDB
func NewQueueDB(db database.Database) *QueueDB {
	return &QueueDB{db: db}
}

// SetRunsState sets the state of the given runs to the given status.
// This func is restricted to setting a final state (errored, completed, skipped).
func (q *QueueDB) SetRunsState(in []*structs.Run, st structs.Status, msg string) error {
	if in == nil || len(in) == 0 {
		return nil
	}
	if !isFinalStatus(st) {
		return fmt.Errorf("%w %s is not a final status (errored, completed, skipped)", errors.ErrInvalidState, st)
	}
	ids := []*database.IDTag{}
	for _, t := range in {
		ids = append(ids, &database.IDTag{ID: t.ID, ETag: t.ETag})
	}
	msgs := []string{}
	if msg != "" {
		msgs = []string{msg}
	}
	_, err := q.db.SetRunsStatus(st, utils.NewRandomID(), ids, msgs...)
	return err
}

// SetTasksState sets the state of the given tasks to the given status.
// This func is restricted to setting a final state (errored, completed, skipped).
func (q *QueueDB) SetTasksState(in []*structs.Task, st structs.Status) error {
	if in == nil || len(in) == 0 {
		return nil
	}
	if !isFinalStatus(st) {
		return fmt.Errorf("%w %s is not a final status (errored, completed, skipped)", errors.ErrInvalidState, st)
	}
	ids := []*database.IDTag{}
	for _, t := range in {
		ids = append(ids, &database.IDTag{ID: t.ID, ETag: t.ETag})
	}
	_, err := q.db.SetTasksStatus(st, utils.NewRandomID(), ids)
	return err
}

// Tasks returns a slice of tasks matching the given ids.
func (q *QueueDB) Tasks(id []string) ([]*structs.Task, error) {
	return q.db.Tasks(&structs.Query{TaskIDs: id, Limit: len(id)})
}

// Runs returns a slice of runs matching the given ids.
func (q *QueueDB) Runs(id []string) ([]*structs.Run, error) {
	return q.db.Runs(&structs.Query{RunIDs: id, Limit: len(id)})
}
