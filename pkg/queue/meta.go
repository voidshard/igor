package queue

import (
	"strings"

	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/structs"
)

// Meta includes all of the task / run information we need to process a task.
type Meta struct {
	Task *structs.Task

	set  bool
	svc  database.QueueDB
	errs chan<- error
}

// SetError will cause the task to be marked as errored.
//
// Errored tasks may be retried (depending on settings).
func (m *Meta) SetError(err error) {
	_, err = m.svc.SetTaskState(m.Task, structs.ERRORED, err.Error())
	m.errs <- err
}

// SetSkip will cause the task to be marked as skipped.
//
// Skipped trumps Errored; we're essentially saying "I no longer care about this."
func (m *Meta) SetSkip(msg ...string) {
	_, err := m.svc.SetTaskState(m.Task, structs.SKIPPED, strings.Join(msg, " "))
	m.errs <- err
}

// SetComplete will cause the task to be marked as completed.
//
// If at the end of the handler the task is still RUNNING or QUEUED then this is called for you.
func (m *Meta) SetComplete(msg ...string) {
	_, err := m.svc.SetTaskState(m.Task, structs.COMPLETED, strings.Join(msg, " "))
	m.errs <- err
}

// SetRunning will cause the task to be marked as running.
//
// This doesn't have any really effect on the task & calling this function at all is optional
// & mostly for bookkeeping - if you have a lot of long running tasks in a batch then you
// might want to call this to indicate when something transitions from queued to running.
//
// It does cause DB overhead, so for lots of swift tasks it's .. probably not worth it.
func (m *Meta) SetRunning(msg ...string) {
	_, err := m.svc.SetTaskState(m.Task, structs.RUNNING, strings.Join(msg, " "))
	m.errs <- err
}
