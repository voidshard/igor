package queue

import (
	"github.com/voidshard/igor/pkg/structs"
)

// Meta includes all of the task / run information we need to process a task.
type Meta struct {
	Task *structs.Task
	Run  *structs.Run

	err  error
	skip bool
	msg  string
}

// SetError will cause the task & run to be marked as errored.
//
// Errored tasks may be retried (depending on settings).
func (m *Meta) SetError(err error) {
	m.err = err
}

// SetSkip will cause the task & run to be marked as skipped.
//
// Skipped trumps Errored; we're essentially saying "I no longer care about this."
func (m *Meta) SetSkip() {
	m.skip = true
}

// SetMessage will set a message on the run.
func (m *Meta) SetMessage(in string) {
	m.msg = in
}
