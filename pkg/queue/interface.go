package queue

import (
	"github.com/voidshard/igor/pkg/structs"
)

type Queue interface {
	// Register a task handler. This is a function that will be called when a task is enqueued.
	//
	// The handler is passed a slice of {Task, Run} pairs; this is to allow for batch processing
	// if the Queue in use supports it (otherwise, you'll always get a single item in the slice).
	//
	// In general, if possible, tasks should be batched. Task queues that don't allow this are
	// somewhat dubious imho.
	Register(task string, handler func(work []*Meta) error) error

	// Run the queue & process tasks (via Register funcs). This should block until Close() is called.
	Run() error

	// Enqueue a task with the given id and arguments.
	//
	// If it supports it, the Queue will return a unique id for the queued task with which we can
	// call Kill(the-given-id) to stop the task from running.
	Enqueue(task *structs.Task) (string, error)

	// Kill a queued task with ID given to us by Enqueue.
	Kill(queuedTaskID string) error

	// Close & shutdown the queue.
	Close() error
}
