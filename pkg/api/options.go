package api

import (
	"time"
)

// Options passed to the Igor API on creation
type Options struct {
	// MaxTaskRuntime is the absolute maximum time a task is permitted to run for.
	// After this time we will attempt to murder the task.
	MaxTaskRuntime time.Duration

	// EventRoutines is the number of goroutines to spawn to handle events
	// (changes to Task(s) & Layer(s))
	EventRoutines int64

	// TidyJobFrequency is how often we look over jobs / layers to check they're in the right states
	// (we do this in case event(s) were dropped / errors occurred)
	TidyJobFrequency time.Duration

	// TidyTaskFrequency is how often we look over tasks to check if they need reaping
	// (ie, they've been running too long & need to be killed / retried)
	TidyTaskFrequency time.Duration

	// TidyRoutines is the number of routines allocated to tidy tasks (above).
	TidyRoutines int64
}
