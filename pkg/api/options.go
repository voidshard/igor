package api

import (
	"time"
)

const (
	defMaxTaskRuntime = 1 * time.Hour
)

// Options passed to the Igor API on creation
type Options struct {
	// MaxTaskRuntime is the absolute maximum time a task is permitted to run for.
	// After this time we will attempt to murder the task.
	MaxTaskRuntime time.Duration

	// EventRoutines is the number of goroutines to spawn to handle events
	// (changes to Task(s) & Layer(s))
	EventRoutines int64

	// TidyLayer is how often we look over layers to check they're in the right states
	// (we do this in case event(s) were dropped / errors occurred)
	TidyLayerFrequency time.Duration

	// TidyTaskFrequency is how often we look over tasks to check if they need reaping
	// (ie, they've been running too long & need to be killed / retried)
	TidyTaskFrequency time.Duration

	// TidyRoutines is the number of routines allocated to tidy tasks (above).
	TidyRoutines int64

	// TidyUpdateThreshold sets how far back in time the last update to an object must
	// be before we'll forcibly recheck it.
	// That is if TidyUpdateThreshold is 5 mins, we only consider in-progress items
	// whose last update was over 5 mins ago.
	// Nb. we only ever check in-progress objects; items in a final state we don't tidy.
	TidyUpdateThreshold time.Duration
}

// OptionsClientDefault runs an Igor service that runs no backend worker routines.
// This is intended either for;
// - clients who want to use the API service to Register queue workers
// - clients who wish to serve an API, without performing background work
func OptionsClientDefault() *Options {
	return &Options{
		MaxTaskRuntime: defMaxTaskRuntime,
	}
}

// OptionsServerDefault runs and Igor service that runs background worker routines to
// ensure consistency of Igor data & handle internal events.
func OptionsServerDefault() *Options {
	return &Options{
		MaxTaskRuntime:      defMaxTaskRuntime,
		EventRoutines:       4,
		TidyRoutines:        2,
		TidyLayerFrequency:  7 * time.Minute,
		TidyTaskFrequency:   3 * time.Minute,
		TidyUpdateThreshold: 5 * time.Minute,
	}
}
