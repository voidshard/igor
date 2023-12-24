package structs

import (
	"strings"
)

// Status is the current status of a job, layer or task.
// Not all objects can have all statuses.
type Status string

const (
	// transient states

	// PENDING is the default status for a new job, layer or task.
	PENDING Status = "PENDING"

	// QUEUED is the status of a task that has been queued in the queue.
	QUEUED Status = "QUEUED"

	// RUNNING is the status of a job, layer or task that is currently running.
	RUNNING Status = "RUNNING"

	// end states

	// KILLED is the status of a task that we're currently killing, before it progresses to ERRORED.
	KILLED Status = "KILLED"

	// COMPLETED is the status of a job, layer or task that has completed successfully.
	COMPLETED Status = "COMPLETED"

	// ERRORED is the status of a job, layer or task that has completed with an error.
	ERRORED Status = "ERRORED"

	// SKIPPED is a status set on a layer or task by a user to indicate that the work should
	// be skipped. This allows job to proceed and possibly complete.
	SKIPPED Status = "SKIPPED"
)

// IsFinalStatus returns true if the given status is a final status.
// Ie. the object has finished running.
//
// This doesn't preclude something from being retried.
func IsFinalStatus(status Status) bool {
	switch status {
	case COMPLETED, SKIPPED, ERRORED, KILLED:
		return true
	default:
		return false
	}
}

// ToStatus converts a string to a Status.
func ToStatus(s string) Status {
	switch strings.ToUpper(s) {
	case "PENDING":
		return PENDING
	case "QUEUED":
		return QUEUED
	case "RUNNING":
		return RUNNING
	case "KILLED":
		return KILLED
	case "COMPLETED":
		return COMPLETED
	case "ERRORED":
		return ERRORED
	case "SKIPPED":
		return SKIPPED
	default:
		return ""
	}
}
