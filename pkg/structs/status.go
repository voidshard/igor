package structs

import (
	"strings"
)

type Status string

const (
	// transient states
	PENDING Status = "PENDING"
	READY   Status = "READY"
	QUEUED  Status = "QUEUED"
	RUNNING Status = "RUNNING"

	// end states
	KILLED    Status = "KILLED"
	COMPLETED Status = "COMPLETED"
	ERRORED   Status = "ERRORED"
	SKIPPED   Status = "SKIPPED"
)

func IsFinalStatus(status Status) bool {
	switch status {
	case COMPLETED, SKIPPED, ERRORED, KILLED:
		return true
	default:
		return false
	}
}

func ToStatus(s string) Status {
	switch strings.ToUpper(s) {
	case "PENDING":
		return PENDING
	case "READY":
		return READY
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
