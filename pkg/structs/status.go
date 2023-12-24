package structs

type Status string

const (
	// transient states
	PENDING Status = "PENDING"
	QUEUED  Status = "QUEUED"
	RUNNING Status = "RUNNING"
	KILLED  Status = "KILLED"

	// end states
	COMPLETED Status = "COMPLETED"
	ERRORED   Status = "ERRORED"
	SKIPPED   Status = "SKIPPED"
)
