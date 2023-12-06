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

func IsFinalStatus(status Status) bool {
	switch status {
	case COMPLETED, SKIPPED, ERRORED:
		return true
	default:
		return false
	}
}
