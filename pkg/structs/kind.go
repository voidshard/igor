package structs

// Kind is the type of object.
//
// We use this with our ObjectRef structs (a set of ID, ETag, Kind) to pin the exact object
// and version we're referring to.
type Kind string

const (
	// KindJob is a job
	KindJob Kind = "Job"

	// KindLayer is a layer
	KindLayer Kind = "Layer"

	// KindTask is a task
	KindTask Kind = "Task"
)
