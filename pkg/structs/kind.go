package structs

type Kind string

const (
	KindJob   Kind = "Job"
	KindLayer Kind = "Layer"
	KindTask  Kind = "Task"
	KindRun   Kind = "Run"
)
