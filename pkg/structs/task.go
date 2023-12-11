package structs

type TaskSpec struct {
	// Required
	Type string `json:"type"`

	// Optional
	Args     []byte `json:"args"`
	Name     string `json:"name"`
	PausedAt int64  `json:"paused_at"`
	Retries  int64  `json:"retries"`
}

type Task struct {
	TaskSpec `json:",inline"`

	ID     string `json:"id"`
	Status Status `json:"status"`
	ETag   string `json:"etag"`

	JobID   string `json:"job_id"`
	LayerID string `json:"layer_id"`

	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`

	QueueTaskID string `json:"queue_task_id"`
	Message     string `json:"message"`
}
