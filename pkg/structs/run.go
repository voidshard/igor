package structs

type Run struct {
	ID     string `json:"id"`
	Status Status `json:"status"`
	ETag   string `json:"etag"`

	JobID   string `json:"job_id"`
	LayerID string `json:"layer_id"`
	TaskID  string `json:"task_id"`

	QueueTaskID string `json:"queue_task_id"`
	Message     string `json:"message"`

	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
}
