package structs

// TaskSpec are fields that can be set when a task is created
type TaskSpec struct {
	// Type is the type of task this is. This should match the name of a
	// registered task handler.
	//
	// Required.
	Type string `json:"type"`

	// Args is some optional data that a task should use.
	Args []byte `json:"args"`

	// Name is an optional human readable name for this task
	Name string `json:"name"`

	// PausedAt is the time this task was paused unix time in seconds.
	// If 0, this task is not paused.
	PausedAt int64 `json:"paused_at"`

	// Retries is the number of times this task can be retried on failure(s).
	// This value is passed on to the Queue.
	Retries int64 `json:"retries"`
}

// Task represents a single unit of work that needs to be done.
type Task struct {
	// TaskSpec are fields that can be set when a task is created
	TaskSpec `json:",inline"`

	// ID is a unique identifier for this task
	ID string `json:"id"`

	// Status is the current status of this task
	Status Status `json:"status"`

	// ETag is used when updating a task for optimistic locking
	ETag string `json:"etag"`

	// JobID is the ID of the job this task belongs to
	JobID string `json:"job_id"`

	// LayerID is the ID of the layer this task belongs to
	LayerID string `json:"layer_id"`

	// CreatedAt is the time this task was created unix time in seconds
	CreatedAt int64 `json:"created_at"`

	// UpdatedAt is the time this task was last updated unix time in seconds
	UpdatedAt int64 `json:"updated_at"`

	// QueueTaskID is the ID of the task in the Queue (ie. the ID returned when we Enqueue it).
	// This ID is kept here for tracking purposes & in order to Kill() the task if required.
	QueueTaskID string `json:"queue_task_id"`

	// Message is an optional message from the task handler (user specified).
	Message string `json:"message"`
}
