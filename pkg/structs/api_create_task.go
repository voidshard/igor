package structs

// CreateTaskRequest is an outline to create a new task.
type CreateTaskRequest struct {
	// TaskSpec are fields that can be set when a task is created
	TaskSpec `json:",inline"`

	// LayerID is the ID of the layer this task belongs to
	LayerID string `json:"layer_id"`
}
