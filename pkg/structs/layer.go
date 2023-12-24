package structs

// LayerSpec are fields that can be set when a layer is created
type LayerSpec struct {
	// Name is an optional human readable name for this layer
	Name string `json:"name"`

	// PausedAt is the time this layer was paused unix time in seconds.
	// If 0, this layer is not paused.
	PausedAt int64 `json:"paused_at"`

	// Priority is the priority of this layer. Higher priority layers can only
	// start when all lower priority layers are complete (or skipped).
	//
	// When a job is created the lowest prioirty layers are set running.
	//
	// A job may have multiple layers with the same priority.
	Priority int64 `json:"priority",db:"layer_priority"`
}

// Layer is a set of related tasks that can be run in parallel.
type Layer struct {
	// LayerSpec are fields that can be set when a layer is created
	LayerSpec `json:",inline"`

	// ID is a unique identifier for this layer
	ID string `json:"id"`

	// Status is the current status of this layer
	Status Status `json:"status"`

	// ETag is used when updating a layer for optimistic locking
	ETag string `json:"etag"`

	// JobID is the ID of the job this layer belongs to
	JobID string `json:"job_id"`

	// CreatedAt is the time this layer was created unix time in seconds
	CreatedAt int64 `json:"created_at"`

	// UpdatedAt is the time this layer was last updated unix time in seconds
	UpdatedAt int64 `json:"updated_at"`
}
