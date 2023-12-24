package structs

// JobSpec are fields that can be set when a job is created
type JobSpec struct {
	// Name is an optional human readable name for this job
	Name string `json:"name"`
}

// Job represents a workflow - some set of layers - that need to be completed in
// some order in to be considered 'done'.
type Job struct {
	// JobSpec are fields that can be set when a job is created
	JobSpec `json:",inline"`

	// ID is a unique identifier for this job
	ID string `json:"id"`

	// Status is the current status of this job
	Status Status `json:"status"`

	// ETag is used when updating a job for optimistic locking
	ETag string `json:"etag"`

	// CreatedAt is the time this job was created unix time in seconds
	CreatedAt int64 `json:"created_at"`

	// UpdatedAt is the time this job was last updated unix time in seconds
	UpdatedAt int64 `json:"updated_at"`
}
