package structs

type JobSpec struct {
	// Optional
	Name string `json:"name"`
}

type Job struct {
	JobSpec `json:",inline"`

	ID     string `json:"id"`
	Status Status `json:"status"`
	ETag   string `json:"etag"`

	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
}
