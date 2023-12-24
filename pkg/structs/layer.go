package structs

type LayerSpec struct {
	// Optional
	Name     string `json:"name"`
	PausedAt int64  `json:"paused_at"`
	Priority int64  `json:"priority"` // lowest goes first, 0 starts immediately
}

type Layer struct {
	LayerSpec `json:",inline"`

	ID     string `json:"id"`
	Status Status `json:"status"`
	ETag   string `json:"etag"`

	JobID string `json:"job_id"`

	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
}
