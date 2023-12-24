package structs

const (
	queryLimitDefault = 1000
	queryLimitMax     = 10000
)

// Query is a set of filters to apply when querying for jobs.
type Query struct {
	// Limit is the max number of entries to return.
	// Note that there is an upper limit to this value, beyond which Igor will cap it.
	Limit int `json:"limit,omitempty"`

	// Offset is the number of entries to skip before returning results.
	Offset int `json:"offset,omitempty"`

	// JobIDs is a list of job IDs to filter by.
	// This applies to jobs, layers and tasks.
	JobIDs []string `json:"job_ids,omitempty"`

	// LayerIDs is a list of layer IDs to filter by.
	// This applies to layers and tasks.
	LayerIDs []string `json:"layer_ids,omitempty"`

	// TaskIDs is a list of task IDs to filter by.
	// This applies to tasks only.
	TaskIDs []string `json:"task_ids,omitempty"`

	// Statuses is a list of statuses to filter by.
	Statuses []Status `json:"statuses,omitempty"`

	// UpdatedBefore filters by tasks that have been updated before the given timestamp.
	// A value of 0 is ignored.
	UpdatedBefore int64 `json:"updated_before,omitempty"`

	// UpdatedAfter filters by tasks that have been updated after the given timestamp.
	// A value of 0 is ignored.
	UpdatedAfter int64 `json:"updated_after,omitempty"`

	// CreatedBefore filters by tasks that have been created before the given timestamp.
	// A value of 0 is ignored.
	CreatedBefore int64 `json:"created_before,omitempty"`

	// CreatedAfter filters by tasks that have been created after the given timestamp.
	// A value of 0 is ignored.
	CreatedAfter int64 `json:"created_after,omitempty"`
}

// Sanitize ensures passed values are somewhat reasonable.
func (q *Query) Sanitize() {
	if q.Limit <= 0 {
		q.Limit = queryLimitDefault
	}
	if q.Limit > queryLimitMax {
		q.Limit = queryLimitMax
	}
	if q.Offset < 0 {
		q.Offset = 0
	}
	if q.JobIDs == nil || len(q.JobIDs) == 0 {
		q.JobIDs = nil
	}
	if q.LayerIDs == nil || len(q.LayerIDs) == 0 {
		q.LayerIDs = nil
	}
	if q.TaskIDs == nil || len(q.TaskIDs) == 0 {
		q.TaskIDs = nil
	}
	if q.Statuses == nil || len(q.Statuses) == 0 {
		q.Statuses = nil
	}
}
