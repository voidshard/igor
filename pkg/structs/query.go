package structs

const (
	queryLimitDefault = 1000
	queryLimitMax     = 10000
)

type Query struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`

	// Filters
	JobIDs   []string `json:"job_ids,omitempty"`
	LayerIDs []string `json:"layer_ids,omitempty"`
	TaskIDs  []string `json:"task_ids,omitempty"`
	Statuses []Status `json:"statuses,omitempty"`
}

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
