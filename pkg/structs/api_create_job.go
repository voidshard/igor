package structs

// CreateJobRequest is an outline to create a new job.
type CreateJobRequest struct {
	// JobSpec are fields that can be set when a job is created
	JobSpec `json:",inline"`

	// Layers is a list of layers that should be created as part of this job.
	// At least one layer is required.
	Layers []JobLayerRequest `json:"layers"`
}

// JobLayerRequest is an outline of a layer that can be created as part of a job.
type JobLayerRequest struct {
	// LayerSpec are fields that can be set when a layer is created
	LayerSpec `json:",inline"`

	// Tasks is a list of tasks that should be created as part of this layer.
	// This can be empty.
	Tasks []TaskSpec
}

// CreateJobResponse is the API response when a job has been created.
//
// Note that the Request (above) actually creates a Job, Layer(s) and possibly even Task(s).
// Everything that has been created is returned in this response organised in a similar
// structure to the Request.
type CreateJobResponse struct {
	// The created job
	*Job `json:",inline"`

	// Layers is a list of layers that have been created as part of this job.
	Layers []*JobLayerResponse `json:"layers"`
}

// JobLayerResponse is the API response when a layer has been created.
type JobLayerResponse struct {
	// The created layer
	*Layer `json:",inline"`

	// Tasks is a list of tasks that have been created as part of this layer (if any).
	Tasks []*Task `json:"tasks"`
}
