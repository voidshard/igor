package structs

type JobLayerRequest struct {
	LayerSpec `json:",inline"`

	Tasks []TaskSpec
}

type CreateJobRequest struct {
	JobSpec `json:",inline"`

	Layers []JobLayerRequest
}

type JobLayerResponse struct {
	*Layer `json:",inline"`

	Tasks []*Task
}

type CreateJobResponse struct {
	*Job `json:",inline"`

	Layers []*JobLayerResponse
}
