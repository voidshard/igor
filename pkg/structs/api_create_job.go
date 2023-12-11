package structs

type JobLayerRequest struct {
	LayerSpec `json:",inline"`

	Tasks []TaskSpec
}

type CreateJobRequest struct {
	JobSpec `json:",inline"`

	Layers []JobLayerRequest `json:"layers"`
}

type JobLayerResponse struct {
	*Layer `json:",inline"`

	Tasks []*Task `json:"tasks"`
}

type CreateJobResponse struct {
	*Job `json:",inline"`

	Layers []*JobLayerResponse `json:"layers"`
}
