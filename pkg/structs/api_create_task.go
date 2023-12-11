package structs

type CreateTaskRequest struct {
	TaskSpec `json:",inline"`
	LayerID  string `json:"layer_id"`
}
