package structs

type ObjectRef struct {
	Kind Kind   `json:"kind"`
	ID   string `json:"id"`
	ETag string `json:"etag"`
}
