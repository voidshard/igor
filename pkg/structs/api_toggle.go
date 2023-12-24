package structs

type ToggleRequest struct {
	Kind string
	ID   string
	ETag string
}

func (t *ToggleRequest) ObjectID() string {
	return t.ID
}

func (t *ToggleRequest) ObjectETag() string {
	return t.ETag
}
