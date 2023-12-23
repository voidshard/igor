package structs

type ObjectRef struct {
	Kind Kind   `json:"kind"`
	ID   string `json:"id"`
	ETag string `json:"etag"`
}

func NewObjectRef(id, etag string) *ObjectRef {
	return &ObjectRef{ID: id, ETag: etag}
}

func (o *ObjectRef) Job() *ObjectRef {
	o.Kind = KindJob
	return o
}

func (o *ObjectRef) Layer() *ObjectRef {
	o.Kind = KindLayer
	return o
}

func (o *ObjectRef) Task() *ObjectRef {
	o.Kind = KindTask
	return o
}
