package structs

// ObjectRef is a reference to a unique object & version.
type ObjectRef struct {
	// Kind is the type of object.
	Kind Kind `json:"kind"`

	// ID is the unique identifier for this object.
	ID string `json:"id"`

	// ETag is the version of this object.
	ETag string `json:"etag"`
}

// NewObjectRef creates a new ObjectRef.
func NewObjectRef(id, etag string) *ObjectRef {
	return &ObjectRef{ID: id, ETag: etag}
}

// Job tags the Ref as kind: Job
func (o *ObjectRef) Job() *ObjectRef {
	o.Kind = KindJob
	return o
}

// Layer tags the Ref as kind: Layer
func (o *ObjectRef) Layer() *ObjectRef {
	o.Kind = KindLayer
	return o
}

// Task tags the Ref as kind: Task
func (o *ObjectRef) Task() *ObjectRef {
	o.Kind = KindTask
	return o
}
