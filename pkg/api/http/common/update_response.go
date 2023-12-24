package common

// UpdateResponse is the response from an update operation, specific to HTTP.
type UpdateResponse struct {
	// Updated is the number of objects updated.
	//
	// Users should verify that this is the number of objects they expected to be updated
	// with the given ID / ETag combination(s) if it is important.
	Updated int64 `json:"updated"`
}
