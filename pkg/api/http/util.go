package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	hp "net/http"

	ie "github.com/voidshard/igor/pkg/errors"
)

var (
	errmap map[int][]error = map[int][]error{
		hp.StatusBadRequest: []error{
			ie.ErrNoLayers,
			ie.ErrNoTasks,
			ie.ErrNoTaskType,
			ie.ErrParentNoFound,
			ie.ErrETagMismatch,
			ie.ErrMaxExceeded,
			ie.ErrInvalidState,
			ie.ErrInvalidArg,
			ie.ErrNotSupported,
		},
	}
)

// mapError returns the http status code for a given error from Igor, or
// http.StatusInternalServerError if the error is not recognised.
func mapError(err error) int {
	if err == nil {
		return hp.StatusOK
	}
	for code, errs := range errmap {
		for _, e := range errs {
			if errors.Is(err, e) {
				return code
			}
		}
	}
	return hp.StatusInternalServerError
}

// unmarshalJson reads the body of a request and attempts to unmarshal it into the given object.
// This function write an error to the writer if an error occurs, and returns the error.
func unmarshalJson(w hp.ResponseWriter, r *hp.Request, obj interface{}) error {
	if r.Body == nil {
		hp.Error(w, "No body", hp.StatusBadRequest)
		return fmt.Errorf("no body")
	}
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields() // catch unwanted fields

	err := d.Decode(obj)
	if err != nil {
		// bad JSON or unrecognized json field
		http.Error(w, err.Error(), http.StatusBadRequest)
		return fmt.Errorf("bad json: %v", err)
	}

	return nil
}
