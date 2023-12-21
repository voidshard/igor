package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/voidshard/igor/internal/utils"
	ie "github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

var (
	errmap map[int][]error = map[int][]error{
		http.StatusBadRequest: []error{
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
		return http.StatusOK
	}
	for code, errs := range errmap {
		for _, e := range errs {
			if errors.Is(err, e) {
				return code
			}
		}
	}
	return http.StatusInternalServerError
}

func unmarshalQuery(w http.ResponseWriter, r *http.Request, out *structs.Query) error {
	q := r.URL.Query()

	if q.Has("limit") {
		limit, err := strconv.Atoi(q.Get("limit"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return fmt.Errorf("bad limit: %v", err)
		}
		out.Limit = limit
	}

	if q.Has("offset") {
		offset, err := strconv.Atoi(q.Get("offset"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return fmt.Errorf("bad offset: %v", err)
		}
		out.Offset = offset
	}

	if q.Has("job_ids") {
		out.JobIDs = q["job_ids"]
		for _, id := range out.JobIDs {
			if !utils.IsValidID(id) {
				http.Error(w, "bad job id", http.StatusBadRequest)
				return fmt.Errorf("bad job id: %v", id)
			}
		}
	}
	if q.Has("layer_ids") {
		out.LayerIDs = q["layer_ids"]
		for _, id := range out.LayerIDs {
			if !utils.IsValidID(id) {
				http.Error(w, "bad layer id", http.StatusBadRequest)
				return fmt.Errorf("bad layer id: %v", id)
			}
		}
	}
	if q.Has("task_ids") {
		out.TaskIDs = q["task_ids"]
		for _, id := range out.TaskIDs {
			if !utils.IsValidID(id) {
				http.Error(w, "bad task id", http.StatusBadRequest)
				return fmt.Errorf("bad task id: %v", id)
			}
		}
	}
	if q.Has("statuses") {
		out.Statuses = []structs.Status{}
		for _, s := range q["statuses"] {
			st := structs.ToStatus(s)
			if st == "" {
				http.Error(w, "bad status", http.StatusBadRequest)
				return fmt.Errorf("bad status: %v", s)
			}
			out.Statuses = append(out.Statuses, st)
		}
	}

	out.Sanitize()
	return nil
}

// unmarshalJson reads the body of a request and attempts to unmarshal it into the given object.
// This function write an error to the writer if an error occurs, and returns the error.
func unmarshalJson(w http.ResponseWriter, r *http.Request, obj interface{}) error {
	if r.Body == nil {
		http.Error(w, "No body", http.StatusBadRequest)
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
