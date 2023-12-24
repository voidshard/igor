package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/voidshard/igor/pkg/structs"
)

// genericPost is a helper to POST data to a given URL and unmarshal the response
func genericPost(addr *url.URL, in interface{}, out interface{}) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}

	resp, err := http.Post(addr.String(), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	} else if resp.Body == nil {
		return fmt.Errorf("no response body with status code %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 { // some error code, assume message is error message
		return fmt.Errorf("bad status code %d, returned %s", resp.StatusCode, string(body))
	}

	return json.Unmarshal(body, out)
}

// genericPatch is a helper to PATCH data to a given URL and unmarshal the response
func genericPatch(addr *url.URL, in interface{}, out interface{}) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}

	// it's kind of odd the HTTP package doesn't have a Patch method where it has Get & Post
	req, err := http.NewRequest(http.MethodPatch, addr.String(), bytes.NewBuffer(data))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	} else if resp.Body == nil {
		return fmt.Errorf("no response body with status code %d", resp.StatusCode)
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 { // some error code, assume message is error message
		return fmt.Errorf("bad status code %d, returned %s", resp.StatusCode, string(body))
	}

	return json.Unmarshal(body, out)
}

// genericGet is a helper to GET data from a given URL and unmarshal the response.
// Implies the Query string is already set, if needed.
func genericGet(addr *url.URL, out interface{}) error {
	resp, err := http.Get(addr.String())
	if err != nil {
		return err
	} else if resp.Body == nil { // there is no data to read
		if resp.StatusCode >= 400 {
			return fmt.Errorf("bad status code: %d", resp.StatusCode)
		}
		return nil
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 { // some error code, assume message is error message
		return fmt.Errorf("bad status code %d, returned %s", resp.StatusCode, string(body))
	}

	return json.Unmarshal(body, out)
}

// setQueryString sets the query string of a URL based on the given query object.
func setQueryString(u *url.URL, q *structs.Query) {
	q.Sanitize()
	values := u.Query()

	if q.Limit > 0 {
		values.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Offset > 0 {
		values.Set("offset", strconv.Itoa(q.Offset))
	}
	if q.JobIDs != nil {
		values["job_ids"] = q.JobIDs
	}
	if q.LayerIDs != nil {
		values["layer_ids"] = q.LayerIDs
	}
	if q.TaskIDs != nil {
		values["task_ids"] = q.TaskIDs
	}
	if q.Statuses != nil {
		ss := []string{}
		for _, s := range q.Statuses {
			ss = append(ss, string(s))
		}
		values["statuses"] = ss
	}

	u.RawQuery = values.Encode()
}
