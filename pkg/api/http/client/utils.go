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
	fmt.Println("BODY", string(body))

	if resp.StatusCode >= 400 { // some error code, assume message is error message
		return fmt.Errorf("bad status code %d, returned %s", resp.StatusCode, string(body))
	}

	return json.Unmarshal(body, out)
}

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
	fmt.Println("BODY", string(body))

	return json.Unmarshal(body, out)
}

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
