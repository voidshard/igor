package client

import (
	"net/url"

	"github.com/voidshard/igor/pkg/api/http/common"
	"github.com/voidshard/igor/pkg/structs"
)

// Client is a simple client for igor's http api
type Client struct {
	url *url.URL
}

// New returns a new client for the given address
func New(address string) (*Client, error) {
	u, err := url.Parse(address)
	return &Client{url: u}, err
}

// Retry the indicated tasks
func (c *Client) Retry(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_RETRY)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

// Kill the indicated tasks
func (c *Client) Kill(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_KILL)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

// Skip the indicated layers and/or tasks
func (c *Client) Skip(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_SKIP)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

// Pause the indicated layers and/or tasks
func (c *Client) Pause(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_PAUSE)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

// Unpause the indicated layers and/or tasks
func (c *Client) Unpause(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_UNPAUSE)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

// Jobs returns jobs matching the given query
func (c *Client) Jobs(q *structs.Query) ([]*structs.Job, error) {
	addr := c.addr(common.API_JOBS)
	setQueryString(addr, q)
	var out []*structs.Job
	return out, genericGet(addr, &out)
}

// Layers returns layers matching the given query
func (c *Client) Layers(q *structs.Query) ([]*structs.Layer, error) {
	addr := c.addr(common.API_LAYERS)
	setQueryString(addr, q)
	var out []*structs.Layer
	return out, genericGet(addr, &out)
}

// Tasks returns tasks matching the given query
func (c *Client) Tasks(q *structs.Query) ([]*structs.Task, error) {
	addr := c.addr(common.API_TASKS)
	setQueryString(addr, q)
	var out []*structs.Task
	return out, genericGet(addr, &out)
}

// CreateJob creates a new job, layer(s) and task(s) & returns the results.
func (c *Client) CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error) {
	addr := c.addr(common.API_JOBS)
	var out structs.CreateJobResponse
	return &out, genericPost(addr, cjr, &out)
}

// CreateTasks creates a new task(s) & returns the results.
func (c *Client) CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error) {
	addr := c.addr(common.API_TASKS)
	var out []*structs.Task
	return out, genericPost(addr, in, &out)
}

// addr returns a url.URL for the given path
func (c *Client) addr(path string) *url.URL {
	return &url.URL{Scheme: c.url.Scheme, Host: c.url.Host, Path: path}
}
