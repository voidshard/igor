package client

import (
	"net/url"

	"github.com/voidshard/igor/pkg/api/http/common"
	"github.com/voidshard/igor/pkg/structs"
)

type Client struct {
	url *url.URL
}

func New(address string) (*Client, error) {
	u, err := url.Parse(address)
	return &Client{url: u}, err
}

func (c *Client) Retry(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_RETRY)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

func (c *Client) Kill(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_KILL)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

func (c *Client) Skip(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_SKIP)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

func (c *Client) Pause(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_PAUSE)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

func (c *Client) Unpause(in []*structs.ObjectRef) (int64, error) {
	addr := c.addr(common.API_UNPAUSE)
	var out common.UpdateResponse
	return out.Updated, genericPatch(addr, in, &out)
}

func (c *Client) Jobs(q *structs.Query) ([]*structs.Job, error) {
	addr := c.addr(common.API_JOBS)
	setQueryString(addr, q)
	var out []*structs.Job
	return out, genericGet(addr, &out)
}

func (c *Client) Layers(q *structs.Query) ([]*structs.Layer, error) {
	addr := c.addr(common.API_LAYERS)
	setQueryString(addr, q)
	var out []*structs.Layer
	return out, genericGet(addr, &out)
}

func (c *Client) Tasks(q *structs.Query) ([]*structs.Task, error) {
	addr := c.addr(common.API_TASKS)
	setQueryString(addr, q)
	var out []*structs.Task
	return out, genericGet(addr, &out)
}

func (c *Client) CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error) {
	addr := c.addr(common.API_JOBS)
	var out structs.CreateJobResponse
	return &out, genericPost(addr, cjr, &out)
}

func (c *Client) CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error) {
	addr := c.addr(common.API_TASKS)
	var out []*structs.Task
	return out, genericPost(addr, in, &out)
}

func (c *Client) addr(path string) *url.URL {
	return &url.URL{Scheme: c.url.Scheme, Host: c.url.Host, Path: path}
}
