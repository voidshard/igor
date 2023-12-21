package main

import (
	"fmt"

	"github.com/voidshard/igor/pkg/api/http/client"
	"github.com/voidshard/igor/pkg/structs"
	// "github.com/voidshard/igor/pkg/api/http/client"
	// "github.com/voidshard/igor/pkg/structs"
)

var data = []byte(`{"name":"test-job-01","id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","status":"RUNNING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","created_at":1703184980,"updated_at":1703184980,"layers":[{"name":"test-layer-01","paused_at":100,"priority":0,"id":"69bc4a75-1026-440f-b9e5-bd5fd15ded58","status":"RUNNING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","created_at":1703184980,"updated_at":1703184980,"tasks":[{"type":"sleep","args":"MQ==","name":"test-task-01-01","paused_at":0,"retries":0,"id":"75703f08-e97d-49cc-bd59-e91093e05b77","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","layer_id":"69bc4a75-1026-440f-b9e5-bd5fd15ded58","created_at":1703184980,"updated_at":1703184980,"queue_task_id":"","message":""},{"type":"sleep","args":"Mg==","name":"test-task-01-02","paused_at":0,"retries":0,"id":"c641457e-6805-4a93-a513-88caea55b07c","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","layer_id":"69bc4a75-1026-440f-b9e5-bd5fd15ded58","created_at":1703184980,"updated_at":1703184980,"queue_task_id":"","message":""},{"type":"sleep","args":"Mw==","name":"test-task-01-03","paused_at":50,"retries":1,"id":"56d9407e-5d8d-4ede-a184-581c3f0a9e4d","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","layer_id":"69bc4a75-1026-440f-b9e5-bd5fd15ded58","created_at":1703184980,"updated_at":1703184980,"queue_task_id":"","message":""}]},{"name":"test-layer-02","paused_at":100,"priority":0,"id":"2da71603-df9a-469f-8fba-48886d195ab8","status":"RUNNING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","created_at":1703184980,"updated_at":1703184980,"tasks":[{"type":"sleep","args":"NA==","name":"test-task-02-01","paused_at":0,"retries":0,"id":"90aae12f-1e91-485a-8dbe-635ad015fb66","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","layer_id":"2da71603-df9a-469f-8fba-48886d195ab8","created_at":1703184980,"updated_at":1703184980,"queue_task_id":"","message":""},{"type":"sleep","args":"NQ==","name":"test-task-02-02","paused_at":0,"retries":0,"id":"32953483-ef6c-419f-b6ab-1bfc63ac70a8","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","layer_id":"2da71603-df9a-469f-8fba-48886d195ab8","created_at":1703184980,"updated_at":1703184980,"queue_task_id":"","message":""},{"type":"sleep","args":"Ng==","name":"test-task-02-03","paused_at":0,"retries":0,"id":"a5071dd8-e2b1-4d78-adc8-94a8af689f36","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","layer_id":"2da71603-df9a-469f-8fba-48886d195ab8","created_at":1703184980,"updated_at":1703184980,"queue_task_id":"","message":""}]},{"name":"test-layer-03","paused_at":0,"priority":10,"id":"01379859-53e3-46a5-8c33-311a0c6ef15b","status":"PENDING","etag":"697b48ea-7edb-4596-b979-4dd46bf41835","job_id":"25c9fc85-337f-4b14-ab94-f49ea17edc78","created_at":1703184980,"updated_at":1703184980,"tasks":[]}]}`)

var id = "856bde0b-71ab-481f-8e3d-a5b2065dda3d"

func main() {
	get()
}

func get() *structs.Job {
	client, err := client.New("http://localhost:8100")
	if err != nil {
		panic(err)
	}

	query := &structs.Query{Limit: 10000, JobIDs: []string{id}}
	jobs, err := client.Jobs(query)
	if err != nil {
		panic(err)
	}
	if len(jobs) != 1 {
		panic("bad")
	}

	fmt.Println(jobs[0])

	return jobs[0]
}

func create() *structs.CreateJobResponse {
	cjr := &structs.CreateJobRequest{
		JobSpec: structs.JobSpec{
			Name: "test",
		},
		Layers: []structs.JobLayerRequest{
			{
				LayerSpec: structs.LayerSpec{
					Name:     "test",
					PausedAt: 100,
				},
				Tasks: []structs.TaskSpec{
					{
						Name:     "test",
						Type:     "sleep",
						PausedAt: 120,
					},
				},
			},
		},
	}

	client, err := client.New("http://localhost:8100")
	if err != nil {
		panic(err)
	}

	resp, err := client.CreateJob(cjr)
	if err != nil {
		panic(err)
	}

	fmt.Println(resp)

	return resp
}
