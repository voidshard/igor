package main

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/voidshard/igor/pkg/structs"
)

func TestJob01(t *testing.T) {
	// create job
	cjr := structs.CreateJobRequest{}

	err := setup.loadTestData("test_job_01.json", &cjr)
	if err != nil {
		t.Fatal(err)
	}

	job, err := setup.client.CreateJob(&cjr)
	if err != nil {
		t.Fatal(err)
	}

	// get job
	query := &structs.Query{Limit: 10000, JobIDs: []string{job.ID}}

	jobs, err := setup.client.Jobs(query)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(jobs), 1)
	assert.Equal(t, jobs[0].ID, job.ID)

	// get layers
	layers, err := setup.client.Layers(query)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(layers), 3)

	layerMap := map[string]*structs.Layer{}
	for _, l := range layers {
		layerMap[l.ID] = l
	}

	for _, originalLayer := range job.Layers {
		layer, ok := layerMap[originalLayer.ID]
		assert.True(t, ok)
		assert.Equal(t, layer.ID, originalLayer.ID)
		assert.Equal(t, layer.Name, originalLayer.Name)
	}

	// get tasks
	tasks, err := setup.client.Tasks(query)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, len(tasks), 9)

	taskMap := map[string]*structs.Task{}
	for _, t := range tasks {
		taskMap[t.ID] = t
	}

	for _, originalLayer := range job.Layers {
		for _, task := range originalLayer.Tasks {
			task, ok := taskMap[task.ID]
			assert.True(t, ok)
			assert.Equal(t, task.ID, task.ID)
			assert.Equal(t, task.Name, task.Name)
		}
	}
}
