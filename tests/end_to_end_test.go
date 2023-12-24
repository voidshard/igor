package main

import (
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/voidshard/igor/pkg/structs"
)

// TestJob02 End to End test
//
// - Creates job with 2 layers, 1 with 2 tasks, 1 with 1 task
// - the 1st task of each layer will die
// - gets job
// - gets layers
// - gets tasks
// - waits for task 01 from layer 01 to be errored
// - waits for layer 02 to be errored
// - skips both the errored layer and task
// - waits for job to finish
func TestJob02(t *testing.T) {
	// create job
	cjr := structs.CreateJobRequest{}

	err := setup.loadTestData("test_job_02.json", &cjr)
	if err != nil {
		t.Fatal(err)
	}

	// create job
	job, err := setup.client.CreateJob(&cjr)
	assert.Nil(t, err)

	// get job
	query := &structs.Query{Limit: 100, JobIDs: []string{job.ID}}
	jobs, err := setup.client.Jobs(query)
	assert.Nil(t, err)
	assert.Equal(t, len(jobs), 1)

	// get layers
	layers, err := setup.client.Layers(query)
	assert.Nil(t, err)
	assert.Equal(t, len(layers), 2)

	// get tasks
	tasks, err := setup.client.Tasks(query)
	assert.Nil(t, err)
	assert.Equal(t, len(tasks), 3)

	// we'll skip one task from layer 01 and layer 02
	skipLayerID := ""
	skipTaskID := ""
	for _, l := range job.Layers {
		if l.Name == "test-layer-02" { // layer 02, whose one task will die
			skipLayerID = l.ID
		}
		for _, t := range l.Tasks {
			if t.Name == "test-task-01-01" { // layer 01's 1st task, which will die
				skipTaskID = t.ID
			}
		}
	}

	// wait for task 01 from layer 01 to die
	start := time.Now()
	for {
		if time.Since(start) > (5 * time.Minute) {
			t.Fatalf("timed out waiting for task %s to finish", skipTaskID)
			return
		}
		time.Sleep(5 * time.Second)
		tasks, err = setup.client.Tasks(&structs.Query{Limit: 5, TaskIDs: []string{skipTaskID}})
		assert.Nil(t, err)
		assert.Equal(t, len(tasks), 1)

		if structs.IsFinalStatus(tasks[0].Status) {
			assert.Equal(t, tasks[0].Status, structs.ERRORED)
			break
		}
	}

	// wait for layer 02 to die
	start = time.Now()
	for {
		if time.Since(start) > (5 * time.Minute) {
			t.Fatalf("timed out waiting for layer %s to finish", skipLayerID)
			return
		}
		time.Sleep(5 * time.Second)
		layers, err = setup.client.Layers(&structs.Query{Limit: 5, LayerIDs: []string{skipLayerID}})
		assert.Nil(t, err)
		assert.Equal(t, len(layers), 1)

		if structs.IsFinalStatus(layers[0].Status) {
			assert.Equal(t, layers[0].Status, structs.ERRORED)
			break
		}
	}

	// now we skip both the errored layer and task
	info, err := setup.client.Skip([]*structs.ObjectRef{
		structs.NewObjectRef(skipLayerID, layers[0].ETag).Layer(),
		structs.NewObjectRef(skipTaskID, tasks[0].ETag).Task(),
	})
	assert.Nil(t, err)
	assert.Equal(t, info, int64(2))

	// wait for the job to finish
	start = time.Now()
	for {
		if time.Since(start) > (5 * time.Minute) {
			t.Fatal("timed out waiting for job to finish")
			return
		}
		time.Sleep(5 * time.Second)
		jobs, err = setup.client.Jobs(query)
		assert.Nil(t, err)
		assert.Equal(t, len(jobs), 1)

		if structs.IsFinalStatus(jobs[0].Status) {
			assert.Equal(t, jobs[0].Status, structs.COMPLETED)
			break
		}
	}
}

// TestJob01 End to End test
//
// Nb. The limits here are set too high to test we don't get more results than we expect.
// We're fetching by ID(s) so we .. shouldn't ..
//
// - Creates job with 3 layers, 2 with 3 tasks, 1 with 0 tasks
// - Gets job
// - Gets layers
// - Gets tasks
// - Creates task on layer with 0 tasks
// - Unpauses all layers & tasks
// - Waits for job to finish
func TestJob01(t *testing.T) {
	cjr := structs.CreateJobRequest{}
	ctr := []*structs.CreateTaskRequest{}

	err := setup.loadTestData("test_job_01.json", &cjr)
	if err != nil {
		t.Fatal(err)
	}
	err = setup.loadTestData("test_task_sleep.json", &ctr)
	if err != nil {
		t.Fatal(err)
	}

	// create job
	job, err := setup.client.CreateJob(&cjr)
	assert.Nil(t, err)

	// get job
	query := &structs.Query{Limit: 100, JobIDs: []string{job.ID}}

	jobs, err := setup.client.Jobs(query)
	assert.Nil(t, err)
	assert.Equal(t, len(jobs), 1)
	assert.Equal(t, jobs[0].ID, job.ID)
	assert.Equal(t, jobs[0].Name, job.Name)
	assert.NotEqual(t, jobs[0].ETag, "")
	assert.NotEqual(t, jobs[0].CreatedAt, 0)
	assert.NotEqual(t, jobs[0].UpdatedAt, 0)

	// get layers
	layers, err := setup.client.Layers(query)
	assert.Nil(t, err)
	assert.Equal(t, len(layers), 3)

	layerMap := map[string]*structs.Layer{}
	for _, l := range layers {
		layerMap[l.ID] = l
	}

	for _, originalLayer := range job.Layers {
		layer, ok := layerMap[originalLayer.ID]
		assert.True(t, ok)
		assert.Equal(t, job.ID, layer.JobID)
		assert.Equal(t, layer.ID, originalLayer.ID)
		assert.Equal(t, layer.Name, originalLayer.Name)
		assert.Equal(t, layer.PausedAt, originalLayer.PausedAt)
		assert.NotEqual(t, layer.ETag, "")
		assert.NotEqual(t, layer.CreatedAt, 0)
		assert.NotEqual(t, layer.UpdatedAt, 0)
	}

	// get tasks
	tasks, err := setup.client.Tasks(query)
	assert.Nil(t, err)
	assert.Equal(t, len(tasks), 6)

	taskMap := map[string]*structs.Task{}
	for _, t := range tasks {
		taskMap[t.ID] = t
	}

	toUnpause := []*structs.ObjectRef{}
	zeroTaskLayerID := ""
	for _, originalLayer := range job.Layers {
		if len(originalLayer.Tasks) == 0 {
			zeroTaskLayerID = originalLayer.ID
		}
		if originalLayer.PausedAt > 0 {
			toUnpause = append(toUnpause, structs.NewObjectRef(originalLayer.ID, originalLayer.ETag).Layer())
		}
		for _, originalTask := range originalLayer.Tasks {
			task, ok := taskMap[originalTask.ID]
			assert.True(t, ok)
			assert.Equal(t, task.ID, originalTask.ID)
			assert.Equal(t, task.Name, originalTask.Name)
			assert.Equal(t, task.PausedAt, originalTask.PausedAt)
			assert.Equal(t, task.LayerID, originalLayer.ID)
			assert.Equal(t, task.JobID, job.ID)
			assert.NotEqual(t, task.ETag, "")
			assert.NotEqual(t, task.CreatedAt, 0)
			assert.NotEqual(t, task.UpdatedAt, 0)
			if task.PausedAt > 0 {
				toUnpause = append(toUnpause, structs.NewObjectRef(task.ID, task.ETag).Task())
			}
		}
	}

	// create task on a layer that exists
	assert.NotEqual(t, zeroTaskLayerID, "")
	ctr[0].LayerID = zeroTaskLayerID
	newTasks, err := setup.client.CreateTasks(ctr)
	assert.Nil(t, err)

	assert.Equal(t, len(newTasks), 1)
	assert.Equal(t, newTasks[0].Name, "added-task")
	assert.Equal(t, newTasks[0].LayerID, zeroTaskLayerID)

	// unpause layers & tasks
	info, err := setup.client.Unpause(toUnpause)
	assert.Nil(t, err)
	assert.Equal(t, info, int64(len(toUnpause)))

	// wait for the job to finish
	start := time.Now()
	for {
		if time.Since(start) > (5 * time.Minute) {
			t.Fatal("timed out waiting for job to finish")
			return
		}
		log.Println("waiting for job to finish")
		time.Sleep(5 * time.Second)
		jobs, err := setup.client.Jobs(query)
		assert.Nil(t, err)
		assert.Equal(t, len(jobs), 1)

		if structs.IsFinalStatus(jobs[0].Status) {
			assert.Equal(t, jobs[0].Status, structs.COMPLETED)
			break
		}
	}
}
