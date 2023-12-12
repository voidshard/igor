package database

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/voidshard/igor/pkg/structs"
)

func TestToSqlTags(t *testing.T) {

}

func TestToJobSqlArgs(t *testing.T) {
	in := &structs.Job{
		JobSpec: structs.JobSpec{
			Name: "name",
		},
		ID:        "id",
		Status:    structs.READY,
		ETag:      "etag",
		CreatedAt: 100,
		UpdatedAt: 200,
	}

	qstr, result := toJobSqlArgs(2, in)

	assert.Equal(t, "($2, $3, $4, $5, $6, $7)", qstr)
	assert.Equal(t, []interface{}{
		in.Name,
		in.ID,
		in.Status,
		in.ETag,
		in.CreatedAt,
		in.UpdatedAt,
	}, result)
}

func TestToLayerSqlArgs(t *testing.T) {
	in := &structs.Layer{
		LayerSpec: structs.LayerSpec{
			Name:     "name",
			PausedAt: 100,
			Order:    12,
		},
		ID:        "id",
		Status:    structs.READY,
		ETag:      "etag",
		JobID:     "jobid",
		CreatedAt: 200,
		UpdatedAt: 300,
	}

	qstr, result := toLayerSqlArgs(2, in)

	assert.Equal(t, "($2, $3, $4, $5, $6, $7, $8, $9, $10)", qstr)
	assert.Equal(t, []interface{}{
		in.Name,
		in.PausedAt,
		in.Order,
		in.ID,
		in.Status,
		in.ETag,
		in.JobID,
		in.CreatedAt,
		in.UpdatedAt,
	}, result)
}

func TestToTaskSqlArgs(t *testing.T) {
	in := &structs.Task{
		TaskSpec: structs.TaskSpec{
			Type:     "type",
			Args:     []byte(`{"a": "b"}`),
			Name:     "name",
			PausedAt: 100,
			Retries:  12,
		},
		ID:          "id",
		Status:      structs.READY,
		ETag:        "etag",
		JobID:       "jobid",
		LayerID:     "layerid",
		QueueTaskID: "queuetaskid",
		Message:     "message",
		CreatedAt:   200,
		UpdatedAt:   300,
	}

	qstr, result := toTaskSqlArgs(2, in)

	assert.Equal(t, "($2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)", qstr)
	assert.Equal(t, []interface{}{
		in.Type,
		in.Args,
		in.Name,
		in.PausedAt,
		in.Retries,
		in.ID,
		in.Status,
		in.ETag,
		in.JobID,
		in.LayerID,
		in.QueueTaskID,
		in.Message,
		in.CreatedAt,
		in.UpdatedAt,
	}, result)
}

func TestStatusToStrings(t *testing.T) {
	cases := []struct {
		Name   string
		In     []structs.Status
		Expect []string
	}{
		{
			Name:   "Empty",
			In:     []structs.Status{},
			Expect: nil,
		},
		{
			Name:   "Nil",
			In:     nil,
			Expect: nil,
		},
		{
			Name: "All",
			In: []structs.Status{
				structs.PENDING,
				structs.READY,
				structs.QUEUED,
				structs.RUNNING,
				structs.COMPLETED,
				structs.ERRORED,
				structs.SKIPPED,
				structs.KILLED,
			},
			Expect: []string{
				"PENDING",
				"READY",
				"QUEUED",
				"RUNNING",
				"COMPLETED",
				"ERRORED",
				"SKIPPED",
				"KILLED",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			out := statusToStrings(c.In)
			assert.Equal(t, c.Expect, out)
		})
	}
}
