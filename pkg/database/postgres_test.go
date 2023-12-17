package database

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/voidshard/igor/pkg/structs"
)

func TestToSqlQuery(t *testing.T) {
	cases := []struct {
		Name          string
		In            map[string][]string
		ExpectQuery   string
		ExpectArgs    []interface{}
		UpdatedBefore int64
		UpdatedAfter  int64
		CreatedBefore int64
		CreatedAfter  int64
	}{
		{
			Name:        "Nil",
			In:          nil,
			ExpectQuery: "",
			ExpectArgs:  []interface{}{},
		},
		{
			Name:        "Empty",
			In:          map[string][]string{},
			ExpectQuery: "",
			ExpectArgs:  []interface{}{},
		},
		{
			Name:          "TimeFilters",
			In:            map[string][]string{},
			ExpectQuery:   "WHERE updated_at >= $1 AND updated_at <= $2 AND created_at >= $3 AND created_at <= $4",
			ExpectArgs:    []interface{}{int64(100), int64(200), int64(300), int64(400)},
			UpdatedBefore: 100,
			UpdatedAfter:  200,
			CreatedBefore: 300,
			CreatedAfter:  400,
		},
		{
			Name: "OneField",
			In: map[string][]string{
				"field": []string{"a"},
			},
			ExpectQuery: "WHERE field IN ($1)",
			ExpectArgs:  []interface{}{"a"},
		},
		{
			Name: "OneFieldMultipleArgs",
			In: map[string][]string{
				"field": []string{"a", "b", "c"},
			},
			ExpectQuery: "WHERE field IN ($1, $2, $3)",
			ExpectArgs:  []interface{}{"a", "b", "c"},
		},
		{
			Name: "MultipleFieldsMultipleArgs",
			In: map[string][]string{
				"field1": []string{"a", "b", "c"},
				"field2": []string{"d", "e", "f"},
			},
			// TODO: we iterate a map; order is not guaranteed
			ExpectQuery: "WHERE field1 IN ($1, $2, $3) AND field2 IN ($4, $5, $6)",
			ExpectArgs:  []interface{}{"a", "b", "c", "d", "e", "f"},
		},
		{
			Name: "MultipleFieldsMultipleArgsWithTimeFilters",
			In: map[string][]string{
				"field1": []string{"a", "b", "c"},
				"field2": []string{"d", "e", "f"},
			},
			ExpectQuery:   "WHERE field1 IN ($1, $2, $3) AND field2 IN ($4, $5, $6) AND updated_at >= $7 AND updated_at <= $8 AND created_at >= $9 AND created_at <= $10",
			ExpectArgs:    []interface{}{"a", "b", "c", "d", "e", "f", int64(100), int64(200), int64(300), int64(400)},
			UpdatedBefore: 100,
			UpdatedAfter:  200,
			CreatedBefore: 300,
			CreatedAfter:  400,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			qstr, args := toSqlQuery(c.In, c.UpdatedBefore, c.UpdatedAfter, c.CreatedBefore, c.CreatedAfter)

			assert.Equal(t, c.ExpectQuery, qstr)
			assert.Equal(t, c.ExpectArgs, args)
		})
	}
}

func TestToSqlIn(t *testing.T) {
	cases := []struct {
		Name        string
		InOffset    int
		InField     string
		InArgs      []string
		ExpectQuery string
		ExpectArgs  []interface{}
	}{
		{
			Name:        "Empty",
			InOffset:    1,
			InField:     "field",
			InArgs:      []string{},
			ExpectQuery: "",
			ExpectArgs:  []interface{}{},
		},
		{
			Name:        "OneArg",
			InOffset:    3,
			InField:     "field",
			InArgs:      []string{"a"},
			ExpectQuery: "field IN ($3)",
			ExpectArgs:  []interface{}{"a"},
		},
		{
			Name:        "MultipleArgs",
			InOffset:    5,
			InField:     "field",
			InArgs:      []string{"a", "b", "c"},
			ExpectQuery: "field IN ($5, $6, $7)",
			ExpectArgs:  []interface{}{"a", "b", "c"},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			qstr, args := toSqlIn(c.InOffset, c.InField, c.InArgs)

			assert.Equal(t, c.ExpectQuery, qstr)
			assert.Equal(t, c.ExpectArgs, args)
		})
	}
}

func TestToListInterface(t *testing.T) {
	in := []string{"a", "b", "c"}

	out := toListInterface(in)

	assert.Equal(t, []interface{}{"a", "b", "c"}, out)
}

func TestToSqlTags(t *testing.T) {
	cases := []struct {
		Name        string
		InOffset    int
		InIds       []*structs.ObjectRef
		ExpectQuery string
		ExpectArgs  []interface{}
	}{
		{
			Name:        "Empty",
			InOffset:    0,
			InIds:       []*structs.ObjectRef{},
			ExpectQuery: "",
			ExpectArgs:  []interface{}{},
		},
		{
			Name:     "TwoIDs",
			InOffset: 5,
			InIds: []*structs.ObjectRef{
				&structs.ObjectRef{ID: "a", ETag: "b"},
				&structs.ObjectRef{ID: "c", ETag: "d"},
			},
			ExpectQuery: "(id=$5 AND etag=$6) OR (id=$7 AND etag=$8)",
			ExpectArgs: []interface{}{
				"a", "b",
				"c", "d",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			qstr, args := toSqlTags(c.InOffset, c.InIds)

			assert.Equal(t, c.ExpectQuery, qstr)
			assert.Equal(t, c.ExpectArgs, args)
		})
	}
}

func TestToJobSqlArgs(t *testing.T) {
	in := &structs.Job{
		JobSpec: structs.JobSpec{
			Name: "name",
		},
		ID:        "id",
		Status:    structs.RUNNING,
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
		Status:    structs.PENDING,
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
		Status:      structs.PENDING,
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
				structs.QUEUED,
				structs.RUNNING,
				structs.COMPLETED,
				structs.ERRORED,
				structs.SKIPPED,
				structs.KILLED,
			},
			Expect: []string{
				"PENDING",
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
