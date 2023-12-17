package api

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

func TestDetermineJobStatus(t *testing.T) {
	layers := []*structs.Layer{
		{LayerSpec: structs.LayerSpec{Name: "a", Priority: 10}, Status: structs.PENDING},   // 0
		{LayerSpec: structs.LayerSpec{Name: "b", Priority: 10}, Status: structs.PENDING},   // 1
		{LayerSpec: structs.LayerSpec{Name: "c", Priority: 10}, Status: structs.COMPLETED}, // 2
		{LayerSpec: structs.LayerSpec{Name: "d", Priority: 10}, Status: structs.SKIPPED},   // 3
		{LayerSpec: structs.LayerSpec{Name: "e", Priority: 10}, Status: structs.ERRORED},   // 4
		{LayerSpec: structs.LayerSpec{Name: "f", Priority: 20}, Status: structs.RUNNING},   // 5
		{LayerSpec: structs.LayerSpec{Name: "g", Priority: 20}, Status: structs.QUEUED},    // 6
		{LayerSpec: structs.LayerSpec{Name: "h", Priority: 20}, Status: structs.PENDING},   // 7
		{LayerSpec: structs.LayerSpec{Name: "i", Priority: 30}, Status: structs.PENDING},   // 8
		{LayerSpec: structs.LayerSpec{Name: "j", Priority: 30}, Status: structs.ERRORED},   // 9
	}

	cases := []struct {
		Name         string
		In           []*structs.Layer
		ExpectStatus structs.Status
		ExpectLayers []*structs.Layer
	}{
		{
			Name:         "Running",
			In:           []*structs.Layer{layers[2], layers[3], layers[5], layers[6], layers[7], layers[8], layers[9]},
			ExpectStatus: structs.RUNNING,
			ExpectLayers: []*structs.Layer{layers[6], layers[7]},
		},
		{
			Name:         "Completed",
			In:           []*structs.Layer{layers[2]},
			ExpectStatus: structs.COMPLETED,
			ExpectLayers: []*structs.Layer{},
		},
		{
			Name:         "Skipped",
			In:           []*structs.Layer{layers[2], layers[3]},
			ExpectStatus: structs.COMPLETED,
			ExpectLayers: []*structs.Layer{},
		},
		{
			Name:         "Errored-LayerErrored",
			In:           []*structs.Layer{layers[2], layers[4]},
			ExpectStatus: structs.ERRORED,
			ExpectLayers: []*structs.Layer{},
		},
		{
			Name:         "Running-LayerErrored", // we can keep going for now, but we will eventually flip to errored
			In:           []*structs.Layer{layers[0], layers[1], layers[4]},
			ExpectStatus: structs.RUNNING,
			ExpectLayers: []*structs.Layer{layers[0], layers[1]},
		},
		{
			Name:         "Running-LayerSkip",
			In:           []*structs.Layer{layers[2], layers[3], layers[5]},
			ExpectStatus: structs.RUNNING,
			ExpectLayers: []*structs.Layer{},
		},
		{
			Name:         "Running-LayerSkip-RunnableLayer",
			In:           []*structs.Layer{layers[2], layers[3], layers[5], layers[6]},
			ExpectStatus: structs.RUNNING,
			ExpectLayers: []*structs.Layer{layers[6]},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			for i := range c.In { // shuffle input, since we require function to sort
				j := rand.Intn(i + 1)
				c.In[i], c.In[j] = c.In[j], c.In[i]
			}

			status, runnable := determineJobStatus(c.In)

			assert.Equal(t, c.ExpectStatus, status)
			assert.ElementsMatch(t, c.ExpectLayers, runnable)
		})
	}
}

func TestLayerCanHaveMoreTasks(t *testing.T) {
	cases := []struct {
		In       structs.Status
		PausedAt int64
		Expect   bool
	}{
		{In: structs.PENDING, Expect: true},
		{In: structs.PENDING, PausedAt: 100, Expect: true},
		{In: structs.QUEUED, Expect: true},
		{In: structs.QUEUED, PausedAt: 100, Expect: true},
		{In: structs.RUNNING, Expect: false},
		{In: structs.RUNNING, PausedAt: 100, Expect: true},
		{In: structs.COMPLETED, Expect: false},
		{In: structs.COMPLETED, PausedAt: 100, Expect: false},
		{In: structs.ERRORED, Expect: false},
		{In: structs.ERRORED, PausedAt: 100, Expect: false},
		{In: structs.SKIPPED, Expect: false},
		{In: structs.SKIPPED, PausedAt: 100, Expect: false},
		{In: structs.KILLED, Expect: false},
		{In: structs.KILLED, PausedAt: 100, Expect: false},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s-%d", c.In, c.PausedAt), func(t *testing.T) {
			layer := &structs.Layer{
				LayerSpec: structs.LayerSpec{
					PausedAt: c.PausedAt,
				},
				Status: c.In,
			}

			assert.Equal(t, c.Expect, layerCanHaveMoreTasks(layer))
		})
	}
}

func TestValidateToggles(t *testing.T) {
	cases := []struct {
		Name      string
		In        []*structs.ObjectRef
		Expect    map[structs.Kind][]*structs.ObjectRef
		ExpectErr error
	}{
		{
			Name:      "Nil",
			In:        nil,
			Expect:    map[structs.Kind][]*structs.ObjectRef{},
			ExpectErr: nil,
		},
		{
			Name:      "Empty",
			In:        []*structs.ObjectRef{},
			Expect:    map[structs.Kind][]*structs.ObjectRef{},
			ExpectErr: nil,
		},
		{
			Name: "InvalidKind",
			In: []*structs.ObjectRef{
				{
					ID:   utils.NewID(0),
					ETag: utils.NewID(1),
					Kind: structs.Kind("test"),
				},
			},
			Expect:    nil,
			ExpectErr: fmt.Errorf("%w test", errors.ErrInvalidArg),
		},
		{
			Name: "InvalidID",
			In: []*structs.ObjectRef{
				{
					ID:   "id",
					ETag: utils.NewID(1),
					Kind: structs.KindJob,
				},
			},
			Expect:    nil,
			ExpectErr: fmt.Errorf("%w id", errors.ErrInvalidArg),
		},
		{
			Name: "InvalidETag",
			In: []*structs.ObjectRef{
				{
					ID:   utils.NewID(0),
					ETag: "etag",
					Kind: structs.KindJob,
				},
			},
			Expect:    nil,
			ExpectErr: fmt.Errorf("%w etag", errors.ErrInvalidArg),
		},
		{
			Name: "Ok",
			In: []*structs.ObjectRef{
				{ID: utils.NewID(0), ETag: utils.NewID(0), Kind: structs.KindJob},
				{ID: utils.NewID(1), ETag: utils.NewID(0), Kind: structs.KindLayer},
				{ID: utils.NewID(2), ETag: utils.NewID(0), Kind: structs.KindLayer},
				{ID: utils.NewID(3), ETag: utils.NewID(0), Kind: structs.KindTask},
				{ID: utils.NewID(4), ETag: utils.NewID(0), Kind: structs.KindTask},
				{ID: utils.NewID(5), ETag: utils.NewID(0), Kind: structs.KindTask},
			},
			Expect: map[structs.Kind][]*structs.ObjectRef{
				structs.KindJob: {
					{ID: utils.NewID(0), ETag: utils.NewID(0), Kind: structs.KindJob},
				},
				structs.KindLayer: {
					{ID: utils.NewID(1), ETag: utils.NewID(0), Kind: structs.KindLayer},
					{ID: utils.NewID(2), ETag: utils.NewID(0), Kind: structs.KindLayer},
				},
				structs.KindTask: {
					{ID: utils.NewID(3), ETag: utils.NewID(0), Kind: structs.KindTask},
					{ID: utils.NewID(4), ETag: utils.NewID(0), Kind: structs.KindTask},
					{ID: utils.NewID(5), ETag: utils.NewID(0), Kind: structs.KindTask},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			result, err := validateToggles(c.In)

			assert.Equal(t, c.ExpectErr, err)
			for k, v := range c.Expect {
				got, ok := result[k]
				assert.True(t, ok)
				assert.Equal(t, len(v), len(got))
				for i, ref := range got {
					assert.Equal(t, v[i].ID, ref.ID)
					assert.Equal(t, v[i].ETag, ref.ETag)
					assert.Equal(t, v[i].Kind, ref.Kind)
				}
			}
		})
	}
}

func TestValidateKind(t *testing.T) {
	cases := []struct {
		Name   string
		Given  structs.Kind
		Expect error
	}{
		{
			Name:   "Empty",
			Given:  structs.Kind(""),
			Expect: fmt.Errorf("%w %s", errors.ErrInvalidArg, ""),
		},
		{
			Name:   "Job",
			Given:  structs.KindJob,
			Expect: nil,
		},
		{
			Name:   "Layer",
			Given:  structs.KindLayer,
			Expect: nil,
		},
		{
			Name:   "Task",
			Given:  structs.KindTask,
			Expect: nil,
		},
		{
			Name:   "Invalid",
			Given:  structs.Kind("test"),
			Expect: fmt.Errorf("%w %s", errors.ErrInvalidArg, "test"),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			err := validateKind(c.Given)
			assert.Equal(t, c.Expect, err)
		})
	}
}

func TestBuildJob(t *testing.T) {
	cases := []struct {
		Name        string
		In          *structs.CreateJobRequest
		TotalTasks  int
		LowestPriority int64
	}{
		{
			Name: "Ok",
			In: &structs.CreateJobRequest{
				JobSpec: structs.JobSpec{
					Name: "testjob",
				},
				Layers: []structs.JobLayerRequest{
					{
						LayerSpec: structs.LayerSpec{
							Name:  "testlayer1",
							Priority: 10,
						},
						Tasks: []structs.TaskSpec{
							{
								Type:     "testz",
								Args:     []byte("test"),
								Name:     "testtask1",
								PausedAt: 0,
								Retries:  1,
							},
							{
								Type:     "testy",
								Args:     []byte("test--x"),
								Name:     "testtask2",
								PausedAt: 100,
								Retries:  10,
							},
						},
					},
					{
						LayerSpec: structs.LayerSpec{
							Name:  "testlayer2",
							Priority: 10,
						},
						Tasks: []structs.TaskSpec{
							{
								Type:     "testz",
								Args:     []byte("test"),
								Name:     "testtask3",
								PausedAt: 0,
								Retries:  0,
							},
						},
					},
					{
						LayerSpec: structs.LayerSpec{
							Name:  "testlayer3",
							Priority: 20,
						},
						Tasks: []structs.TaskSpec{
							{
								Type: "testz",
							},
							{
								Type: "testy",
							},
							{
								Type: "testx",
							},
						},
					},
					{
						LayerSpec: structs.LayerSpec{
							Name:  "testlayer4",
							Priority: 30,
						},
						Tasks: []structs.TaskSpec{},
					},
				},
			},
			TotalTasks:  6,
			LowestPriority: 10,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			resJob, resLayers, resTasks, resTaskMap := buildJob(c.In)

			seenLayers := map[string]bool{}
			seenTasks := map[string]bool{}
			etag := resJob.ETag

			assert.Equal(t, c.In.Name, resJob.Name)
			assert.Equal(t, len(c.In.Layers), len(resLayers))
			assert.NotEqual(t, "", resJob.ID)
			assert.NotEqual(t, "", resJob.ETag)
			for i, l := range resLayers {
				assert.NotEqual(t, "", l.ID)
				assert.NotContains(t, seenLayers, l.ID)
				assert.Equal(t, l.ETag, etag)
				assert.Equal(t, l.JobID, resJob.ID)
				assert.Equal(t, l.Name, c.In.Layers[i].Name)
				assert.Equal(t, l.PausedAt, c.In.Layers[i].PausedAt)
				assert.Equal(t, l.Priority, c.In.Layers[i].Priority)
				if l.Priority == c.LowestPriority {
					assert.Equal(t, l.Status, structs.RUNNING)
				} else {
					assert.Equal(t, l.Status, structs.PENDING)
				}
				seenLayers[l.ID] = true

				ltasks, ok := resTaskMap[l.ID]
				if ok {
					for j, k := range ltasks {
						assert.Equal(t, l.JobID, k.JobID)
						assert.Equal(t, l.ID, k.LayerID)
						assert.NotEqual(t, "", k.ID)
						assert.Equal(t, k.ETag, etag)
						assert.NotContains(t, seenTasks, k.ID)
						assert.Equal(t, k.Type, c.In.Layers[i].Tasks[j].Type)
						assert.Equal(t, k.Args, c.In.Layers[i].Tasks[j].Args)
						assert.Equal(t, k.Name, c.In.Layers[i].Tasks[j].Name)
						assert.Equal(t, k.PausedAt, c.In.Layers[i].Tasks[j].PausedAt)
						assert.Equal(t, k.Retries, c.In.Layers[i].Tasks[j].Retries)
						assert.Equal(t, k.Status, structs.PENDING)
						seenTasks[k.ID] = true
					}
				} else {
					assert.Nil(t, c.In.Layers[i].Tasks)
				}
			}
			assert.Equal(t, c.TotalTasks, len(resTasks))
		})
	}
}

func TestValidateTaskSpec(t *testing.T) {
	cases := []struct {
		Name   string
		In     *structs.TaskSpec
		Expect error
	}{
		{
			Name: "Ok",
			In: &structs.TaskSpec{
				Name:    "test",
				Type:    "test",
				Args:    []byte("test"),
				Retries: 1,
			},
			Expect: nil,
		},
		{
			Name: "TaskNameExceedsMax",
			In: &structs.TaskSpec{
				Type: "test",
				Name: strings.Repeat("a", maxNameLength+1),
			},
			Expect: fmt.Errorf(
				"%w task name %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				strings.Repeat("a", maxNameLength+1),
				maxNameLength+1,
				maxNameLength,
			),
		},
		{
			Name:   "TaskTypeEmpty",
			In:     &structs.TaskSpec{},
			Expect: errors.ErrNoTaskType,
		},
		{
			Name: "TaskTypeExceedsMax",
			In:   &structs.TaskSpec{Name: "test", Type: strings.Repeat("a", maxNameLength+1)},
			Expect: fmt.Errorf(
				"%w task type %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				strings.Repeat("a", maxNameLength+1),
				maxNameLength+1,
				maxNameLength,
			),
		},
		{
			Name: "TaskArgsExceedsMax",
			In:   &structs.TaskSpec{Name: "test", Type: "test", Args: bytes.Repeat([]byte("a"), maxArgsLength+1)},
			Expect: fmt.Errorf(
				"%w task args %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				bytes.Repeat([]byte("a"), maxArgsLength+1),
				maxArgsLength+1,
				maxArgsLength,
			),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			err := validateTaskSpec(c.In)
			assert.Equal(t, c.Expect, err)
		})
	}
}

func TestValidateJobRequest(t *testing.T) {
	cases := []struct {
		Name   string
		In     *structs.CreateJobRequest
		Expect error
	}{
		{
			Name: "Ok",
			In: &structs.CreateJobRequest{
				JobSpec: structs.JobSpec{
					Name: "test",
				},
				Layers: []structs.JobLayerRequest{
					{
						LayerSpec: structs.LayerSpec{Name: "test", Priority: 10},
						Tasks: []structs.TaskSpec{
							{Name: "test", Type: "test", Args: []byte("test"), Retries: 1},
						},
					},
				},
			},
			Expect: nil,
		},
		{
			Name: "NoLayers",
			In: &structs.CreateJobRequest{
				JobSpec: structs.JobSpec{
					Name: "test",
				},
				Layers: []structs.JobLayerRequest{},
			},
			Expect: errors.ErrNoLayers,
		},
		{
			Name: "NilLayers",
			In: &structs.CreateJobRequest{
				JobSpec: structs.JobSpec{
					Name: "test",
				},
				Layers: nil,
			},
			Expect: errors.ErrNoLayers,
		},
		{
			Name: "JobNameExceedsMax",
			In: &structs.CreateJobRequest{
				JobSpec: structs.JobSpec{
					Name: strings.Repeat("a", maxNameLength+1),
				},
				Layers: []structs.JobLayerRequest{
					{
						LayerSpec: structs.LayerSpec{Name: "test"},
					},
				},
			},
			Expect: fmt.Errorf(
				"%w job name %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				strings.Repeat("a", maxNameLength+1),
				maxNameLength+1,
				maxNameLength,
			),
		},
		{
			Name: "LayerNameExceedsMax",
			In: &structs.CreateJobRequest{
				Layers: []structs.JobLayerRequest{
					{
						LayerSpec: structs.LayerSpec{Name: strings.Repeat("a", maxNameLength+1)},
					},
				},
			},
			Expect: fmt.Errorf(
				"%w layer name %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				strings.Repeat("a", maxNameLength+1),
				maxNameLength+1,
				maxNameLength,
			),
		},
		{
			Name: "TaskNameExceedsMax",
			In: &structs.CreateJobRequest{
				Layers: []structs.JobLayerRequest{
					{
						LayerSpec: structs.LayerSpec{Name: "test"},
						Tasks: []structs.TaskSpec{
							{
								Type: "test",
								Name: strings.Repeat("a", maxNameLength+1),
							},
						},
					},
				},
			},
			Expect: fmt.Errorf(
				"%w task name %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				strings.Repeat("a", maxNameLength+1),
				maxNameLength+1,
				maxNameLength,
			),
		},
		{
			Name: "TaskTypeEmpty",
			In: &structs.CreateJobRequest{
				Layers: []structs.JobLayerRequest{
					{
						Tasks: []structs.TaskSpec{{}},
					},
				},
			},
			Expect: errors.ErrNoTaskType,
		},
		{
			Name: "TaskTypeExceedsMax",
			In: &structs.CreateJobRequest{
				Layers: []structs.JobLayerRequest{
					{
						Tasks: []structs.TaskSpec{
							{Name: "test", Type: strings.Repeat("a", maxNameLength+1)},
						},
					},
				},
			},
			Expect: fmt.Errorf(
				"%w task type %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				strings.Repeat("a", maxNameLength+1),
				maxNameLength+1,
				maxNameLength,
			),
		},
		{
			Name: "TaskArgsExceedsMax",
			In: &structs.CreateJobRequest{
				Layers: []structs.JobLayerRequest{
					{
						Tasks: []structs.TaskSpec{
							{Name: "test", Type: "test", Args: bytes.Repeat([]byte("a"), maxArgsLength+1)},
						},
					},
				},
			},
			Expect: fmt.Errorf(
				"%w task args %s is %d chars, max %d",
				errors.ErrMaxExceeded,
				bytes.Repeat([]byte("a"), maxArgsLength+1),
				maxArgsLength+1,
				maxArgsLength,
			),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			err := validateCreateJobRequest(c.In)
			assert.Equal(t, c.Expect, err)
		})
	}
}
