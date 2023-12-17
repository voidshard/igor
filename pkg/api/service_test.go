package api

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	"github.com/voidshard/igor/internal/mocks/pkg/database_mock"
	"github.com/voidshard/igor/internal/mocks/pkg/queue_mock"
	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/database/changes"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/queue"
	"github.com/voidshard/igor/pkg/structs"
)

func init() {
	defLimit = 2
	timeNow = func() int64 { return 1000000 }
}

func TestClose(t *testing.T) {
	qu := queue_mock.NewMockQueue(gomock.NewController(t))
	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{qu: qu, db: db}

	qu.EXPECT().Close().Return(nil)
	db.EXPECT().Close().Return(nil)

	err := svc.Close()

	assert.Nil(t, err)
}

func TestRegister(t *testing.T) {
	qu := queue_mock.NewMockQueue(gomock.NewController(t))
	svc := &Service{qu: qu}

	name := "test-task"
	hnd := func(w []*queue.Meta) error { return nil }

	qu.EXPECT().Register(name, gomock.Any()).Return(nil)

	err := svc.Register(name, hnd)

	assert.Nil(t, err)
}

func TestRun(t *testing.T) {
	qu := queue_mock.NewMockQueue(gomock.NewController(t))
	svc := &Service{qu: qu}

	qu.EXPECT().Run().Return(nil)

	svc.Run()
}

func TestTogglePauseLayer(t *testing.T) {
	id0 := utils.NewID(0)
	in := &structs.ObjectRef{Kind: structs.KindLayer, ID: id0, ETag: id0}

	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}

	timestep := int64(123)

	db.EXPECT().SetLayersPaused(timestep, gomock.Any(), []*structs.ObjectRef{in}).Return(int64(1), nil)

	count, err := svc.togglePause(timestep, []*structs.ObjectRef{in})

	assert.Nil(t, err)
	assert.Equal(t, int64(1), count)
}

func TestTogglePauseTask(t *testing.T) {
	id0 := utils.NewID(0)
	in := &structs.ObjectRef{Kind: structs.KindTask, ID: id0, ETag: id0}

	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}

	timestep := int64(123)

	db.EXPECT().SetTasksPaused(timestep, gomock.Any(), []*structs.ObjectRef{in}).Return(int64(1), nil)

	count, err := svc.togglePause(timestep, []*structs.ObjectRef{in})

	assert.Nil(t, err)
	assert.Equal(t, int64(1), count)
}

func TestRetryTasksSkipTaskOnETagMismatch(t *testing.T) {
	id0 := utils.NewID(0)
	in := &structs.ObjectRef{Kind: structs.KindTask, ID: id0, ETag: id0}
	task := &structs.Task{ID: id0, ETag: "e1", Status: structs.ERRORED}

	db := database_mock.NewMockDatabase(gomock.NewController(t))
	qu := queue_mock.NewMockQueue(gomock.NewController(t))
	svc := &Service{db: db, qu: qu}

	db.EXPECT().Tasks(&structs.Query{Limit: 1, TaskIDs: []string{id0}}).Return([]*structs.Task{task}, nil)
	// we don't call any other db / queue funcs as the task etag doesn't match the given one

	count, err := svc.retryTasks([]*structs.ObjectRef{in})

	assert.Nil(t, err)
	assert.Equal(t, int64(0), count)
}

func TestRetryTasks(t *testing.T) {
	id0 := utils.NewID(0)

	cases := []struct {
		Name string
		In   *structs.ObjectRef
		Task *structs.Task
	}{
		{
			Name: "RetryTask",
			In:   &structs.ObjectRef{Kind: structs.KindTask, ID: id0, ETag: id0},
			Task: &structs.Task{ID: id0, ETag: id0, Status: structs.ERRORED},
		},
		{
			Name: "RetryAndKillTask",
			In:   &structs.ObjectRef{Kind: structs.KindTask, ID: id0, ETag: id0},
			Task: &structs.Task{ID: id0, ETag: id0, Status: structs.RUNNING, QueueTaskID: "old-queued-task-id"},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			qu := queue_mock.NewMockQueue(gomock.NewController(t))
			svc := &Service{db: db, qu: qu, errs: make(chan error)}

			var internalErr error

			db.EXPECT().Tasks(&structs.Query{Limit: 1, TaskIDs: []string{id0}}).Return([]*structs.Task{c.Task}, nil)
			if c.Task.QueueTaskID != "" {
				go func() {
					internalErr = <-svc.errs
				}()
				qu.EXPECT().Kill(c.Task.QueueTaskID).Return(nil)
				db.EXPECT().SetTaskQueueID(c.Task.ID, c.Task.ETag, gomock.Any(), "", structs.ERRORED).Return(nil)
			}
			qu.EXPECT().Enqueue(c.Task).Return("queue-task-id", nil)
			if c.Task.QueueTaskID == "" {
				db.EXPECT().SetTaskQueueID(c.Task.ID, c.Task.ETag, gomock.Any(), "queue-task-id", structs.QUEUED).Return(nil)
			} else {
				db.EXPECT().SetTaskQueueID(c.Task.ID, gomock.Any(), gomock.Any(), "queue-task-id", structs.QUEUED).Return(nil)
			}

			count, err := svc.retryTasks([]*structs.ObjectRef{c.In})

			assert.Nil(t, internalErr)
			assert.Nil(t, err)
			assert.Equal(t, int64(1), count)
		})
	}
}

func TestSkipLayers(t *testing.T) {
	id0 := utils.NewID(0)
	id1 := utils.NewID(1)

	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}

	in := []*structs.ObjectRef{
		{Kind: structs.KindLayer, ID: id0, ETag: id0},
		{Kind: structs.KindLayer, ID: id1, ETag: id0},
	}

	db.EXPECT().SetLayersStatus(structs.SKIPPED, gomock.Any(), in).Return(int64(2), nil)

	count, err := svc.Skip(in)

	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)
}

func TestSkipTasks(t *testing.T) {
	id0 := utils.NewID(0)
	id1 := utils.NewID(1)

	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}

	in := []*structs.ObjectRef{
		{Kind: structs.KindTask, ID: id0, ETag: id0},
		{Kind: structs.KindTask, ID: id1, ETag: id0},
	}

	db.EXPECT().SetTasksStatus(structs.SKIPPED, gomock.Any(), in).Return(int64(2), nil)

	count, err := svc.Skip(in)

	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)
}

func TestKillTasks(t *testing.T) {
	id0 := utils.NewID(0)
	id1 := utils.NewID(1)

	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}

	in := []*structs.ObjectRef{
		{Kind: structs.KindTask, ID: id0, ETag: id0},
		{Kind: structs.KindTask, ID: id1, ETag: id0},
	}

	db.EXPECT().SetTasksStatus(structs.KILLED, gomock.Any(), in).Return(int64(2), nil)

	count, err := svc.Kill(in)

	assert.Nil(t, err)
	assert.Equal(t, int64(2), count)
}

func TestJobs(t *testing.T) {
	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}
	q := &structs.Query{Limit: 10}

	db.EXPECT().Jobs(q).Return([]*structs.Job{}, nil)

	svc.Jobs(q)
}

func TestLayers(t *testing.T) {
	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}
	q := &structs.Query{Limit: 10}

	db.EXPECT().Layers(q).Return([]*structs.Layer{}, nil)

	svc.Layers(q)
}

func TestTasks(t *testing.T) {
	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db}
	q := &structs.Query{Limit: 10}

	db.EXPECT().Tasks(q).Return([]*structs.Task{}, nil)

	svc.Tasks(q)
}

func TestCreateTasksRejectsInvalidIDs(t *testing.T) {
	svc := &Service{}
	_, err := svc.CreateTasks([]*structs.CreateTaskRequest{
		{LayerID: "x", TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
	})

	assert.ErrorIs(t, err, errors.ErrInvalidArg)
}

func TestCreateTasks(t *testing.T) {
	id0 := utils.NewID(0)
	id1 := utils.NewID(1)
	id2 := utils.NewID(2)

	cases := []struct {
		Name            string
		In              []*structs.CreateTaskRequest
		Layers          []*structs.Layer
		ExpectCreateJob bool
	}{
		{
			Name: "SingleTask_LayerID",
			In: []*structs.CreateTaskRequest{
				{LayerID: id0, TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
			},
			Layers: []*structs.Layer{
				{ID: id0, ETag: "e1", Status: structs.PENDING},
			},
		},
		{
			Name: "SingleTask_NoLayerID",
			In: []*structs.CreateTaskRequest{
				{TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
			},
			Layers:          []*structs.Layer{},
			ExpectCreateJob: true,
		},
		{
			Name: "TasksWithAndWithoutLayerID",
			In: []*structs.CreateTaskRequest{
				{LayerID: id0, TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
				{TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
			},
			Layers: []*structs.Layer{
				{ID: id0, ETag: "e1", Status: structs.PENDING},
			},
			ExpectCreateJob: true,
		},
		{
			Name: "MultipleTasksWithLayerID",
			In: []*structs.CreateTaskRequest{
				{LayerID: id0, TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
				{LayerID: id1, TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
				{LayerID: id2, TaskSpec: structs.TaskSpec{Name: "test-task", Type: "test-type"}},
			},
			Layers: []*structs.Layer{
				{ID: id0, ETag: "e1", Status: structs.PENDING},
				{ID: id1, ETag: "e2", Status: structs.PENDING},
				{ID: id2, ETag: "e3", Status: structs.PENDING},
			},
			ExpectCreateJob: false,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db}

			if len(c.Layers) > 0 {
				ids := []string{}
				for _, task := range c.In {
					if task.LayerID != "" {
						ids = append(ids, task.LayerID)
					}
				}
				db.EXPECT().Layers(&structs.Query{Limit: len(ids), LayerIDs: ids}).Return(c.Layers, nil)
			}

			if c.ExpectCreateJob {
				db.EXPECT().InsertJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
			}
			db.EXPECT().InsertTasks(gomock.Any()).Return(nil)

			result, err := svc.CreateTasks(c.In)

			assert.Nil(t, err)
			assert.Equal(t, len(c.In), len(result))
		})
	}
}

func TestCreateTasksEmptyNil(t *testing.T) {
	cases := []struct {
		Name string
		In   []*structs.CreateTaskRequest
	}{
		{
			Name: "Nil",
			In:   nil,
		},
		{
			Name: "Empty",
			In:   []*structs.CreateTaskRequest{},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db}

			_, err := svc.CreateTasks(c.In)

			assert.ErrorIs(t, err, errors.ErrNoTasks)
		})
	}
}

func TestCreateJob(t *testing.T) {
	cases := []struct {
		Name string
		In   *structs.CreateJobRequest
	}{
		{
			"CreateJob",
			&structs.CreateJobRequest{
				JobSpec: structs.JobSpec{
					Name: "test-job",
				},
				Layers: []structs.JobLayerRequest{
					{
						LayerSpec: structs.LayerSpec{Name: "a", Priority: 10},
						Tasks: []structs.TaskSpec{
							{Name: "t1", Type: "foo"},
							{Name: "t2", Type: "bar"},
						},
					},
					{LayerSpec: structs.LayerSpec{Name: "b", Priority: 10}},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db}

			db.EXPECT().InsertJob(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

			resp, err := svc.CreateJob(c.In)

			assert.Nil(t, err)
			assert.NotNil(t, resp)

			assert.Equal(t, c.In.Name, resp.Name)
			assert.Equal(t, len(c.In.Layers), len(resp.Layers))
			for i, l := range c.In.Layers {
				assert.Equal(t, l.Name, resp.Layers[i].Name)
				assert.Equal(t, l.Priority, resp.Layers[i].Priority)
				assert.Equal(t, len(l.Tasks), len(resp.Layers[i].Tasks))
			}
		})
	}
}

func TestEnqueueTasks(t *testing.T) {
	cases := []struct {
		Name       string
		In         *structs.Task
		ErrOnQueue error
		ErrOnSet   error
	}{
		{
			Name:       "TaskQueued",
			In:         &structs.Task{ID: "xxt1", ETag: "e1", Status: structs.PENDING},
			ErrOnQueue: nil,
			ErrOnSet:   nil,
		},
		{
			Name:       "TaskQueued_Paused",
			In:         &structs.Task{ID: "xxt1", ETag: "e1", Status: structs.PENDING, TaskSpec: structs.TaskSpec{PausedAt: 100}},
			ErrOnQueue: nil,
			ErrOnSet:   nil,
		},
		{
			Name:       "Task_FailOnQueue",
			In:         &structs.Task{ID: "xxt1", ETag: "e1", Status: structs.PENDING},
			ErrOnQueue: fmt.Errorf("fail"),
			ErrOnSet:   nil,
		},
		{
			Name:       "Task_FailOnSet",
			In:         &structs.Task{ID: "xxt1", ETag: "e1", Status: structs.PENDING},
			ErrOnQueue: nil,
			ErrOnSet:   fmt.Errorf("fail"),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			qu := queue_mock.NewMockQueue(gomock.NewController(t))
			svc := &Service{db: db, qu: qu}

			if c.In.PausedAt > 0 {
				return
			}

			qu.EXPECT().Enqueue(c.In).Return("queue-task-id", c.ErrOnQueue)
			if c.ErrOnQueue != nil {
				db.EXPECT().SetTasksStatus(structs.ERRORED, gomock.Any(), []*structs.ObjectRef{{ID: "xxt1", ETag: "e1"}}, c.ErrOnQueue.Error()).Return(int64(1), nil)
			} else {
				db.EXPECT().SetTaskQueueID(c.In.ID, c.In.ETag, gomock.Any(), "queue-task-id", structs.QUEUED).Return(c.ErrOnSet)
				if c.ErrOnSet != nil {
					qu.EXPECT().Kill("queue-task-id").Return(nil)
					db.EXPECT().SetTasksStatus(structs.ERRORED, gomock.Any(), []*structs.ObjectRef{{ID: "xxt1", ETag: "e1"}}, c.ErrOnSet.Error()).Return(int64(1), nil)
				}
			}

			err := svc.enqueueTasks([]*structs.Task{c.In})

			if c.ErrOnQueue == nil && c.ErrOnSet == nil {
				// ie. if nothing went wrong
				assert.Equal(t, structs.QUEUED, c.In.Status)
				assert.Equal(t, "queue-task-id", c.In.QueueTaskID)
				assert.NotEqual(t, "e1", c.In.ETag)
				assert.Nil(t, err)
			}
		})
	}
}

func TestHandleLayerEventFinalState(t *testing.T) {
	layer := &structs.Layer{ID: "l1", ETag: "e1", JobID: "j1"}
	states := append(incompleteStates, structs.ERRORED, structs.KILLED)
	layers := []*structs.Layer{
		{LayerSpec: structs.LayerSpec{Name: "a", Priority: 10}, Status: structs.PENDING}, // 0
		{LayerSpec: structs.LayerSpec{Name: "b", Priority: 10}, Status: structs.SKIPPED}, // 1
		{LayerSpec: structs.LayerSpec{Name: "c", Priority: 10}, Status: structs.ERRORED}, // 2
		{LayerSpec: structs.LayerSpec{Name: "d", Priority: 10}, Status: structs.RUNNING}, // 3
	}

	cases := []struct {
		Name        string
		LayerStatus structs.Status   // set on layer
		Layers      []*structs.Layer // other layers in the job
		RunLayers   []*structs.Layer // layers that should be run
		JobStatus   structs.Status   // set on job
	}{
		{
			Name:        "LayerComplete_NoOtherLayers",
			LayerStatus: structs.COMPLETED,
			Layers:      []*structs.Layer{},
			RunLayers:   []*structs.Layer{},
			JobStatus:   structs.COMPLETED,
		},
		{
			Name:        "LayerSkipped_NoOtherLayers",
			LayerStatus: structs.SKIPPED,
			Layers:      []*structs.Layer{},
			RunLayers:   []*structs.Layer{},
			JobStatus:   structs.COMPLETED,
		},
		{
			Name:        "LayerErrored_NoOtherLayers",
			LayerStatus: structs.ERRORED,
			Layers:      []*structs.Layer{},
			RunLayers:   []*structs.Layer{},
			JobStatus:   structs.ERRORED,
		},
		{
			Name:        "LayerComplete_OtherLayerPending",
			LayerStatus: structs.COMPLETED,
			Layers:      []*structs.Layer{layers[0]},
			RunLayers:   []*structs.Layer{layers[0]},
			JobStatus:   structs.RUNNING,
		},
		{
			Name:        "LayerComplete_OtherLayerRunning",
			LayerStatus: structs.COMPLETED,
			Layers:      []*structs.Layer{layers[3]},
			RunLayers:   []*structs.Layer{},
			JobStatus:   structs.RUNNING,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			layer.Status = c.LayerStatus

			db := database_mock.NewMockDatabase(gomock.NewController(t))
			qu := queue_mock.NewMockQueue(gomock.NewController(t))
			svc := &Service{db: db, qu: qu, errs: make(chan error, 1)}

			var internalErr error

			if c.LayerStatus == structs.SKIPPED {
				go func() {
					internalErr = <-svc.errs
				}()

				// we look up incomplete tasks for skipping
				db.EXPECT().Tasks(&structs.Query{
					Statuses: incompleteStates,
					Limit:    defLimit,
					LayerIDs: []string{layer.ID},
				})
			}

			db.EXPECT().Layers(&structs.Query{
				Limit:    defLimit,
				JobIDs:   []string{layer.JobID},
				Statuses: states,
			}).Return(append(c.Layers, layer), nil)
			if len(c.Layers)+1 >= defLimit {
				// because we shim in another entry
				db.EXPECT().Layers(&structs.Query{
					Limit:    defLimit,
					Offset:   defLimit,
					JobIDs:   []string{layer.JobID},
					Statuses: states,
				}).Return([]*structs.Layer{}, nil)
			}

			if structs.IsFinalStatus(c.JobStatus) {
				db.EXPECT().Jobs(&structs.Query{Limit: 1, JobIDs: []string{"j1"}}).Return([]*structs.Job{{ID: "j1", ETag: "x1", Status: structs.RUNNING}}, nil)
				db.EXPECT().SetJobsStatus(c.JobStatus, gomock.Any(), []*structs.ObjectRef{{ID: "j1", ETag: "x1"}}).Return(int64(1), nil)
			}

			if len(c.RunLayers) > 0 {
				ids := []*structs.ObjectRef{}
				for _, l := range c.RunLayers {
					ids = append(ids, &structs.ObjectRef{ID: l.ID, ETag: l.ETag})
				}
				db.EXPECT().SetLayersStatus(structs.RUNNING, gomock.Any(), ids).Return(int64(len(ids)), nil)
			}

			err := svc.handleLayerEvent(&changes.Change{Old: layer, New: layer})

			assert.Nil(t, err)
			assert.Nil(t, internalErr)
		})
	}
}

func TestHandleLayerEventPaused(t *testing.T) {
	layer := &structs.Layer{ID: "l1", ETag: "e1", Status: structs.PENDING, LayerSpec: structs.LayerSpec{PausedAt: 100}}

	cases := []struct {
		Name   string
		Status structs.Status
	}{
		{
			Name:   "Pending",
			Status: structs.PENDING,
		},
		{
			Name:   "Running",
			Status: structs.RUNNING,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			qu := queue_mock.NewMockQueue(gomock.NewController(t))
			svc := &Service{db: db, qu: qu}

			layer.Status = c.Status

			err := svc.handleLayerEvent(&changes.Change{Old: layer, New: layer})

			// nb. if the db or queue are called, this will fail
			// so we're essentially asserting neither of these are used when a layer is paused

			assert.Nil(t, err)
		})
	}
}

func TestHandleLayerEventRunning(t *testing.T) {
	layer := &structs.Layer{ID: "l1", ETag: "e1", Status: structs.RUNNING}
	states := append(incompleteStates, structs.ERRORED, structs.KILLED)

	cases := []struct {
		Name    string
		Pending []*structs.Task
		Other   []*structs.Task
		Expect  structs.Status
	}{
		{
			Name: "Pending",
			Pending: []*structs.Task{
				{ID: "t1", ETag: "e1", LayerID: "l1", Status: structs.PENDING},
			},
			Other:  []*structs.Task{},
			Expect: structs.RUNNING,
		},
		{
			Name:    "Running",
			Pending: []*structs.Task{},
			Other: []*structs.Task{
				{ID: "t1", ETag: "e1", LayerID: "l1", Status: structs.RUNNING},
			},
			Expect: structs.RUNNING,
		},
		{
			Name:    "Completed",
			Pending: []*structs.Task{},
			Other: []*structs.Task{
				{ID: "t1", ETag: "e1", LayerID: "l1", Status: structs.COMPLETED},
			},
			Expect: structs.COMPLETED,
		},
	}

	for _, c := range cases {
		db := database_mock.NewMockDatabase(gomock.NewController(t))
		qu := queue_mock.NewMockQueue(gomock.NewController(t))
		svc := &Service{db: db, qu: qu}

		// enqueue layer tasks will look up pending tasks
		db.EXPECT().Tasks(&structs.Query{Limit: defLimit, LayerIDs: []string{"l1"}, Statuses: []structs.Status{structs.PENDING}}).Return(c.Pending, nil)

		// pending tasks are enqueued
		if len(c.Pending) > 0 {
			for _, task := range c.Pending {
				qu.EXPECT().Enqueue(task).Return("queue-task-id", nil)
				db.EXPECT().SetTaskQueueID(task.ID, task.ETag, gomock.Any(), "queue-task-id", structs.QUEUED).Return(nil)
			}
		} else {
			// if no pending tasks, we call determineLayerStatus & need to look up tasks of various statuses
			db.EXPECT().Tasks(&structs.Query{Limit: defLimit, LayerIDs: []string{"l1"}, Statuses: states}).Return(c.Other, nil)

			// finally, if we need to update the layer status, we do so
			if c.Expect != layer.Status {
				db.EXPECT().SetLayersStatus(c.Expect, gomock.Any(), []*structs.ObjectRef{{ID: "l1", ETag: "e1"}}).Return(int64(1), nil)
			}
		}

		err := svc.handleLayerEvent(&changes.Change{Old: layer, New: layer})

		assert.Nil(t, err)
	}
}

func TestHandleLayerEventDeleted(t *testing.T) {
	layer := &structs.Layer{ID: "l1", ETag: "e1"}
	ch := &changes.Change{Old: layer, New: nil}
	svc := &Service{}

	err := svc.handleLayerEvent(ch)

	assert.Nil(t, err)
}

func TestEnqueueLayerTasks(t *testing.T) {
	layerID := "l1"

	cases := []struct {
		Name    string
		Queries []*structs.Query
		Tasks   [][]*structs.Task
	}{
		{
			Name: "SingleQuery",
			Queries: []*structs.Query{
				{Limit: defLimit, LayerIDs: []string{layerID}, Statuses: []structs.Status{structs.PENDING}},
			},
			Tasks: [][]*structs.Task{
				{&structs.Task{ID: "t1", ETag: "e1", LayerID: layerID}},
			},
		},
		{
			Name: "MultipleQueries",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: []structs.Status{structs.PENDING}},
				{Offset: 2, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: []structs.Status{structs.PENDING}},
				{Offset: 4, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: []structs.Status{structs.PENDING}},
			},
			Tasks: [][]*structs.Task{
				{&structs.Task{ID: "t1", ETag: "e1", LayerID: layerID}, &structs.Task{ID: "t2", ETag: "e2", LayerID: layerID}},
				{&structs.Task{ID: "t3", ETag: "e3", LayerID: layerID}, &structs.Task{ID: "t4", ETag: "e4", LayerID: layerID}},
				{},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			qu := queue_mock.NewMockQueue(gomock.NewController(t))
			svc := &Service{db: db, qu: qu}

			tasks := 0
			for i := 0; i < len(c.Queries); i++ {
				db.EXPECT().Tasks(c.Queries[i]).Return(c.Tasks[i], nil)
				for _, task := range c.Tasks[i] {
					tasks++
					qu.EXPECT().Enqueue(task).Return("queue-task-id", nil)
					db.EXPECT().SetTaskQueueID(task.ID, task.ETag, gomock.Any(), "queue-task-id", structs.QUEUED).Return(nil)
				}
			}

			total, err := svc.enqueueLayerTasks(layerID)

			assert.Nil(t, err)
			assert.Equal(t, tasks, total)
		})
	}
}

func TestDetermineLayerStatus(t *testing.T) {
	states := append(incompleteStates, structs.ERRORED, structs.KILLED)
	layerID := "l1"
	tasks := []*structs.Task{
		{Status: structs.PENDING},   // 0
		{Status: structs.QUEUED},    // 1
		{Status: structs.RUNNING},   // 2
		{Status: structs.COMPLETED}, // 3
		{Status: structs.SKIPPED},   // 4
		{Status: structs.ERRORED},   // 5
		{Status: structs.KILLED},    // 6
	}

	cases := []struct {
		Name    string
		Queries []*structs.Query
		Tasks   [][]*structs.Task
		Expect  structs.Status
	}{
		{
			Name: "Running-BreakOnQueued",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[0], tasks[1]}},
			Expect: structs.RUNNING,
		},
		{
			Name: "Running-BreakOnRunning",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[0], tasks[2]}},
			Expect: structs.RUNNING,
		},
		{
			Name: "Pending",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[0]}},
			Expect: structs.PENDING,
		},
		{
			Name: "Errored-TaskErrored",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
				{Offset: 2, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[3], tasks[5]}, {}},
			Expect: structs.ERRORED,
		},
		{
			Name: "Errored-TaskKilled",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
				{Offset: 2, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[3], tasks[6]}, {}},
			Expect: structs.ERRORED,
		},
		{
			Name: "Completed-AllCompleted",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
				{Offset: 2, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[3], tasks[4]}, {}},
			Expect: structs.COMPLETED,
		},
		{
			Name: "Completed-OneSkipped",
			Queries: []*structs.Query{
				{Offset: 0, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
				{Offset: 2, Limit: defLimit, LayerIDs: []string{layerID}, Statuses: states},
			},
			Tasks:  [][]*structs.Task{{tasks[3], tasks[4]}, {}},
			Expect: structs.COMPLETED,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db}

			for i := 0; i < len(c.Queries); i++ {
				db.EXPECT().Tasks(c.Queries[i]).Return(c.Tasks[i], nil)
			}

			status, err := svc.determineLayerStatus(layerID)

			assert.Nil(t, err)
			assert.Equal(t, c.Expect, status)
		})
	}
}

func TestSvcDetermineJobStatus(t *testing.T) {
	layers := []*structs.Layer{
		{LayerSpec: structs.LayerSpec{Name: "a", Priority: 10}, Status: structs.PENDING},   // 0
		{LayerSpec: structs.LayerSpec{Name: "b", Priority: 10}, Status: structs.COMPLETED}, // 1
		{LayerSpec: structs.LayerSpec{Name: "c", Priority: 10}, Status: structs.SKIPPED},   // 2
		{LayerSpec: structs.LayerSpec{Name: "d", Priority: 10}, Status: structs.ERRORED},   // 3
	}
	jid := "j1"
	states := append(incompleteStates, structs.ERRORED, structs.KILLED)

	cases := []struct {
		Name         string
		Layers       []*structs.Layer
		ExpQueries   []*structs.Query
		ExpJobStatus structs.Status
		ExpRunLayers []*structs.Layer
	}{
		{
			Name:   "SingleQuery",
			Layers: []*structs.Layer{layers[0]},
			ExpQueries: []*structs.Query{
				{JobIDs: []string{"j1"}, Limit: defLimit, Statuses: states},
			},
			ExpJobStatus: structs.RUNNING,
			ExpRunLayers: []*structs.Layer{layers[0]},
		},
		{
			Name: "MultipleQueries",
			Layers: []*structs.Layer{
				layers[1], layers[2], layers[3],
			},
			ExpQueries: []*structs.Query{
				{JobIDs: []string{"j1"}, Limit: defLimit, Statuses: states},
				{JobIDs: []string{"j1"}, Limit: defLimit, Statuses: states, Offset: 2},
			},
			ExpJobStatus: structs.ERRORED,
			ExpRunLayers: []*structs.Layer{},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db}

			for i := 0; i < len(c.Layers); i += defLimit {
				var res []*structs.Layer
				if i+defLimit > len(c.Layers) {
					res = c.Layers[i:]
				} else {
					res = c.Layers[i : i+defLimit]
				}
				db.EXPECT().Layers(c.ExpQueries[i/defLimit]).Return(res, nil)
			}

			status, runLayers, err := svc.determineJobStatus(jid)

			assert.Nil(t, err)
			assert.Equal(t, c.ExpJobStatus, status)
			assert.ElementsMatch(t, c.ExpRunLayers, runLayers)
		})
	}
}

func TestKillTask(t *testing.T) {
	in := &structs.Task{ID: "t1", ETag: "e1", QueueTaskID: "q1"}

	qu := queue_mock.NewMockQueue(gomock.NewController(t))
	db := database_mock.NewMockDatabase(gomock.NewController(t))
	svc := &Service{db: db, qu: qu}

	qu.EXPECT().Kill("q1").Return(nil)
	db.EXPECT().SetTaskQueueID("t1", "e1", gomock.Any(), "", structs.ERRORED).Return(nil)

	err := svc.killTask(in)

	assert.Nil(t, err)
	assert.Equal(t, structs.ERRORED, in.Status)
	assert.Equal(t, "", in.QueueTaskID)
	assert.NotEqual(t, "e1", in.ETag)
}

func TestHandleTaskEventUpdatedFinalState(t *testing.T) {
	tag := "etag"

	cases := []struct {
		Name                 string
		Task                 *structs.Task
		Layer                *structs.Layer
		Tasks                []*structs.Task // other tasks in the layer
		ExpectKillTask       bool
		ExpectSetLayerStatus structs.Status
	}{
		{
			Name:                 "TaskComplete_NoOtherTasks",
			Task:                 &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.COMPLETED},
			Layer:                &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			Tasks:                []*structs.Task{},
			ExpectKillTask:       false,
			ExpectSetLayerStatus: structs.COMPLETED,
		},
		{
			Name:                 "TaskErrored_NoOtherTasks",
			Task:                 &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.ERRORED},
			Layer:                &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			Tasks:                []*structs.Task{},
			ExpectKillTask:       false,
			ExpectSetLayerStatus: structs.ERRORED,
		},
		{
			Name:                 "TaskKilled_NoOtherTasks",
			Task:                 &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.KILLED, QueueTaskID: "q1"},
			Layer:                &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			Tasks:                []*structs.Task{},
			ExpectKillTask:       true,
			ExpectSetLayerStatus: structs.ERRORED,
		},
		{
			Name:  "TaskComplete_OtherTasksRunning",
			Task:  &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.COMPLETED},
			Layer: &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			Tasks: []*structs.Task{
				{ID: "t2", LayerID: "l1", ETag: tag, Status: structs.RUNNING},
			},
			ExpectKillTask: false,
			//ExpectSetLayerStatus: structs.RUNNING, it's already running so no set should occur
		},
		{
			Name:  "TaskComplete_OtherTasksErrored",
			Task:  &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.COMPLETED},
			Layer: &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			Tasks: []*structs.Task{
				{ID: "t2", LayerID: "l1", ETag: tag, Status: structs.ERRORED},
			},
			ExpectKillTask:       false,
			ExpectSetLayerStatus: structs.ERRORED,
		},
	}

	for _, c := range cases {
		db := database_mock.NewMockDatabase(gomock.NewController(t))
		qu := queue_mock.NewMockQueue(gomock.NewController(t))
		svc := &Service{db: db, qu: qu}

		db.EXPECT().Layers(&structs.Query{Limit: 1, LayerIDs: []string{"l1"}}).Return([]*structs.Layer{c.Layer}, nil)

		if c.ExpectKillTask {
			qu.EXPECT().Kill(c.Task.QueueTaskID).Return(nil)
			db.EXPECT().SetTaskQueueID(c.Task.ID, c.Task.ETag, gomock.Any(), "", structs.ERRORED).Return(nil)
		}
		if c.Task.Status != structs.COMPLETED && c.Task.Status != structs.SKIPPED {
			c.Tasks = append(c.Tasks, c.Task) // because we would return ourself in the query
		}
		db.EXPECT().Tasks(&structs.Query{
			Limit:    defLimit,
			LayerIDs: []string{"l1"},
			Statuses: append(incompleteStates, structs.ERRORED, structs.KILLED),
		}).Return(c.Tasks, nil)

		if c.ExpectSetLayerStatus != "" {
			db.EXPECT().SetLayersStatus(c.ExpectSetLayerStatus, gomock.Any(), []*structs.ObjectRef{{ID: "l1", ETag: tag}}).Return(int64(1), nil)
		}

		err := svc.handleTaskEvent(&changes.Change{Old: c.Task, New: c.Task})

		assert.Nil(t, err)
	}
}

func TestHandleTaskEventUpdatedQueueOrSkip(t *testing.T) {
	tag := "etag"
	cases := []struct {
		Name        string
		Task        *structs.Task
		Layer       *structs.Layer
		ExpectQueue bool
		ExpectSkip  bool
	}{
		{
			Name:        "LayerRunning",
			Task:        &structs.Task{LayerID: "l1", ETag: tag, Status: structs.PENDING},
			Layer:       &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			ExpectQueue: true,
		},
		{
			Name:        "QIDSet-LayerRunning",
			Task:        &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.PENDING, QueueTaskID: "q1"},
			Layer:       &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			ExpectQueue: false,
		},
		{
			Name:        "LayerRunningPaused",
			Task:        &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.PENDING},
			Layer:       &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING, LayerSpec: structs.LayerSpec{PausedAt: 100}},
			ExpectQueue: false,
		},
		{
			Name:        "Paused-LayerRunning",
			Task:        &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.PENDING, TaskSpec: structs.TaskSpec{PausedAt: 100}},
			Layer:       &structs.Layer{ID: "l1", ETag: tag, Status: structs.RUNNING},
			ExpectQueue: false,
		},
		{
			Name:        "LayerSkipped",
			Task:        &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.PENDING},
			Layer:       &structs.Layer{ID: "l1", ETag: tag, Status: structs.SKIPPED},
			ExpectQueue: false,
			ExpectSkip:  true,
		},
		{
			Name:        "LayerNotRunning",
			Task:        &structs.Task{ID: "t1", LayerID: "l1", ETag: tag, Status: structs.PENDING},
			Layer:       &structs.Layer{ID: "l1", ETag: tag, Status: structs.PENDING},
			ExpectQueue: false,
			ExpectSkip:  false,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			qu := queue_mock.NewMockQueue(gomock.NewController(t))
			svc := &Service{db: db, qu: qu}

			db.EXPECT().Layers(&structs.Query{Limit: 1, LayerIDs: []string{"l1"}}).Return([]*structs.Layer{c.Layer}, nil)
			if c.ExpectQueue {
				qu.EXPECT().Enqueue(c.Task).Return("queue-task-id", nil)
				db.EXPECT().SetTaskQueueID(c.Task.ID, c.Task.ETag, gomock.Any(), "queue-task-id", structs.QUEUED).Return(nil)
			}
			if c.ExpectSkip {
				db.EXPECT().SetTasksStatus(structs.SKIPPED, gomock.Any(), []*structs.ObjectRef{{ID: "t1", ETag: tag}}).Return(int64(1), nil)
			}

			err := svc.handleTaskEvent(&changes.Change{Old: c.Task, New: c.Task})

			assert.Nil(t, err)
		})
	}
}

func TestHandleTaskEventDeleted(t *testing.T) {
	in := &changes.Change{
		Old: &structs.Task{},
		New: nil,
	}
	svc := &Service{}

	err := svc.handleTaskEvent(in)

	assert.Nil(t, err)
}

func TestHandleTaskEventCreated(t *testing.T) {
	in := &changes.Change{
		Old: nil,
		New: &structs.Task{},
	}
	svc := &Service{}

	err := svc.handleTaskEvent(in)

	assert.Nil(t, err)
}

func TestQueueTidyLayerWork(t *testing.T) {
	tidyThres := 5 * time.Minute

	cases := []struct {
		Name   string
		Layers []*structs.Layer
	}{
		{
			Name: "Single",
			Layers: []*structs.Layer{
				&structs.Layer{ID: "l0", ETag: "e0"},
			},
		},
		{
			Name: "Multiple",
			Layers: []*structs.Layer{
				&structs.Layer{ID: "l0", ETag: "e0"},
				&structs.Layer{ID: "l1", ETag: "e1"},
				&structs.Layer{ID: "l2", ETag: "e2"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db, opts: &Options{TidyUpdateThreshold: tidyThres}}

			for i := 0; i < len(c.Layers); i += defLimit {
				var res []*structs.Layer
				if i+defLimit > len(c.Layers) {
					res = c.Layers[i:]
				} else {
					res = c.Layers[i : i+defLimit]
				}
				db.EXPECT().Layers(&structs.Query{
					Limit:         defLimit,
					Statuses:      incompleteStates,
					Offset:        i,
					UpdatedBefore: timeNow() - int64(tidyThres.Seconds()),
				}).Return(res, nil)
			}

			echan := make(chan error)
			wchan := make(chan *structs.Layer)
			wg := &sync.WaitGroup{}

			var err error
			go func() {
				for e := range echan {
					err = e
				}
			}()

			wg.Add(len(c.Layers))
			work := []*structs.Layer{}
			go func() {
				for w := range wchan {
					work = append(work, w)
					wg.Done()
				}
			}()

			svc.queueTidyLayerWork(echan, wchan)

			wg.Wait()

			assert.Equal(t, c.Layers, work)
			assert.Nil(t, err)
		})
	}
}

func TestQueueTidyTaskWork(t *testing.T) {
	tidyThres := 5 * time.Minute

	cases := []struct {
		Name  string
		Tasks []*structs.Task
	}{
		{
			Name: "Single",
			Tasks: []*structs.Task{
				&structs.Task{ID: "t0", ETag: "e0"},
			},
		},
		{
			Name: "Multiple",
			Tasks: []*structs.Task{
				&structs.Task{ID: "t0", ETag: "e0"},
				&structs.Task{ID: "t1", ETag: "e1"},
				&structs.Task{ID: "t2", ETag: "e2"},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db, opts: &Options{TidyUpdateThreshold: tidyThres}}

			for i := 0; i < len(c.Tasks); i += defLimit {
				var res []*structs.Task
				if i+defLimit > len(c.Tasks) {
					res = c.Tasks[i:]
				} else {
					res = c.Tasks[i : i+defLimit]
				}
				db.EXPECT().Tasks(&structs.Query{
					Limit:         defLimit,
					Statuses:      incompleteStates,
					Offset:        i,
					UpdatedBefore: timeNow() - int64(tidyThres.Seconds()),
				}).Return(res, nil)
			}

			echan := make(chan error)
			wchan := make(chan *structs.Task)
			wg := &sync.WaitGroup{}

			var err error
			go func() {
				for e := range echan {
					err = e
				}
			}()

			wg.Add(len(c.Tasks))
			work := []*structs.Task{}
			go func() {
				for w := range wchan {
					work = append(work, w)
					wg.Done()
				}
			}()

			svc.queueTidyTaskWork(echan, wchan)

			wg.Wait()

			assert.Equal(t, c.Tasks, work)
			assert.Nil(t, err)
		})
	}
}

func TestSkipLayerTasks(t *testing.T) {
	cases := []struct {
		Name          string
		In            string
		Queries       []*structs.Query
		Tasks         [][]*structs.Task
		ExpectedError error
	}{
		{
			Name: "Single",
			In:   "layer-x",
			Queries: []*structs.Query{
				{Limit: defLimit, LayerIDs: []string{"layer-x"}, Statuses: incompleteStates},
			},
			Tasks:         [][]*structs.Task{{{ID: "t0", ETag: "tx"}}},
			ExpectedError: nil,
		},
		{
			Name: "Multiple",
			In:   "layer-1",
			Tasks: [][]*structs.Task{
				{{ID: "t0", ETag: "tx"}, {ID: "t1", ETag: "tx"}},
				{{ID: "t2", ETag: "tx"}, {ID: "t3", ETag: "tx4"}},
				{{ID: "t4", ETag: "tx7"}},
			},
			Queries: []*structs.Query{
				{
					Limit:    defLimit,
					LayerIDs: []string{"layer-1"},
					Statuses: incompleteStates,
					Offset:   0,
				},
				{
					Limit:    defLimit,
					LayerIDs: []string{"layer-1"},
					Statuses: incompleteStates,
					Offset:   2,
				},
				{
					Limit:    defLimit,
					LayerIDs: []string{"layer-1"},
					Statuses: incompleteStates,
					Offset:   4,
				},
			},
			ExpectedError: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			svc := &Service{db: db}

			for i := 0; i < len(c.Queries); i++ {
				ids := []*structs.ObjectRef{}
				for _, task := range c.Tasks[i] {
					ids = append(ids, &structs.ObjectRef{ID: task.ID, ETag: task.ETag})
				}
				db.EXPECT().Tasks(c.Queries[i]).Return(c.Tasks[i], nil)
				db.EXPECT().SetTasksStatus(structs.SKIPPED, gomock.Any(), ids).Return(int64(len(ids)), nil)
			}

			err := svc.skipLayerTasks(c.In)

			assert.Equal(t, c.ExpectedError, err)
		})
	}
}
