package database

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	"github.com/voidshard/igor/internal/mocks/pkg/database_mock"
	"github.com/voidshard/igor/internal/utils"
	"github.com/voidshard/igor/pkg/errors"
	"github.com/voidshard/igor/pkg/structs"
)

func TestTasks(t *testing.T) {
	cases := []struct {
		Name         string
		InIds        []string
		ExpectDBCall bool
		ExpectQuery  *structs.Query
		ReturnTasks  []*structs.Task
		ReturnErr    error
		ExpectErr    error
	}{
		{
			Name:         "InvalidID",
			InIds:        []string{"a", "b", "c"},
			ExpectDBCall: false,
			ExpectQuery:  nil,
			ReturnTasks:  nil,
			ReturnErr:    nil,
			ExpectErr:    fmt.Errorf("%w %s is not a valid task id", errors.ErrInvalidArg, "a"),
		},
		{
			Name:         "UniqueIDs",
			InIds:        []string{utils.NewID(1), utils.NewID(1), utils.NewID(2)},
			ExpectDBCall: true,
			ExpectQuery: &structs.Query{
				TaskIDs: []string{utils.NewID(1), utils.NewID(2)},
				Limit:   2,
			},
			ReturnTasks: []*structs.Task{&structs.Task{ID: utils.NewID(1)}, &structs.Task{ID: utils.NewID(2)}},
			ReturnErr:   nil,
			ExpectErr:   nil,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			svc := database_mock.NewMockDatabase(gomock.NewController(t))
			qdb := defaultQDB{db: svc}

			if c.ExpectDBCall {
				svc.EXPECT().Tasks(c.ExpectQuery).Return(c.ReturnTasks, c.ReturnErr)
			}

			tasks, err := qdb.Tasks(c.InIds)

			if c.ExpectErr != nil {
				assert.NotNil(t, err)
				assert.EqualError(t, err, c.ExpectErr.Error())
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, c.ReturnTasks, tasks)
		})
	}
}

func TestSetTaskStateInvalid(t *testing.T) {
	cases := []struct {
		Name     string
		InTask   *structs.Task
		InStatus structs.Status
		Error    error
	}{
		{
			Name:     "NilTask",
			InTask:   nil,
			InStatus: structs.RUNNING,
			Error:    nil,
		},
		{
			Name:     "InvalidStatusPending",
			InTask:   &structs.Task{ID: utils.NewID(1), ETag: utils.NewID(2)},
			InStatus: structs.PENDING,
			Error:    fmt.Errorf("%w %s is not a permitted status (running, errored, completed, skipped)", errors.ErrInvalidState, structs.PENDING),
		},
		{
			Name:     "InvalidStatusReady",
			InTask:   &structs.Task{ID: utils.NewID(1), ETag: utils.NewID(2)},
			InStatus: structs.READY,
			Error:    fmt.Errorf("%w %s is not a permitted status (running, errored, completed, skipped)", errors.ErrInvalidState, structs.READY),
		},
		{
			Name:     "InvalidStatusQueued",
			InTask:   &structs.Task{ID: utils.NewID(1), ETag: utils.NewID(2)},
			InStatus: structs.QUEUED,
			Error:    fmt.Errorf("%w %s is not a permitted status (running, errored, completed, skipped)", errors.ErrInvalidState, structs.QUEUED),
		},
		{
			Name:     "InvalidStatusKilled",
			InTask:   &structs.Task{ID: utils.NewID(1), ETag: utils.NewID(2)},
			InStatus: structs.KILLED,
			Error:    fmt.Errorf("%w %s is not a permitted status (running, errored, completed, skipped)", errors.ErrInvalidState, structs.KILLED),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			svc := database_mock.NewMockDatabase(gomock.NewController(t))
			qdb := defaultQDB{db: svc}

			_, err := qdb.SetTaskState(c.InTask, c.InStatus, "")

			if c.Error != nil {
				assert.NotNil(t, err)
				assert.EqualError(t, err, c.Error.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestSetTaskState(t *testing.T) {
	cases := []struct {
		Name          string
		InTask        *structs.Task
		InStatus      structs.Status
		InMsg         string
		ReturnAltered int64
		ReturnErr     error
		ExpectEtag    bool
		ExpectErr     error
	}{
		{
			Name: "SetRunning",
			InTask: &structs.Task{
				ID:   utils.NewID(1),
				ETag: utils.NewID(2),
			},
			InStatus:      structs.RUNNING,
			InMsg:         "c",
			ReturnAltered: 1,
			ReturnErr:     nil,
			ExpectEtag:    true,
			ExpectErr:     nil,
		},
		{
			Name: "SetErrored",
			InTask: &structs.Task{
				ID:   utils.NewID(1),
				ETag: utils.NewID(2),
			},
			InStatus:      structs.ERRORED,
			InMsg:         "c",
			ReturnAltered: 1,
			ReturnErr:     nil,
			ExpectEtag:    true,
			ExpectErr:     nil,
		},
		{
			Name: "SetCompleted",
			InTask: &structs.Task{
				ID:   utils.NewID(1),
				ETag: utils.NewID(2),
			},
			InStatus:      structs.COMPLETED,
			InMsg:         "c",
			ReturnAltered: 1,
			ReturnErr:     nil,
			ExpectEtag:    true,
			ExpectErr:     nil,
		},
		{
			Name: "SetSkipped",
			InTask: &structs.Task{
				ID:   utils.NewID(1),
				ETag: utils.NewID(2),
			},
			InStatus:      structs.SKIPPED,
			InMsg:         "c",
			ReturnAltered: 1,
			ReturnErr:     nil,
			ExpectEtag:    true,
			ExpectErr:     nil,
		},
		{
			Name: "FailedSet",
			InTask: &structs.Task{
				ID:   utils.NewID(1),
				ETag: utils.NewID(2),
			},
			InStatus:      structs.SKIPPED,
			ReturnAltered: 1,
			ReturnErr:     fmt.Errorf("fail"),
			ExpectEtag:    false,
			ExpectErr:     fmt.Errorf("fail"),
		},
		{
			Name: "EtagMismatch",
			InTask: &structs.Task{
				ID:   utils.NewID(1),
				ETag: utils.NewID(2),
			},
			InStatus:      structs.SKIPPED,
			ReturnAltered: 0,
			ReturnErr:     nil,
			ExpectEtag:    false,
			ExpectErr:     fmt.Errorf("%w updated altered 0 entries", errors.ErrETagMismatch),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			svc := database_mock.NewMockDatabase(gomock.NewController(t))
			qdb := defaultQDB{db: svc}

			svc.EXPECT().SetTasksStatus(c.InStatus, gomock.Any(), []*structs.ObjectRef{&structs.ObjectRef{ID: c.InTask.ID, ETag: c.InTask.ETag}}, c.InMsg).Return(c.ReturnAltered, c.ReturnErr)

			value, err := qdb.SetTaskState(c.InTask, c.InStatus, c.InMsg)

			if c.ExpectErr != nil {
				assert.EqualError(t, err, c.ExpectErr.Error())
			} else {
				assert.NoError(t, err)
			}
			if c.ExpectEtag {
				assert.False(t, value == "")
			} else {
				assert.True(t, value == "")
			}
		})
	}

}
