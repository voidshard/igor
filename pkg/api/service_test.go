package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	"github.com/voidshard/igor/internal/mocks/pkg/database_mock"
	"github.com/voidshard/igor/pkg/structs"
)

func TestSkipLayerTasks(t *testing.T) {
	defLimitSkipLayerTasks = 2

	cases := []struct {
		Name          string
		In            []*structs.ObjectRef
		Mock          func(*database_mock.MockDatabase)
		ExpectedError error
	}{
		{
			Name: "Single",
			In: []*structs.ObjectRef{
				&structs.ObjectRef{ID: "0", ETag: "e0"},
			},
			Mock: func(db *database_mock.MockDatabase) {
				tasks := []*structs.Task{{ID: "t0", ETag: "tx"}}
				gomock.InOrder(
					db.EXPECT().Tasks(&structs.Query{
						Limit:    defLimitSkipLayerTasks,
						LayerIDs: []string{"0"},
						Statuses: incompleteStates,
					}).Return(tasks, nil),
					db.EXPECT().SetTasksStatus(
						structs.SKIPPED,
						gomock.Any(),
						[]*structs.ObjectRef{
							&structs.ObjectRef{ID: "t0", ETag: "tx"},
						},
					).Return(int64(1), nil),
				)
			},
			ExpectedError: nil,
		},
		{
			Name: "Multiple",
			In: []*structs.ObjectRef{
				&structs.ObjectRef{ID: "0", ETag: "e0"},
				&structs.ObjectRef{ID: "1", ETag: "e1"},
				&structs.ObjectRef{ID: "2", ETag: "e2"},
			},
			Mock: func(db *database_mock.MockDatabase) {
				tasks1 := []*structs.Task{{ID: "t0", ETag: "tx"}, {ID: "t1", ETag: "tx"}}
				tasks2 := []*structs.Task{{ID: "t2", ETag: "tx"}, {ID: "t3", ETag: "tx4"}}
				tasks3 := []*structs.Task{{ID: "t4", ETag: "tx7"}}
				gomock.InOrder(
					db.EXPECT().Tasks(&structs.Query{
						Limit:    defLimitSkipLayerTasks,
						LayerIDs: []string{"0", "1", "2"},
						Statuses: incompleteStates,
						Offset:   0,
					}).Return(tasks1, nil),
					db.EXPECT().SetTasksStatus(
						structs.SKIPPED,
						gomock.Any(),
						[]*structs.ObjectRef{
							&structs.ObjectRef{ID: "t0", ETag: "tx"},
							&structs.ObjectRef{ID: "t1", ETag: "tx"},
						},
					).Return(int64(2), nil),
					db.EXPECT().Tasks(&structs.Query{
						Limit:    defLimitSkipLayerTasks,
						LayerIDs: []string{"0", "1", "2"},
						Statuses: incompleteStates,
						Offset:   2,
					}).Return(tasks2, nil),
					db.EXPECT().SetTasksStatus(
						structs.SKIPPED,
						gomock.Any(),
						[]*structs.ObjectRef{
							&structs.ObjectRef{ID: "t2", ETag: "tx"},
							&structs.ObjectRef{ID: "t3", ETag: "tx4"},
						},
					).Return(int64(2), nil),
					db.EXPECT().Tasks(&structs.Query{
						Limit:    defLimitSkipLayerTasks,
						LayerIDs: []string{"0", "1", "2"},
						Statuses: incompleteStates,
						Offset:   4,
					}).Return(tasks3, nil),
					db.EXPECT().SetTasksStatus(
						structs.SKIPPED,
						gomock.Any(),
						[]*structs.ObjectRef{
							&structs.ObjectRef{ID: "t4", ETag: "tx7"},
						},
					).Return(int64(1), nil),
				)
			},
			ExpectedError: nil,
		},
		{
			Name:          "Nil",
			In:            nil,
			ExpectedError: nil,
		},
		{
			Name:          "Empty",
			In:            []*structs.ObjectRef{},
			ExpectedError: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			db := database_mock.NewMockDatabase(gomock.NewController(t))
			if c.Mock != nil {
				c.Mock(db)
			}
			svc := &Service{db: db}

			err := svc.skipLayerTasks(c.In)

			assert.Equal(t, c.ExpectedError, err)
		})
	}
}
