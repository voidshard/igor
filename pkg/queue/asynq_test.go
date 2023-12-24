package queue

import (
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	"github.com/voidshard/igor/internal/mocks/pkg/database_mock"
	"github.com/voidshard/igor/pkg/structs"
)

func TestDeaggregateTasks(t *testing.T) {
	svc := database_mock.NewMockQueueDB(gomock.NewController(t))
	asynk := &Asynq{svc: svc}

	svc.EXPECT().Tasks([]string{"1", "2", "3"}).Return([]*structs.Task{
		&structs.Task{ID: "1"},
		&structs.Task{ID: "2"},
		&structs.Task{ID: "3"},
	}, nil)

	given := asynq.NewTask("group", []byte("1"+asyncAggRune+"2"+asyncAggRune+"3"+asyncAggRune))

	result, err := asynk.deaggregateTasks(given)

	assert.Nil(t, err)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "1", result[0].Task.ID)
	assert.Equal(t, "2", result[1].Task.ID)
	assert.Equal(t, "3", result[2].Task.ID)
}

func TestAggregate(t *testing.T) {
	group := "group"

	cases := []struct {
		Name   string
		Given  [][]byte
		Expect []byte
	}{
		{
			"SingleTask",
			[][]byte{[]byte("1-payload")},
			[]byte("1-payload" + asyncAggRune),
		},
		{
			"MultipleTasks",
			[][]byte{[]byte("1-payload"), []byte("2-payload")},
			[]byte("1-payload" + asyncAggRune + "2-payload" + asyncAggRune),
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			in := []*asynq.Task{}
			for _, b := range c.Given {
				in = append(in, asynq.NewTask(group, b))
			}

			result := aggregate(group, in)

			assert.Equal(t, c.Expect, result.Payload())
		})
	}

}

func TestAggregatedTask(t *testing.T) {
	assert.Equal(t, "aggregatedZAP", aggregatedTask("ZAP"))
}
