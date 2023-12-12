package queue

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	gomock "go.uber.org/mock/gomock"

	"github.com/voidshard/igor/internal/mocks/pkg/database_mock"
	"github.com/voidshard/igor/pkg/structs"
)

func TestSetError(t *testing.T) {
	svc := database_mock.NewMockQueueDB(gomock.NewController(t))
	errs := make(chan error)
	m := &Meta{
		Task: &structs.Task{
			ID: "x",
		},
		errs: errs,
		svc:  svc,
	}

	svc.EXPECT().SetTaskState(m.Task, structs.ERRORED, "a b c").Return("", nil)

	go func() {
		err := <-errs
		assert.Nil(t, err)
	}()

	m.SetError(fmt.Errorf("a b c"))
}

func TestSetSkip(t *testing.T) {
	svc := database_mock.NewMockQueueDB(gomock.NewController(t))
	errs := make(chan error)
	m := &Meta{
		Task: &structs.Task{
			ID: "x",
		},
		errs: errs,
		svc:  svc,
	}
	messages := []string{"a", "b", "c"}

	svc.EXPECT().SetTaskState(m.Task, structs.SKIPPED, "a b c").Return("", nil)

	go func() {
		err := <-errs
		assert.Nil(t, err)
	}()

	m.SetSkip(messages...)
}

func TestSetComplete(t *testing.T) {
	svc := database_mock.NewMockQueueDB(gomock.NewController(t))
	errs := make(chan error)
	m := &Meta{
		Task: &structs.Task{
			ID: "x",
		},
		errs: errs,
		svc:  svc,
	}
	messages := []string{"a", "b", "c"}

	svc.EXPECT().SetTaskState(m.Task, structs.COMPLETED, "a b c").Return("", nil)

	go func() {
		err := <-errs
		assert.Nil(t, err)
	}()

	m.SetComplete(messages...)
}

func TestSetRunning(t *testing.T) {
	svc := database_mock.NewMockQueueDB(gomock.NewController(t))
	errs := make(chan error)
	m := &Meta{
		Task: &structs.Task{
			ID: "x",
		},
		errs: errs,
		svc:  svc,
	}
	messages := []string{"a", "b", "c"}

	svc.EXPECT().SetTaskState(m.Task, structs.RUNNING, "a b c").Return("", nil)

	go func() {
		err := <-errs
		assert.Nil(t, err)
	}()

	m.SetRunning(messages...)
}
