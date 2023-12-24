package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFinalStatus(t *testing.T) {
	cases := []struct {
		Name   string
		Given  Status
		Expect bool
	}{
		{"StatusUndefined", "x", false},
		{"StatusPending", PENDING, false},
		{"StatusQueued", QUEUED, false},
		{"StatusRunning", RUNNING, false},
		{"StatusKilled", KILLED, true},
		{"StatusCompleted", COMPLETED, true},
		{"StatusErrored", ERRORED, true},
		{"StatusSkipped", SKIPPED, true},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			assert.Equal(t, IsFinalStatus(c.Given), c.Expect)
		})
	}
}

func TestToStatus(t *testing.T) {
	cases := []struct {
		Name   string
		Given  string
		Expect Status
	}{
		{"StatusUndefined", "x", ""},
		{"StatusPending", "PENDING", PENDING},
		{"StatusQueued", "QUEUED", QUEUED},
		{"StatusRunning", "RUNNING", RUNNING},
		{"StatusKilled", "KILLED", KILLED},
		{"StatusCompleted", "COMPLETED", COMPLETED},
		{"StatusErrored", "ERRORED", ERRORED},
		{"StatusSkipped", "SKIPPED", SKIPPED},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			assert.Equal(t, ToStatus(c.Given), c.Expect)
		})
	}

}
