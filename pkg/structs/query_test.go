package structs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitize(t *testing.T) {
	cases := []struct {
		Name   string
		Given  *Query
		Expect *Query
	}{
		{
			Name:   "SetsDefaultLimit",
			Given:  &Query{},
			Expect: &Query{Limit: queryLimitDefault},
		},
		{
			Name:   "SetsMaxLimit",
			Given:  &Query{Limit: queryLimitMax + 1},
			Expect: &Query{Limit: queryLimitMax},
		},
		{
			Name:   "SanitizesOffset",
			Given:  &Query{Limit: 1, Offset: -1},
			Expect: &Query{Limit: 1, Offset: 0},
		},
		{
			Name:   "ZeroJobs",
			Given:  &Query{Limit: 1, JobIDs: []string{}},
			Expect: &Query{Limit: 1},
		},
		{
			Name:   "ZeroLayers",
			Given:  &Query{Limit: 1, LayerIDs: []string{}},
			Expect: &Query{Limit: 1},
		},
		{
			Name:   "ZeroTasks",
			Given:  &Query{Limit: 1, TaskIDs: []string{}},
			Expect: &Query{Limit: 1},
		},
		{
			Name:   "ZeroStatuses",
			Given:  &Query{Limit: 1, Statuses: []Status{}},
			Expect: &Query{Limit: 1},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			c.Given.Sanitize()
			assert.Equal(t, c.Given, c.Expect)
		})
	}
}
