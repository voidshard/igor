package changes

// It's kind of odd having this in the pkg/changes package, but it avoids a circular dependency when importing
// mocks during testing

import (
	"github.com/voidshard/igor/pkg/structs"
)

type Change struct {
	Kind structs.Kind
	Old  interface{}
	New  interface{}
}

type Stream interface {
	Next() (*Change, error)
	Close() error
}
