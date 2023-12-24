package api

import (
	"github.com/voidshard/igor/internal/core"
	"github.com/voidshard/igor/pkg/database"
	"github.com/voidshard/igor/pkg/queue"
	"github.com/voidshard/igor/pkg/structs"
)

func NewAPI(db database.Database, qu queue.Queue, opts *structs.Options) (API, error) {
	return core.NewService(db, qu, opts)
}
