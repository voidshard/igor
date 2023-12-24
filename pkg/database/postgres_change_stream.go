package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/voidshard/igor/pkg/database/changes"
	"github.com/voidshard/igor/pkg/structs"
)

// pgChangeStream implements how we stream changes from postgres
type pgChangeStream struct {
	ctx    context.Context
	conn   *pgxpool.Conn
	closed bool
}

// pgRawPayload lets us unmarshal just the table name from a notification
type pgRawPayload struct {
	Table string `json:"table"`
}

// pgLayerPayload lets us unmarshal a layer change from a notification
type pgLayerPayload struct {
	Old *structs.Layer `json:"old"`
	New *structs.Layer `json:"new"`
}

// pgTaskPayload lets us unmarshal a task change from a notification
type pgTaskPayload struct {
	Old *structs.Task `json:"old"`
	New *structs.Task `json:"new"`
}

// Next blocks until a change is available or the stream is closed.
func (p *pgChangeStream) Next() (*changes.Change, error) {
	if p.closed {
		return nil, nil
	}

	notification, err := p.conn.Conn().WaitForNotification(p.ctx)
	if err != nil {
		return nil, err
	}

	payload := pgRawPayload{}
	err = json.Unmarshal([]byte(notification.Payload), &payload)
	if err != nil {
		return nil, err
	}

	ch := &changes.Change{}
	if strings.HasPrefix(payload.Table, "l") {
		ch.Kind = structs.KindLayer
		lyr := pgLayerPayload{}
		err = json.Unmarshal([]byte(notification.Payload), &lyr)
		if lyr.Old != nil {
			ch.Old = lyr.Old
		}
		if lyr.New != nil {
			ch.New = lyr.New
		}
		return ch, nil
	} else if strings.HasPrefix(payload.Table, "t") {
		ch.Kind = structs.KindTask
		tsk := pgTaskPayload{}
		err = json.Unmarshal([]byte(notification.Payload), &tsk)
		if tsk.Old != nil {
			ch.Old = tsk.Old
		}
		if tsk.New != nil {
			ch.New = tsk.New
		}
		return ch, nil
	}

	return nil, fmt.Errorf("unknown kind for table %s", payload.Table)
}

// Close closes the stream.
func (p *pgChangeStream) Close() error {
	p.closed = true
	p.conn.Release()
	return nil
}
