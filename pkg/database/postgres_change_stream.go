package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/voidshard/igor/pkg/structs"
)

type pgChangeStream struct {
	ctx    context.Context
	conn   *pgxpool.Conn
	closed bool
}

type pgRawPayload struct {
	Table string `json:"table"`
}

type pgLayerPayload struct {
	Old *structs.Layer `json:"old"`
	New *structs.Layer `json:"new"`
}

type pgTaskPayload struct {
	Old *structs.Task `json:"old"`
	New *structs.Task `json:"new"`
}

func (p *pgChangeStream) Next() (*Change, error) {
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

	if strings.HasPrefix(payload.Table, "l") {
		lyr := pgLayerPayload{}
		err = json.Unmarshal([]byte(notification.Payload), &lyr)
		return &Change{Old: lyr.Old, New: lyr.New, Kind: structs.KindLayer}, err
	} else if strings.HasPrefix(payload.Table, "t") {
		tsk := pgTaskPayload{}
		err = json.Unmarshal([]byte(notification.Payload), &tsk)
		return &Change{Old: tsk.Old, New: tsk.New, Kind: structs.KindTask}, err
	}

	return nil, fmt.Errorf("unknown kind for table %s", payload.Table)
}

func (p *pgChangeStream) Close() error {
	p.closed = true
	p.conn.Release()
	return nil
}
