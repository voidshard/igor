package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/voidshard/igor/pkg/database/changes"
	"github.com/voidshard/igor/pkg/structs"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	opts *Options
	pool *pgxpool.Pool
}

func NewPostgres(opts *Options) (*Postgres, error) {
	pool, err := pgxpool.New(context.Background(), opts.URL)
	return &Postgres{pool: pool, opts: opts}, err
}

func (p *Postgres) Close() error {
	p.pool.Close()
	return nil
}

func (p *Postgres) InsertJob(j *structs.Job, ls []*structs.Layer, ts []*structs.Task) error {
	// before we open a transaction, build all the SQL

	// job
	jstr, jargs := toJobSqlArgs(1, j) // the sql lib starts at 1
	jstr = fmt.Sprintf(`INSERT INTO %s (name, id, status, etag, created_at, updated_at) VALUES %s;`, string(structs.KindJob), jstr)

	// layers
	lstrs, largs := []string{}, []interface{}{}
	for _, l := range ls {
		s, a := toLayerSqlArgs(len(largs)+1, l)
		lstrs = append(lstrs, s)
		largs = append(largs, a...)
	}
	lstr := strings.Join(lstrs, ",") // join so its (),(),() etc
	lstr = fmt.Sprintf(`INSERT INTO %s (name, paused_at, order, id, status, etag, job_id, created_at, updated_at) VALUES %s;`, string(structs.KindLayer), lstr)

	// tasks
	tstrs, targs := []string{}, []interface{}{}
	for _, t := range ts {
		s, a := toTaskSqlArgs(len(targs)+1, t)
		tstrs = append(tstrs, s)
		targs = append(targs, a...)
	}
	tstr := strings.Join(tstrs, ",") // join so its (),(),() etc
	tstr = fmt.Sprintf(`INSERT INTO %s (type, args, name, paused_at, retries, id, status, etag, job_id, layer_id, queue_task_id, message, created_at, updated_at) VALUES %s;`, string(structs.KindTask), tstr)

	// ok, we're ready to go
	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, jstr, jargs...)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	_, err = tx.Exec(ctx, lstr, largs...)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	_, err = tx.Exec(ctx, tstr, targs...)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		tx.Rollback(ctx)
	}
	return err
}

func (p *Postgres) InsertTasks(in []*structs.Task) error {
	tstrs, targs := []string{}, []interface{}{}
	for _, t := range in {
		s, a := toTaskSqlArgs(1, t)
		tstrs = append(tstrs, s)
		targs = append(targs, a...)
	}
	tstr := strings.Join(tstrs, ",") // join so its (),(),() etc
	tstr = fmt.Sprintf(`INSERT INTO %s (type, args, name, paused_at, retries, id, status, etag, job_id, layer_id, queue_task_id, message, created_at, updated_at) VALUES %s;`, string(structs.KindTask), tstr)

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// we have a FK task.layer_id -> layer.id so we can let the DB ensure the layer exists
	_, err = conn.Exec(ctx, tstr, targs...)
	return err
}

func (p *Postgres) SetLayersPaused(at int64, newTag string, ids []*structs.ObjectRef) (int64, error) {
	return p.setPaused(string(structs.KindLayer), at, newTag, ids)
}

func (p *Postgres) SetTasksPaused(at int64, newTag string, ids []*structs.ObjectRef) (int64, error) {
	return p.setPaused(string(structs.KindTask), at, newTag, ids)
}

func (p *Postgres) SetJobsStatus(status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error) {
	return p.setStatus(string(structs.KindJob), status, newTag, ids)
}

func (p *Postgres) SetLayersStatus(status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error) {
	return p.setStatus(string(structs.KindLayer), status, newTag, ids)
}

func (p *Postgres) SetTasksStatus(status structs.Status, newTag string, ids []*structs.ObjectRef, msg ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var qstr string
	var args []interface{}
	if msg == nil || len(msg) == 0 {
		qstr, args = toSqlTags(4, ids)
		qstr = fmt.Sprintf(`UPDATE %s SET status=$1, etag=$2, updated_at=$3 WHERE %s;`, string(structs.KindTask), qstr)
		args = append([]interface{}{status, newTag, timeNow()}, args...)
	} else {
		qstr, args = toSqlTags(5, ids)
		qstr = fmt.Sprintf(`UPDATE %s SET status=$1, etag=$2, updated_at=$3, message=$4 WHERE %s;`, string(structs.KindTask), qstr)
		args = append([]interface{}{status, newTag, timeNow(), strings.Join(msg, " ")}, args...)
	}

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	info, err := conn.Exec(ctx, qstr, args...)
	if err == nil {
		return info.RowsAffected(), nil
	}
	return 0, err
}

func (p *Postgres) SetTaskQueueID(taskID, etag, newEtag, queueTaskID string, state structs.Status) (int64, error) {
	qstr := fmt.Sprintf(`UPDATE %s SET queue_task_id=$1, etag=$2, updated_at=$3, status=$4 WHERE id=$5 AND etag=$6;`, string(structs.KindTask))
	args := []interface{}{queueTaskID, newEtag, timeNow(), state, taskID, etag}

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	info, err := conn.Exec(ctx, qstr, args...)
	if err == nil {
		return info.RowsAffected(), nil
	}
	return 0, err
}

func (p *Postgres) Jobs(q *structs.Query) ([]*structs.Job, error) {
	where, args := toSqlQuery(map[string][]string{
		"id":     q.JobIDs,
		"status": statusToStrings(q.Statuses),
	})
	args = append(args, q.Limit, q.Offset)

	// TODO: prepare statement
	qstr := fmt.Sprintf(`SELECT name, id, status, etag, created_at, updated_at FROM %s %s ORDER BY created_at DESC LIMIT $%d OFFSET $%d;`,
		string(structs.KindJob), where, len(args)-1, len(args),
	)

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, qstr, args...)
	if err != nil {
		return nil, err
	}

	jobs := []*structs.Job{}
	for rows.Next() {
		j := structs.Job{}
		err = rows.Scan(
			&j.Name,
			&j.ID,
			&j.Status,
			&j.ETag,
			&j.CreatedAt,
			&j.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, &j)
	}

	return jobs, nil
}

func (p *Postgres) Layers(q *structs.Query) ([]*structs.Layer, error) {
	where, args := toSqlQuery(map[string][]string{
		"job_id": q.JobIDs,
		"id":     q.LayerIDs,
		"status": statusToStrings(q.Statuses),
	})
	args = append(args, q.Limit, q.Offset)

	qstr := fmt.Sprintf(`SELECT name, paused_at, order, id, status, etag, job_id, created_at, updated_at FROM %s %s 
	ORDER BY created_at DESC LIMIT $%d OFFSET $%d;`,
		string(structs.KindLayer), where, len(args)-1, len(args),
	)

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, qstr, args...)
	if err != nil {
		return nil, err
	}

	layers := []*structs.Layer{}
	for rows.Next() {
		l := structs.Layer{}
		err = rows.Scan(
			&l.Name,
			&l.PausedAt,
			&l.Order,
			&l.ID,
			&l.Status,
			&l.ETag,
			&l.JobID,
			&l.CreatedAt,
			&l.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		layers = append(layers, &l)
	}

	return layers, nil
}

func (p *Postgres) Tasks(q *structs.Query) ([]*structs.Task, error) {
	where, args := toSqlQuery(map[string][]string{
		"job_id":   q.JobIDs,
		"layer_id": q.LayerIDs,
		"id":       q.TaskIDs,
		"status":   statusToStrings(q.Statuses),
	})
	args = append(args, q.Limit, q.Offset)

	qstr := fmt.Sprintf(`SELECT type, args, name, paused_at, id, status, etag, job_id, layer_id, queue_task_id, message, created_at, updated_at FROM %s %s
	ORDER BY created_at DESC LIMIT $%d OFFSET $%d;`,
		string(structs.KindTask), where, len(args)-1, len(args),
	)

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, qstr, args...)
	if err != nil {
		return nil, err
	}

	tasks := []*structs.Task{}
	for rows.Next() {
		t := structs.Task{}
		err = rows.Scan(
			&t.Type,
			&t.Args,
			&t.Name,
			&t.PausedAt,
			&t.ID,
			&t.Status,
			&t.ETag,
			&t.JobID,
			&t.LayerID,
			&t.QueueTaskID,
			&t.Message,
			&t.CreatedAt,
			&t.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, &t)
	}

	return tasks, nil
}

func (p *Postgres) Changes() (changes.Stream, error) {
	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	_, err = conn.Exec(ctx, "listen igor_events")
	return &pgChangeStream{
		ctx:  ctx,
		conn: conn,
	}, err
}

func (p *Postgres) setStatus(table string, status structs.Status, newTag string, ids []*structs.ObjectRef) (int64, error) {
	qstr, args := toSqlTags(4, ids)
	qstr = fmt.Sprintf(`UPDATE %s SET status=$1, etag=$2, updated_at=$3 WHERE %s;`, table, qstr)
	args = append([]interface{}{status, newTag, timeNow()}, args...)

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	info, err := conn.Exec(ctx, qstr, args...)
	if err == nil {
		return info.RowsAffected(), nil
	}
	return 0, err
}

func (p *Postgres) setPaused(table string, at int64, newTag string, ids []*structs.ObjectRef) (int64, error) {
	qstr, args := toSqlTags(4, ids)
	qstr = fmt.Sprintf(`UPDATE %s SET paused_at=$1, etag=$2, updated_at=$3 WHERE %s;`, table, qstr)
	args = append([]interface{}{at, newTag, timeNow()}, args...)

	ctx := context.Background()
	conn, err := p.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	info, err := conn.Exec(ctx, qstr, args...)
	if err == nil {
		return info.RowsAffected(), nil
	}
	return 0, err
}

func toSqlQuery(in map[string][]string) (string, []interface{}) {
	if in == nil || len(in) == 0 {
		return "", []interface{}{}
	}
	and := []string{}
	args := []interface{}{}
	for k, v := range in {
		if v == nil || len(v) == 0 {
			continue
		}
		s, a := toSqlIn(len(args)+1, k, v)
		and = append(and, s)
		args = append(args, a...)
	}
	if len(and) == 0 {
		return "", args
	}
	return fmt.Sprintf("WHERE %s", strings.Join(and, " AND ")), args
}

func toSqlIn(offset int, field string, args []string) (string, []interface{}) {
	if len(args) == 0 {
		return "", []interface{}{}
	}
	vals := []string{}
	ifargs := []interface{}{}
	for i, a := range args {
		vals = append(vals, fmt.Sprintf("$%d", i+offset))
		ifargs = append(ifargs, a)
	}
	return fmt.Sprintf("%s IN (%s)", field, strings.Join(vals, ", ")), ifargs
}

func toListInterface(in []string) []interface{} {
	// This is so dumb..
	l := make([]interface{}, len(in))
	for i, v := range in {
		l[i] = v
	}
	return l
}

func toSqlTags(offset int, ids []*structs.ObjectRef) (string, []interface{}) {
	vals := []string{}
	subs := []interface{}{}
	for _, id := range ids {
		vals = append(vals, fmt.Sprintf("(id=$%d AND etag=$%d)", offset+len(subs), offset+len(subs)+1))
		subs = append(subs, id.ID, id.ETag)
	}
	return strings.Join(vals, " OR "), subs
}

func toJobSqlArgs(offset int, j *structs.Job) (string, []interface{}) {
	vals := []string{}
	for i := offset; i < 6+offset; i++ {
		vals = append(vals, fmt.Sprintf("$%d", i))
	}
	if j.CreatedAt == 0 {
		j.CreatedAt = timeNow()
		j.UpdatedAt = j.CreatedAt
	}
	return fmt.Sprintf("(%s)", strings.Join(vals, ", ")), []interface{}{
		j.Name,
		j.ID,
		j.Status,
		j.ETag,
		j.CreatedAt,
		j.UpdatedAt,
	}
}

func toLayerSqlArgs(offset int, l *structs.Layer) (string, []interface{}) {
	vals := []string{}
	for i := offset; i < 9+offset; i++ {
		vals = append(vals, fmt.Sprintf("$%d", i))
	}
	if l.CreatedAt == 0 {
		l.CreatedAt = timeNow()
		l.UpdatedAt = l.CreatedAt
	}
	return fmt.Sprintf("(%s)", strings.Join(vals, ", ")), []interface{}{
		l.Name,
		l.PausedAt,
		l.Order,
		l.ID,
		l.Status,
		l.ETag,
		l.JobID,
		l.CreatedAt,
		l.UpdatedAt,
	}
}

func toTaskSqlArgs(offset int, t *structs.Task) (string, []interface{}) {
	vals := []string{}
	for i := offset; i < 14+offset; i++ {
		vals = append(vals, fmt.Sprintf("$%d", i))
	}
	if t.CreatedAt == 0 {
		t.CreatedAt = timeNow()
		t.UpdatedAt = t.CreatedAt
	}
	return fmt.Sprintf("(%s)", strings.Join(vals, ", ")), []interface{}{
		t.Type,
		t.Args,
		t.Name,
		t.PausedAt,
		t.Retries,
		t.ID,
		t.Status,
		t.ETag,
		t.JobID,
		t.LayerID,
		t.QueueTaskID,
		t.Message,
		t.CreatedAt,
		t.UpdatedAt,
	}
}

func statusToStrings(in []structs.Status) []string {
	if in == nil || len(in) == 0 {
		return nil
	}
	out := []string{}
	for _, s := range in {
		out = append(out, string(s))
	}
	return out
}

func timeNow() int64 {
	return time.Now().Unix()
}
