package database

import (
	"os"
	"strings"

	"github.com/voidshard/igor/pkg/structs"

	r "gopkg.in/rethinkdb/rethinkdb-go.v6"
)

const (
	keyID      = "id"
	keyJobID   = "JobID"
	keyLayerID = "LayerID"
	keyTaskID  = "TaskID"
	keyPaused  = "PausedAt"
	keyStatus  = "Status"
	keyETag    = "ETag"
)

type closable interface {
	Close() error
}

type RethinkImpl struct {
	opts    *Options
	session *r.Session
	klose   []closable
}

type rthinkDeltaLayer struct {
	New *structs.Layer `gorethink:"new_val"`
	Old *structs.Layer `gorethink:"old_val"`
}

type rthinkDeltaTask struct {
	New *structs.Task `gorethink:"new_val"`
	Old *structs.Task `gorethink:"old_val"`
}

type rthinkDeltaRun struct {
	New *structs.Run `gorethink:"new_val"`
	Old *structs.Run `gorethink:"old_val"`
}

func NewRethinkImpl(opts *Options) (*RethinkImpl, error) {
	if opts.Address == "" {
		opts.Address = "localhost:28015"
	}
	if opts.Database == "" {
		opts.Database = "igor"
	}
	if opts.Password == "" {
		opts.Password = os.Getenv("DB_PASSWORD")
	}
	session, err := r.Connect(r.ConnectOpts{
		Address:    opts.Address,
		Database:   opts.Database,
		Username:   opts.Username,
		Password:   opts.Password,
		InitialCap: 10,
		MaxOpen:    10,
	})
	if err != nil {
		return nil, err
	}
	me := &RethinkImpl{
		opts:    opts,
		session: session,
		klose:   []closable{},
	}
	return me, me.setup()
}

func (re *RethinkImpl) setup() error {
	// Create database if it doesn't exist
	_, err := r.DBCreate(re.opts.Database).RunWrite(re.session)
	if err != nil && !isErrExists(err) {
		return err
	}

	// Create tables if they don't exist
	for _, table := range []string{
		string(structs.KindJob),
		string(structs.KindLayer),
		string(structs.KindTask),
		string(structs.KindRun),
	} {
		_, err := r.DB(re.opts.Database).TableCreate(table).RunWrite(re.session)
		if err != nil && !isErrExists(err) {
			return err
		}
	}
	return nil
}

func (re *RethinkImpl) Changes() (<-chan *Change, error) {
	// We care about any changes that mean we have to interact with the Queue system.
	// Ie. something that means we have to launch or stop a task.
	out := make(chan *Change)

	// channels to receive data from rethink
	layerDelta, err := r.DB(re.opts.Database).Table(string(structs.KindLayer)).Changes().Run(re.session)
	if err != nil {
		return nil, err
	}
	taskDelta, err := r.DB(re.opts.Database).Table(string(structs.KindTask)).Changes().Run(re.session)
	if err != nil {
		return nil, err
	}
	runDelta, err := r.DB(re.opts.Database).Table(string(structs.KindRun)).Changes().Run(re.session)
	if err != nil {
		return nil, err
	}

	// the set of stuff we need to close on exit
	re.klose = append(re.klose, layerDelta, taskDelta, runDelta)

	// pump from rethink into our internal channel
	go func() {
		defer close(out)
		obj := &rthinkDeltaLayer{}
		for layerDelta.Next(obj) {
			out <- &Change{
				Kind: structs.KindLayer,
				Old:  obj.Old,
				New:  obj.New,
			}
		}
	}()
	go func() {
		obj := &rthinkDeltaTask{}
		for layerDelta.Next(obj) {
			out <- &Change{
				Kind: structs.KindTask,
				Old:  obj.Old,
				New:  obj.New,
			}
		}
	}()
	go func() {
		obj := &rthinkDeltaRun{}
		for layerDelta.Next(obj) {
			out <- &Change{
				Kind: structs.KindRun,
				Old:  obj.Old,
				New:  obj.New,
			}
		}
	}()

	return out, nil
}

func (re *RethinkImpl) Close() error {
	for _, k := range re.klose {
		k.Close()
	}
	return re.session.Close()
}

func (re *RethinkImpl) Jobs(q *structs.Query) (result []*structs.Job, err error) {
	call := r.DB(re.opts.Database).Table(string(structs.KindJob))
	if q.JobIDs != nil { // the other filters are not relevant
		call = call.GetAll(toListInterface(q.JobIDs)...)
	}
	cursor, err := call.OrderBy(keyID).Skip(q.Offset).Limit(q.Limit).Run(re.session)
	if err != nil {
		defer cursor.Close()
	}
	return result, cursor.All(&result)
}

func (re *RethinkImpl) Layers(q *structs.Query) (result []*structs.Layer, err error) {
	call := r.DB(re.opts.Database).Table(string(structs.KindLayer))
	if q.LayerIDs != nil {
		call = call.GetAll(toListInterface(q.LayerIDs)...)
	} else if q.JobIDs != nil {
		call = call.Filter(toReOrFilter(keyJobID, q.JobIDs))
	}
	cursor, err := call.OrderBy(keyID).Skip(q.Offset).Limit(q.Limit).Run(re.session)
	if err != nil {
		defer cursor.Close()
	}
	return result, cursor.All(&result)
}

func (re *RethinkImpl) Tasks(q *structs.Query) (result []*structs.Task, err error) {
	call := r.DB(re.opts.Database).Table(string(structs.KindTask))
	if q.TaskIDs != nil {
		call = call.GetAll(toListInterface(q.TaskIDs)...)
	} else {
		if q.JobIDs != nil {
			call = call.Filter(toReOrFilter(keyJobID, q.JobIDs))
		}
		if q.LayerIDs != nil {
			call = call.Filter(toReOrFilter(keyLayerID, q.LayerIDs))
		}
	}
	cursor, err := call.OrderBy(keyID).Skip(q.Offset).Limit(q.Limit).Run(re.session)
	if err != nil {
		defer cursor.Close()
	}
	return result, cursor.All(&result)
}

func (re *RethinkImpl) Runs(q *structs.Query) (result []*structs.Run, err error) {
	call := r.DB(re.opts.Database).Table(string(structs.KindRun))
	if q.RunIDs != nil {
		call = call.GetAll(toListInterface(q.RunIDs)...)
	} else {
		if q.JobIDs != nil {
			call = call.Filter(toReOrFilter(keyJobID, q.JobIDs))
		}
		if q.LayerIDs != nil {
			call = call.Filter(toReOrFilter(keyLayerID, q.LayerIDs))
		}
		if q.TaskIDs != nil {
			call = call.Filter(toReOrFilter(keyTaskID, q.TaskIDs))
		}
	}
	cursor, err := call.OrderBy(keyID).Skip(q.Offset).Limit(q.Limit).Run(re.session)
	if err != nil {
		defer cursor.Close()
	}
	return result, cursor.All(&result)
}

func (re *RethinkImpl) InsertJobs(in []*structs.Job) error {
	_, err := r.DB(re.opts.Database).Table(string(structs.KindJob)).Insert(in).RunWrite(re.session)
	return err
}

func (re *RethinkImpl) InsertLayers(in []*structs.Layer) error {
	_, err := r.DB(re.opts.Database).Table(string(structs.KindLayer)).Insert(in).RunWrite(re.session)
	return err
}

func (re *RethinkImpl) InsertTasks(in []*structs.Task) error {
	_, err := r.DB(re.opts.Database).Table(string(structs.KindTask)).Insert(in).RunWrite(re.session)
	return err
}

func (re *RethinkImpl) InsertRuns(in []*structs.Run) error {
	_, err := r.DB(re.opts.Database).Table(string(structs.KindRun)).Insert(in).RunWrite(re.session)
	return err
}

func (re *RethinkImpl) SetLayersPaused(at int64, newTag string, ids []*IDTag) (int64, error) {
	info, err := r.DB(re.opts.Database).Table(string(structs.KindLayer)).Filter(toReQuery(ids)).Update(map[string]interface{}{keyPaused: at, keyETag: newTag}).RunWrite(re.session)
	if err != nil {
		return 0, err
	}
	return int64(info.Replaced + info.Updated), err
}

func (re *RethinkImpl) SetTasksPaused(at int64, newTag string, ids []*IDTag) (int64, error) {
	info, err := r.DB(re.opts.Database).Table(string(structs.KindTask)).Filter(toReQuery(ids)).Update(map[string]interface{}{keyPaused: at, keyETag: newTag}).RunWrite(re.session)
	if err != nil {
		return 0, err
	}
	return int64(info.Replaced + info.Updated), err
}

func (re *RethinkImpl) SetJobsStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error) {
	info, err := r.DB(re.opts.Database).Table(string(structs.KindJob)).Filter(toReQuery(ids)).Update(map[string]interface{}{keyStatus: status, keyETag: newTag}).RunWrite(re.session)
	if err != nil {
		return 0, err
	}
	return int64(info.Replaced + info.Updated), err
}

func (re *RethinkImpl) SetLayersStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error) {
	info, err := r.DB(re.opts.Database).Table(string(structs.KindLayer)).Filter(toReQuery(ids)).Update(map[string]interface{}{keyStatus: status, keyETag: newTag}).RunWrite(re.session)
	if err != nil {
		return 0, err
	}
	return int64(info.Replaced + info.Updated), err
}

func (re *RethinkImpl) SetTasksStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error) {
	info, err := r.DB(re.opts.Database).Table(string(structs.KindTask)).Filter(toReQuery(ids)).Update(map[string]interface{}{keyStatus: status, keyETag: newTag}).RunWrite(re.session)
	if err != nil {
		return 0, err
	}
	return int64(info.Replaced + info.Updated), err
}

func (re *RethinkImpl) SetRunsStatus(status structs.Status, newTag string, ids []*IDTag) (int64, error) {
	info, err := r.DB(re.opts.Database).Table(string(structs.KindRun)).Filter(toReQuery(ids)).Update(map[string]interface{}{keyStatus: status, keyETag: newTag}).RunWrite(re.session)
	if err != nil {
		return 0, err
	}
	return int64(info.Replaced + info.Updated), err
}

func isErrExists(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "already exists")
}

// returns a RethinkDB query that is an OR of all the values
// Ie. field == values[0] OR field == values[1] OR ...
func toReOrFilter(field string, values []string) interface{} {
	if len(values) == 0 {
		return nil
	} else if len(values) == 1 {
		return r.Row.Field(field).Eq(values[0])
	}
	q := r.Or(r.Row.Field(field).Eq(values[0]))
	for _, value := range values[1:] {
		q = q.Or(r.Row.Field(field).Eq(value))
	}
	return q
}

// returns a RethinkDB query that is an OR (ID AND ETag) of all the values
// Ie. (ID == ids[0].ID AND ETag == ids[0].ETag) OR (ID == ids[1].ID AND ETag == ids[1].ETag) OR ...
func toReQuery(ids []*IDTag) interface{} {
	if len(ids) == 0 {
		return nil
	} else if len(ids) == 1 {
		return r.Row.Field(keyID).Eq(ids[0].ID).And(r.Row.Field(keyETag).Eq(ids[0].ETag))
	}
	q := r.Or(r.Row.Field(keyID).Eq(ids[0].ID).And(r.Row.Field(keyETag).Eq(ids[0].ETag)))
	for _, id := range ids[1:] {
		q = q.Or(r.Row.Field(keyID).Eq(id.ID).And(r.Row.Field(keyETag).Eq(id.ETag)))
	}
	return q
}

func toListInterface(in []string) []interface{} {
	// This is so dumb..
	l := make([]interface{}, len(in))
	for i, v := range in {
		l[i] = v
	}
	return l
}
