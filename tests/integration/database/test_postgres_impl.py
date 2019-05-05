import pytest
import time
import uuid

from igor import domain
from igor import enums
from igor import exceptions as exc
from igor.database.postgres_impl import PostgresDB, _Transaction

from integration import utils


class DatabaseTest:
    """Defines a standard set of tests for a db to complete.

    Inherit & override the setup / tear down and clear functions.

    """

    db = None
    db_container = None
    conn = None

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    @classmethod
    def clear_db(cls):
        pass

    def _user(self, is_admin=False):
        """Create test user

        :return: User

        """
        u = domain.User()
        u.name = uuid.uuid4().hex
        u.password = "foobar"
        u.is_admin = is_admin
        self.db.create_user(u)
        return u

    def _job(self, key=None, meta=None, user=None):
        """Create test job

        :param key:
        :return: Job

        """
        if not user:
            user = self._user()

        l = domain.Layer(key=key)
        l.user_id = user.id
        l.metadata = meta or {}

        t = domain.Task(key=key)
        t.user_id = user.id
        t.layer_id = l.id
        t.type = "sometype"
        t.metadata = meta or {}

        j = domain.Job(key=key)
        j.user_id = user.id
        j.metadata = meta or {}

        l.job_id = j.id

        self.db.create_job(j, [l], [t])
        return j

    def _layer(self, key=None, meta=None, user=None):
        """Create test layer

        :param key:
        :return: Layer

        """
        if not user:
            user = self._user()

        l = domain.Layer(key=key)
        l.user_id = user.id
        l.metadata = meta or {}

        j = domain.Job(key=key)
        j.user_id = user.id
        j.metadata = meta or {}

        l.job_id = j.id

        self.db.create_job(j, [l], [])
        return l

    def _task(self, key=None, meta=None, user=None):
        """Create test task

        :param key:
        :return: Task

        """
        if not user:
            user = self._user()

        l = domain.Layer(key=key)
        l.metadata = meta or {}
        l.user_id = user.id

        t = domain.Task(key=key)
        t.layer_id = l.id
        t.type = "sometype"
        t.user_id = user.id
        t.metadata = meta or {}

        j = domain.Job(key=key)
        j.user_id = user.id
        j.metadata = meta or {}

        l.job_id = j.id
        t.job_id = j.id

        self.db.create_job(j, [l], [t])

        return t

    def _worker(self, key=None):
        w = domain.Worker(key=key)
        self.db.create_worker(w)
        return w

    def test_idle_workers_ok(self):
        # arrange
        self.clear_db()
        workers = sorted([
            self._worker().id,
            self._worker().id,
            self._worker().id,
            self._worker().id,
            self._worker().id,
        ])

        # act
        result = self.db.idle_workers(len(workers) + 100, 0)

        # assert
        assert sorted([i.id for i in result]) == workers

    def test_idle_workers_ignores_those_working(self):
        # arrange
        self.clear_db()

        task = self._task()

        workers = sorted([
            self._worker().id,
            self._worker().id,
            self._worker().id,
        ])

        processing = [
            self._worker().id,
            self._worker().id,
        ]
        q = domain.Query(filters=[domain.Filter(worker_ids=processing)])
        for w in self.db.get_workers(q):
            self.db.update_worker(
                w.id,
                w.etag,
                job_id=task.job_id,
                layer_id=task.layer_id,
                task_id=task.id,
                task_started=time.time(),
            )

        # act
        result = self.db.idle_workers(len(workers) + 100, 0)
        result_ids = sorted([i.id for i in result])

        # assert
        assert result_ids == workers
        for id in processing:
            assert id not in result_ids

    def test_idle_worker_count_ok(self):
        # arrange
        self.clear_db()

        workers = [
            self._worker(),
            self._worker(),
            self._worker(),
            self._worker(),
            self._worker(),
        ]

        # act
        result = self.db.idle_worker_count()

        # assert
        assert result == len(workers)

    def test_create_user_enforces_unique_name(self):
        # arrange
        self.clear_db()
        u = self._user()

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.create_user(u)

    def test_idle_worker_count_ignores_those_working(self):
        # arrange
        self.clear_db()

        task = self._task()

        workers = [  # make 5 workers
            self._worker(),
            self._worker(),
            self._worker(),
            self._worker(),
            self._worker(),
        ]

        processing = [  # record that 2 of them are working
            workers[-1].id,
            workers[-2].id,
        ]
        q = domain.Query(filters=[domain.Filter(worker_ids=processing)])
        for w in self.db.get_workers(q):
            self.db.update_worker(
                w.id,
                w.etag,
                job_id=task.job_id,
                layer_id=task.layer_id,
                task_id=task.id,
                task_started=time.time(),
            )

        # act
        result = self.db.idle_worker_count()

        # assert
        assert result == len(workers) - len(processing)

    def test_update_worker_ok(self):
        # arrange
        w = self._worker()
        q = domain.Query(filters=[domain.Filter(worker_ids=[w.id])], limit=1)
        task = self._task()

        old_tag = w.etag
        expect = {
            "job_id": task.job_id,
            "layer_id": task.layer_id,
            "task_id": task.id,
            "task_started": int(time.time()),
            "task_finished": int(time.time()),
            "last_ping": int(time.time()),
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
        }

        # act
        self.db.update_worker(
            w.id,
            old_tag,
            **expect
        )

        result = self.db.get_workers(q)[0]

        # assert
        assert result.id == w.id
        assert result.etag != old_tag
        assert {
           "job_id": result.job_id,
           "layer_id": result.layer_id,
           "task_id": result.task_id,
           "task_started": result.task_started,
           "task_finished": result.task_finished,
           "last_ping": result.last_ping,
           "metadata": result.metadata,
        } == expect

    def test_delete_worker_ok(self):
        # arrange
        w = self._worker()
        q = domain.Query(filters=[domain.Filter(worker_ids=[w.id])], limit=1)
        before = self.db.get_workers(q)

        # act
        self.db.delete_worker(w.id)
        after = self.db.get_workers(q)

        # assert
        assert len(before) == 1
        assert len(after) == 0

    def test_create_worker_ok(self):
        # arrange
        w = domain.Worker()
        q = domain.Query(filters=[domain.Filter(worker_ids=[w.id])], limit=1)

        # act
        self.db.create_worker(w)
        result = self.db.get_workers(q)

        # assert
        assert result
        assert len(result) == 1
        assert isinstance(result[0], domain.Worker)

    @pytest.mark.parametrize("given, expect", [
        ("Foobar", bytes("Foobar", encoding="utf8")),
        ({"foo": "bar"}, bytes(str({"foo": "bar"}), encoding="utf8")),
        ([1, "foo", True, "bar"], bytes(str([1, "foo", True, "bar"]), encoding="utf8")),
        (b"Foobar", bytes("Foobar", encoding="utf8")),
        (56789, bytes('56789', encoding="utf8")),
        (5.132, bytes('5.132', encoding="utf8")),
        (True, bytes('True', encoding="utf8")),
        (None, None),
    ])
    def test_update_task_result_ok(self, given, expect):
        # arrange
        t = self._task()
        q = domain.Query(filters=[domain.Filter(task_ids=[t.id])], limit=1)

        # act
        self.db.update_task(t.id, t.etag, result=given)

        task = self.db.get_tasks(q)[0]

        # assert
        assert task.id == t.id
        if expect is None:
            assert task.result is None
        else:
            assert task.result == str(expect, encoding='utf8')

    def test_update_task_result_raises_on_invalid_type(self):
        # arrange
        t = self._task()

        class Foo:
            pass

        # act & assert
        with pytest.raises(exc.InvalidArg):
            self.db.update_task(t.id, t.etag, result=Foo())

    def test_task_update_ok(self):
        # arrange
        t = self._task()
        q = domain.Query(filters=[domain.Filter(task_ids=[t.id])], limit=1)

        old_tag = t.etag
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": enums.State.PENDING.value,
        }

        # act
        tag = self.db.update_task(t.id, t.etag, **expect)

        task = self.db.get_tasks(q)[0]

        # assert
        assert tag != old_tag
        assert task.etag == tag
        assert {
            "runner_id": task.runner_id,
            "metadata": task.metadata,
            "state": task.state,
        } == expect

    def test_task_update_raises_on_invalid_state(self):
        # arrange
        t = self._task()

        # act
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": "FOO",
        }

        # act & assert
        with pytest.raises(exc.InvalidState):
            self.db.update_task(t.id, t.etag, **expect)

    def test_task_update_raises_on_invalid_id(self):
        # arrange
        t = self._task()

        # act
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": enums.State.RUNNING.value,
        }

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.update_task("??", t.etag, **expect)

    def test_task_update_raises_on_invalid_etag(self):
        # arrange
        t = self._task()

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.update_task(t.id, "lolwhat", runner_id="something")

    def test_task_update_permits_write_with_null_etag(self):
        # arrange
        t = self._task()
        q = domain.Query(filters=[domain.Filter(task_ids=[t.id])], limit=1)

        # act
        self.db.update_task(t.id, None, runner_id="something")

        task = self.db.get_tasks(q)[0]

        # assert
        assert task.id == t.id
        assert task.etag != t.etag
        assert task.runner_id == 'something'

    def test_task_create_ok(self):
        # arrange
        l = self._layer()

        t = domain.Task()
        t.type = "sometype"
        t.job_id = l.job_id
        t.layer_id = l.id
        q = domain.Query(filters=[domain.Filter(task_ids=[t.id])], limit=1)

        # act
        self.db.create_tasks(l.id, [t])

        tasks = self.db.get_tasks(q)

        # assert
        assert len(tasks) == 1
        assert isinstance(tasks[0], domain.Task)
        assert tasks[0].id == t.id
        assert tasks[0].job_id == t.job_id
        assert tasks[0].layer_id == t.layer_id

    def test_layer_update_raises_on_invalid_id(self):
        """We should be forbidden to write if we give an invalid id
        """
        # arrange
        l = self._layer()

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.update_layer("???", l.etag, runner_id="fooboo")

    def test_layer_update_raises_on_invalid_state(self):
        """We should be forbidden to write if we give an invalid id
        """
        # arrange
        l = self._layer()

        # act & assert
        with pytest.raises(exc.InvalidState):
            self.db.update_layer(l.id, l.etag, state="FOOBAR")

    def test_layer_update_permitted_on_null_etag(self):
        """We should be forbidden to write if we give an invalid etag
        """
        # arrange
        l = self._layer()
        q = domain.Query(filters=[domain.Filter(layer_ids=[l.id])], limit=1)

        # act
        self.db.update_layer(l.id, None, runner_id="fooboo")

        layer = self.db.get_layers(q)[0]

        # assert
        assert l.id == layer.id
        assert l.etag != layer.etag
        assert layer.runner_id == 'fooboo'

    def test_layer_update_raises_on_invalid_etag(self):
        """We should be forbidden to write if we give an invalid etag
        """
        # arrange
        l = self._layer()

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.update_layer(l.id, "garbage", runner_id="fooboo")

    def test_layer_update_ok(self):
        # arrange
        l = self._layer()
        q = domain.Query(filters=[domain.Filter(layer_ids=[l.id])], limit=1)

        # act
        old_tag = l.etag
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": enums.State.PENDING.value,
            "priority": 10000,
        }

        # act
        tag = self.db.update_layer(l.id, l.etag, **expect)

        layer = self.db.get_layers(q)[0]

        # assert
        assert tag != old_tag
        assert layer.etag == tag
        assert {
            "runner_id": layer.runner_id,
            "metadata": layer.metadata,
            "state": layer.state,
            "priority": layer.priority,
        } == expect

    def test_job_update_raises_on_invalid_id(self):
        """We should be forbidden to write if we give an invalid id
        """
        # arrange
        j = self._job()

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.update_job("???", j.etag, runner_id="fooboo")

    def test_job_update_raises_on_invalid_state(self):
        """We should be forbidden to write if we give an invalid etag
        """
        # arrange
        j = self._job()

        # act & assert
        with pytest.raises(exc.InvalidState):
            self.db.update_job(j.id, j.etag, state="foobar")

    def test_job_update_raises_on_invalid_etag(self):
        """We should be forbidden to write if we give an invalid etag
        """
        # arrange
        j = self._job()

        # act & assert
        with pytest.raises(exc.WriteConflictError):
            self.db.update_job(j.id, "garbage", runner_id="fooboo")

    def test_job_update_permits_write_on_null_etag(self):
        # arrange
        j = self._job()
        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        # act
        self.db.update_job(j.id, None, runner_id="fooboo")

        job = self.db.get_jobs(q)[0]

        # assert
        assert j.id == job.id
        assert j.etag != job.etag
        assert job.runner_id == "fooboo"

    def test_job_update_ok(self):
        # arrange
        j = self._job()
        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        old_tag = j.etag
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": enums.State.QUEUED.value,
        }

        # act
        tag = self.db.update_job(j.id, j.etag, **expect)

        job = self.db.get_jobs(q)[0]

        # assert
        assert tag != old_tag
        assert job.etag == tag
        assert {
            "runner_id": job.runner_id,
            "metadata": job.metadata,
            "state": job.state,
        } == expect

    def test_job_force_delete_ok(self):
        # arrange
        j = self._job()
        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        before_delete = self.db.get_jobs(q)

        # act
        self.db.force_delete_job(j.id)

        after_delete = self.db.get_jobs(q)

        # assert
        assert len(before_delete) == 1
        assert len(after_delete) == 0

    def test_job_create_ok(self):
        # arrange
        u = self._user()

        l = domain.Layer()
        l.user_id = u.id

        j = domain.Job(key="mykey")
        j.user_id = u.id
        l.job_id = j.id

        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        # act
        self.db.create_job(j, [l], [])

        jobs = self.db.get_jobs(q)

        # assert
        assert len(jobs) == 1
        assert isinstance(jobs[0], domain.Job)
        assert jobs[0].id == j.id


class TestPostgres(DatabaseTest):
    """Standard DB tests, using Postgres
    """

    @classmethod
    def setup_class(cls):
        # start ourselves a new container w/ empty db
        client = utils.Client()
        cls.db_container = client.postgres_container()

        # connect to the container
        cls.db = PostgresDB(host="localhost")
        cls.conn = cls.db._conn

    @classmethod
    def teardown_class(cls):
        cls.db_container.stop()
        time.sleep(3)  # wait for db to shutdown

    @classmethod
    def clear_db(cls):
        """Helper func to drop everything in the db.

        """
        cur = cls.conn.cursor()

        # update this table to remove any FK problems
        cur.execute((
            f"UPDATE {_Transaction._T_WKR} "
            f"SET str_task_id=null, "
            f"str_layer_id=null, "
            f"str_job_id=null;"
        ))

        for table in [  # remove all data, order matters
            _Transaction._T_REC,
            _Transaction._T_WKR,
            _Transaction._T_TSK,
            _Transaction._T_LYR,
            _Transaction._T_JOB,
            _Transaction._T_USR,
        ]:
            cur.execute(f"DELETE FROM {table};")

        cls.conn.commit()
