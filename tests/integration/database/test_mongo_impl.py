import pytest
import time
import uuid

from igor import domain
from igor import exceptions as exc
from igor.database.mongo_impl import MongoDB

from integration import utils


class TestMongo:

    db = None
    db_container = None
    CONTAINER_START_WAIT_TIME = 1

    @classmethod
    def setup_class(cls):
        # start ourselves a new container & give it some time to get ready
        client = utils.Client()
        cls.db_container = client.mongo_container()
        cls.db_container.start()
        time.sleep(cls.CONTAINER_START_WAIT_TIME)

        # connect to the container
        cls.db = MongoDB(host="localhost", archive_mode=True)

    @classmethod
    def teardown_class(cls):
        try:
            cls.db_container.stop()
        except:
            pass

    @classmethod
    def clear_db(cls):
        """Helper func to drop everything in the db.

        This uses mongo specific knowledge .. but this IS the TestMongo class.

        """
        cls.db._conn[MongoDB._DB][MongoDB._T_JOB].remove({})
        cls.db._conn[MongoDB._DB][MongoDB._T_LYR].remove({})
        cls.db._conn[MongoDB._DB][MongoDB._T_TSK].remove({})
        cls.db._conn[MongoDB._DB][MongoDB._T_WKR].remove({})
        cls.db._conn[MongoDB._DB][MongoDB._A_JOB].remove({})
        cls.db._conn[MongoDB._DB][MongoDB._A_LYR].remove({})
        cls.db._conn[MongoDB._DB][MongoDB._A_TSK].remove({})

    def _job(self, key=None, meta=None):
        """Create test job

        :param key:
        :return: Job

        """
        l = domain.Layer(key=key)
        l.metadata = meta or {}

        t = domain.Task(key=key)
        t.layer_id = l.id
        t.type = "sometype"
        t.metadata = meta or {}

        j = domain.Job(key=key)
        j.metadata = meta or {}
        self.db.create_job(j, [l], [t])
        return j

    def _layer(self, key=None, meta=None):
        """Create test layer

        :param key:
        :return: Layer

        """
        l = domain.Layer(key=key)
        l.metadata = meta or {}

        j = domain.Job(key=key)
        j.metadata = meta or {}
        self.db.create_job(j, [l], [])

        return l

    def _task(self, key=None, meta=None):
        """Create test task

        :param key:
        :return: Task

        """
        l = domain.Layer(key=key)
        l.metadata = meta or {}

        t = domain.Task(key=key)
        t.layer_id = l.id
        t.type = "sometype"
        t.metadata = meta or {}

        j = domain.Job(key=key)
        j.metadata = meta or {}
        self.db.create_job(j, [l], [t])

        return t

    def _worker(self, key=None):
        w = domain.Worker(key=key)
        self.db.create_worker(w)
        return w

    def test_force_delete_archives(self):
        # arrange
        j = self._job()

        # act
        self.db.force_delete_job(j.id)

        job_docs = list(self.db._conn[MongoDB._DB][MongoDB._A_JOB].find({}))
        lyr_docs = list(self.db._conn[MongoDB._DB][MongoDB._A_LYR].find({}))
        tsk_docs = list(self.db._conn[MongoDB._DB][MongoDB._A_TSK].find({}))

        # assert
        assert len(job_docs) == 1
        assert len(lyr_docs) == 1
        assert len(tsk_docs) == 1

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
                job_id=str(uuid.uuid4()),
                layer_id=str(uuid.uuid4()),
                task_id=str(uuid.uuid4()),
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
        u = domain.User(name="Bob")
        self.db.create_user(u)

        # act & assert
        with pytest.raises(exc.WriteFailError):
            self.db.create_user(u)

    def test_idle_worker_count_ignores_those_working(self):
        # arrange
        self.clear_db()

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
                job_id=str(uuid.uuid4()),
                layer_id=str(uuid.uuid4()),
                task_id=str(uuid.uuid4()),
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

        old_tag = w.etag
        expect = {
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
            "task_started": time.time(),
            "task_finished": time.time(),
            "last_ping": time.time(),
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

    def test_task_update_ok(self):
        # arrange
        t = self._task()
        q = domain.Query(filters=[domain.Filter(task_ids=[t.id])], limit=1)

        # act
        old_tag = t.etag
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": "FOOBAR",
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

    def test_task_delete_ok(self):
        # arrange
        t = self._task()
        q = domain.Query(filters=[domain.Filter(task_ids=[t.id])], limit=1)

        tasks_before = self.db.get_tasks(q)

        # act
        self.db.delete_tasks([t.id])

        tasks_after = self.db.get_tasks(q)

        # assert
        assert len(tasks_before) == 1
        assert len(tasks_after) == 0

    def test_task_create_ok(self):
        # arrange
        l = self._layer()

        t = domain.Task()
        t.type = "sometype"
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
        with pytest.raises(exc.WriteFailError):
            self.db.update_layer("???", l.etag, runner_id="fooboo")

    def test_layer_update_raises_on_invalid_etag(self):
        """We should be forbidden to write if we give an invalid etag
        """
        # arrange
        l = self._layer()

        # act & assert
        with pytest.raises(exc.WriteFailError):
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
            "state": "FOOBAR",
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
        with pytest.raises(exc.WriteFailError):
            self.db.update_job("???", j.etag, runner_id="fooboo")

    def test_job_update_raises_on_invalid_etag(self):
        """We should be forbidden to write if we give an invalid etag
        """
        # arrange
        j = self._job()

        # act & assert
        with pytest.raises(exc.WriteFailError):
            self.db.update_job(j.id, "garbage", runner_id="fooboo")

    def test_job_update_ok(self):
        # arrange
        j = self._job()
        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        old_tag = j.etag
        expect = {
            "runner_id": "abc1234567890",
            "metadata": {"ab": "cd", "a": True, "b": False, "c": 1},
            "state": "FOOBAR",
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

    def test_job_delete_ok(self):
        # arrange
        j = self._job()
        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        before_delete = self.db.get_jobs(q)

        # act
        self.db.delete_jobs([j.id])

        after_delete = self.db.get_jobs(q)

        # assert
        assert len(before_delete) == 1
        assert len(after_delete) == 0

    def test_job_create_ok(self):
        # arrange
        l = domain.Layer()

        j = domain.Job(key="mykey")
        q = domain.Query(filters=[domain.Filter(job_ids=[j.id])], limit=1)

        # act
        self.db.create_job(j, [l], [])

        jobs = self.db.get_jobs(q)

        # assert
        assert len(jobs) == 1
        assert isinstance(jobs[0], domain.Job)
        assert jobs[0].id == j.id
