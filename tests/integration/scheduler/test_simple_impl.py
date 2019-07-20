import time
import pytest
import uuid

from igor import domain
from igor import enums
from igor import exceptions as exc
from igor.runner import DefaultRunner
from igor.scheduler.simple_impl import Simple
from igor import service

from integration import utils


class SchedulerTest:
    """Generic tests for our abstract functions. All non abstract
    subclasses of the base scheduler should satisfy these.

    """

    redis_container = None
    db_container = None
    scheduler = None
    db = None
    svc = None
    runner = None
    queue = None

    def dequeue_all(self):
        count = 0
        while self.runner.queued_tasks():
            self.queue.pop(self.runner._QUEUE_MAIN)
            count += 1
        return count

    @classmethod
    def clear_db(cls):
        """Helper func to drop everything in the db.

        """
        pass

    def teardown_method(self, test_method):
        self.dequeue_all()
        self.clear_db()

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

    def _job(
        self,
        job_paused=False,
        layer_paused=False,
        tasks_paused=False,
        tasks=10, 
        layer_state=enums.State.PENDING.value,
        task_state=enums.State.PENDING.value,
        task_max_attempts=2,
    ):
        """Create test job

        """
        user = self._user()

        l = domain.Layer()
        l.user_id = user.id
        l.state = layer_state
        l.paused = time.time() if layer_paused else None
        l.metadata = {}

        task_list = []
        for i in range(0, tasks):
            t = domain.Task()
            t.user_id = user.id
            t.paused = time.time() if tasks_paused else None
            t.layer_id = l.id
            t.type = "sometype"
            t.state = task_state
            t.metadata = {}
            t.max_attempts = task_max_attempts
            task_list.append(t)

        j = domain.Job()
        j.user_id = user.id
        j.metadata = {}
        j.paused = time.time() if job_paused else None

        l.job_id = j.id

        self.db.create_job(j, [l], task_list)
        return j, l, task_list

    def _worker(self):
        w = domain.Worker()
        self.db.create_worker(w)
        return w

    @classmethod
    def teardown_class(cls):
        try:
            cls.db_container.stop()
        except:
            pass

        try:
            cls.redis_container.stop()
        except:
            pass

    def test_archive_completed_jobs(self):
        # arrange
        user = self._user()

        l1 = domain.Layer()
        l1.state = enums.State.COMPLETED.value
        l1.user_id = user.id

        j = domain.Job()
        j.state = enums.State.COMPLETED.value
        j.user_id = user.id
        l1.job_id = j.id

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])
        self.db.create_job(j, [l1], [])

        # act
        self.scheduler.archive_completed_jobs()

        # assert
        assert not self.db.get_jobs(query)

    def test_multiparent_layers_one_parents_errored(self):
        # arrange
        user = self._user()

        l1 = domain.Layer()
        l1.state = enums.State.RUNNING.value
        l1.user_id = user.id
        l1.metadata = {}

        l2 = domain.Layer()
        l2.state = enums.State.ERRORED.value
        l2.user_id = user.id
        l2.metadata = {}

        l3 = domain.Layer()
        l3.state = enums.State.PENDING.value
        l3.user_id = user.id
        l3.metadata = {}

        #
        #  L1  L2
        #  |   |
        #  +-+-+
        #    |
        #    L3
        #
        l1.siblings = [l2.id]
        l1.children = [l3.id]
        l2.siblings = [l1.id]
        l2.children = [l3.id]
        l3.parents = [l1.id, l2.id]

        def task(layer_id, state):
            t = domain.Task()
            t.user_id = user.id
            t.state = state
            t.layer_id = layer_id
            t.type = "sometype"
            t.metadata = {}
            t.max_attempts = 0
            return t

        j = domain.Job()
        j.state = enums.State.RUNNING.value
        j.user_id = user.id
        j.metadata = {}
        l1.job_id = j.id
        l2.job_id = j.id
        l3.job_id = j.id

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])
        self.db.create_job(
            j,
            [l1, l2, l3],
            [
                task(l1.id, enums.State.COMPLETED.value),
                task(l2.id, enums.State.ERRORED.value),
                task(l3.id, enums.State.PENDING.value),
            ]
        )

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)

        assert jobs[0].state == enums.State.ERRORED.value
        assert {
            l1.id: enums.State.COMPLETED.value,
            l2.id: enums.State.ERRORED.value,
            l3.id: enums.State.PENDING.value,
        } == {l.id: l.state for l in layers}

    def test_multiparent_layers_both_parents_complete(self):
        # arrange
        user = self._user()

        l1 = domain.Layer()
        l1.state = enums.State.RUNNING.value
        l1.user_id = user.id
        l1.metadata = {}

        l2 = domain.Layer()
        l2.state = enums.State.RUNNING.value
        l2.user_id = user.id
        l2.metadata = {}

        l3 = domain.Layer()
        l3.state = enums.State.PENDING.value
        l3.user_id = user.id
        l3.metadata = {}

        #
        #  L1  L2
        #  |   |
        #  +-+-+
        #    |
        #    L3
        #
        l1.siblings = [l2.id]
        l1.children = [l3.id]
        l2.siblings = [l1.id]
        l2.children = [l3.id]
        l3.parents = [l1.id, l2.id]

        def task(layer_id, state):
            t = domain.Task()
            t.user_id = user.id
            t.state = state
            t.layer_id = layer_id
            t.type = "sometype"
            t.metadata = {}
            t.max_attempts = 0
            return t

        j = domain.Job()
        j.state = enums.State.RUNNING.value
        j.user_id = user.id
        j.metadata = {}
        l1.job_id = j.id
        l2.job_id = j.id
        l3.job_id = j.id

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])
        self.db.create_job(
            j,
            [l1, l2, l3],
            [
                task(l1.id, enums.State.COMPLETED.value),
                task(l2.id, enums.State.COMPLETED.value),
                task(l3.id, enums.State.PENDING.value),
            ]
        )

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)

        assert jobs[0].state == enums.State.RUNNING.value
        assert {
            l1.id: enums.State.COMPLETED.value,
            l2.id: enums.State.COMPLETED.value,
            l3.id: enums.State.QUEUED.value,
        } == {l.id: l.state for l in layers}

    def test_multichild_layers_parent_completes(self):
        # arrange
        user = self._user()

        l1 = domain.Layer()
        l1.state = enums.State.RUNNING.value
        l1.user_id = user.id
        l1.metadata = {}

        l2 = domain.Layer()
        l2.state = enums.State.PENDING.value
        l2.user_id = user.id
        l2.metadata = {}

        l3 = domain.Layer()
        l3.state = enums.State.PENDING.value
        l3.user_id = user.id
        l3.metadata = {}

        #
        #    L1
        #    |
        #  +-+-+
        #  |   |
        #  L2  L3
        #
        l1.siblings = []
        l1.children = [l2.id, l3.id]
        l2.siblings = [l3.id]
        l2.parents = [l1.id]
        l3.siblings = [l2.id]
        l3.parents = [l1.id]

        def task(layer_id, state):
            t = domain.Task()
            t.user_id = user.id
            t.state = state
            t.layer_id = layer_id
            t.type = "sometype"
            t.metadata = {}
            t.max_attempts = 0
            return t

        j = domain.Job()
        j.state = enums.State.RUNNING.value
        j.user_id = user.id
        j.metadata = {}
        l1.job_id = j.id
        l2.job_id = j.id
        l3.job_id = j.id

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])
        self.db.create_job(
            j,
            [l1, l2, l3],
            [
                task(l1.id, enums.State.COMPLETED.value),
                task(l2.id, enums.State.PENDING.value),
                task(l3.id, enums.State.PENDING.value),
            ]
        )

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)

        assert jobs[0].state == enums.State.RUNNING.value
        assert {
            l1.id: enums.State.COMPLETED.value,
            l2.id: enums.State.QUEUED.value,
            l3.id: enums.State.QUEUED.value,
        } == {l.id: l.state for l in layers}

    def test_multiparent_layers_one_parent_running(self):
        # arrange 
        user = self._user()
        
        l1 = domain.Layer()
        l1.state = enums.State.RUNNING.value
        l1.user_id = user.id
        l1.metadata = {}

        l2 = domain.Layer()
        l2.state = enums.State.RUNNING.value
        l2.user_id = user.id
        l2.metadata = {}

        l3 = domain.Layer()
        l3.state = enums.State.PENDING.value
        l3.user_id = user.id
        l3.metadata = {}

        #
        #  L1  L2
        #  |   |
        #  +-+-+
        #    |
        #    L3
        #
        l1.siblings = [l2.id]
        l1.children = [l3.id]
        l2.siblings = [l1.id]
        l2.children = [l3.id]
        l3.parents = [l1.id, l2.id]

        def task(layer_id, state):
            t = domain.Task()
            t.user_id = user.id
            t.state = state
            t.layer_id = layer_id
            t.type = "sometype"
            t.metadata = {}
            t.max_attempts = 0
            return t

        j = domain.Job()
        j.state = enums.State.RUNNING.value
        j.user_id = user.id
        j.metadata = {}
        l1.job_id = j.id
        l2.job_id = j.id
        l3.job_id = j.id

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])
        self.db.create_job(
            j,
            [l1, l2, l3],
            [
                task(l1.id, enums.State.COMPLETED.value),
                task(l2.id, enums.State.RUNNING.value),
                task(l3.id, enums.State.PENDING.value),
            ]
        )

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)

        assert jobs[0].state == enums.State.RUNNING.value
        assert {
            l1.id: enums.State.COMPLETED.value,
            l2.id: enums.State.RUNNING.value,
            l3.id: enums.State.PENDING.value,
        } == {l.id: l.state for l in layers}

    @pytest.mark.parametrize(
        "child_initial_state, child_final_state, job_final_state", [
            #
            # These are scenarios in which a layer completes, but has a child layer
            #
            (
                enums.State.PENDING.value,
                enums.State.QUEUED.value,
                enums.State.RUNNING.value,
            ),
            (
                enums.State.QUEUED.value,
                enums.State.QUEUED.value,
                enums.State.RUNNING.value,
            ),
            (
                enums.State.RUNNING.value,
                enums.State.RUNNING.value,
                enums.State.RUNNING.value,
            ),
            (
                enums.State.ERRORED.value,
                enums.State.RUNNING.value,
                enums.State.RUNNING.value,
            ),
    ])
    def test_parent_child_layers(self, child_initial_state, child_final_state, job_final_state):
        # arrange
        user = self._user()

        l1 = domain.Layer()
        l1.state = enums.State.RUNNING.value
        l1.user_id = user.id
        l1.metadata = {}

        l2 = domain.Layer()
        l2.state = child_initial_state
        l2.user_id = user.id
        l2.metadata = {}

        #
        #  L1
        #  |
        #  +
        #  |
        #  L2
        #
        l1.children = [l2.id]
        l2.parents = [l1.id]

        t1 = domain.Task()
        t1.user_id = user.id
        t1.layer_id = l1.id
        t1.state = enums.State.COMPLETED.value
        t1.type = "sometype"
        t1.metadata = {}
        t1.max_attempts = 0

        t2 = domain.Task()
        t2.user_id = user.id
        t2.state = enums.State.PENDING.value
        t2.layer_id = l2.id
        t2.type = "sometype"
        t2.metadata = {}
        t2.max_attempts = 0

        j = domain.Job()
        j.state = enums.State.RUNNING.value
        j.user_id = user.id
        j.metadata = {}

        l1.job_id = j.id
        l2.job_id = j.id

        self.db.create_job(j, [l1, l2], [t1, t2])

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)

        done_layers = [l for l in layers if l.state == enums.State.COMPLETED.value]
        other_layers = [l for l in layers if l.state == child_final_state]

        assert len(jobs) == 1
        assert jobs[0].state == job_final_state
        assert len(done_layers) == 1
        assert done_layers[0].id == l1.id

        assert len(other_layers) == 1
        assert other_layers[0].id == l2.id

    @pytest.mark.parametrize(
        "sibling_initial_state, sibling_final_state, job_final_state", [
            #
            # These are scenarios in which a layer completes, but has a sibling layer in some
            # non-completed state.
            #
            (
                enums.State.PENDING.value,
                enums.State.QUEUED.value,
                enums.State.RUNNING.value,
            ),
            (
                enums.State.RUNNING.value,
                enums.State.RUNNING.value,
                enums.State.RUNNING.value,
            ),
            (
                enums.State.ERRORED.value,
                enums.State.RUNNING.value,
                enums.State.RUNNING.value,
                # because check_running_layers should launch tasks that are pending
                # OR errored from layers that are running/errored, thus yielding a
                # running layer & job
            ),
    ])
    def test_sibling_layers_complete_and_other(
        self, sibling_initial_state, sibling_final_state, job_final_state
    ):
        """We expect the running layer to become completed, the non running
        one to get queued.

        """
        # arrange
        user = self._user()

        l1 = domain.Layer()
        l1.state = enums.State.RUNNING.value
        l1.user_id = user.id
        l1.metadata = {}

        l2 = domain.Layer()
        l2.state = sibling_initial_state
        l2.user_id = user.id
        l2.metadata = {}

        #   (start)
        #   L1   L2
        #  (finish)
        #
        l1.siblings = [l2.id]
        l2.siblings = [l1.id]

        t1 = domain.Task()
        t1.user_id = user.id
        t1.layer_id = l1.id
        t1.state = enums.State.COMPLETED.value
        t1.type = "sometype"
        t1.metadata = {}
        t1.max_attempts = 0

        t2 = domain.Task()
        t2.user_id = user.id
        t2.state = enums.State.PENDING.value
        t2.layer_id = l2.id
        t2.type = "sometype"
        t2.metadata = {}
        t2.max_attempts = 0

        j = domain.Job()
        j.state = enums.State.RUNNING.value
        j.user_id = user.id
        j.metadata = {}

        l1.job_id = j.id
        l2.job_id = j.id

        self.db.create_job(j, [l1, l2], [t1, t2])

        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[j.id])])

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)
        tasks = self.db.get_tasks(query=query)

        done_layers = [l for l in layers if l.state == enums.State.COMPLETED.value]
        other_layers = [l for l in layers if l.state == sibling_final_state]

        assert len(jobs) == 1
        assert jobs[0].state == job_final_state
        assert len(done_layers) == 1
        assert done_layers[0].id == l1.id

        assert len(other_layers) == 1
        assert other_layers[0].id == l2.id

    @pytest.mark.parametrize("task_state, final_layer_state, final_job_state, max_attempts", [
        #
        # The easiest cases: Where a single layer has all it's tasks in a given state
        # Nb. by default we've created tasks where with max attempts = 0
        #
        (
            enums.State.COMPLETED.value,
            enums.State.COMPLETED.value,
            enums.State.COMPLETED.value,
            0,
        ),
        (
            enums.State.ERRORED.value,
            enums.State.ERRORED.value,
            enums.State.ERRORED.value,
            0,
        ),
    ])
    def test_one_running_layer(self, task_state, final_layer_state, final_job_state, max_attempts):
        # arrange
        job, lyr, tasks = self._job(
            layer_state=enums.State.RUNNING.value,
            task_state=task_state,
            task_max_attempts=max_attempts,
        )
        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[job.id])])

        # act
        self.scheduler.check_running_layers()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)

        assert layers[0].state == final_layer_state
        assert jobs[0].state == final_job_state

    def test_orphan_workers(self):
        # arrange
        job, lyr, tasks = self._job(layer_state=enums.State.QUEUED.value, tasks=1)
        tsk = tasks[0]
        w = self._worker()

        self.svc.start_work_task(w.id, tsk.id)
        self.svc.update_worker(
            w.id,
            None,
            last_ping=0,  # worker last pinged in ageeeees ago
            task_started=0,
        )

        # act
        self.scheduler.orphan_workers()

        tsk = self.svc.one_task(tsk.id)

        # assert
        assert tsk.state == enums.State.QUEUED.value  # task is marked queued

        with pytest.raises(exc.WorkerNotFound):  # the worker has been removed
            self.svc.one_worker(w.id)

    @pytest.mark.parametrize("kwargs", [
        {"job_paused": True},
        {"layer_paused": True},
        {"tasks_paused": True},
    ])
    def test_queue_work_ignores_paused(self, kwargs):
        # arrange
        job, _, _ = self._job(layer_state=enums.State.QUEUED.value, **kwargs)
        _ = [self._worker(), self._worker()]
        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[job.id])])

        # act
        self.scheduler.queue_work()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)
        tasks = self.db.get_tasks(query=query)

        queued_tasks = [t for t in tasks if t.state == enums.State.QUEUED.value]
        running_layers = [l for l in layers if l.state == enums.State.RUNNING.value]
        running_jobs = [j for j in jobs if j.state == enums.State.RUNNING.value]

        if "tasks_paused" not in kwargs:
            assert not running_jobs
            assert not running_layers
        assert not queued_tasks
        assert self.runner.queued_tasks() == 0

    def test_queue_work_queues_tasks(self):
        """Checks that

            Given
             - there are workers
             - there is work to do

            queue_work() should
             - set some / all of the tasks to 'queued'
             - queue the same number of tasks with the runner
             - set the only job / layer available to running

        """
        # arrange
        job, _, _ = self._job(layer_state=enums.State.QUEUED.value)
        _ = [self._worker(), self._worker()]
        query = domain.Query(limit=100, filters=[domain.Filter(job_ids=[job.id])])

        # act
        self.scheduler.queue_work()

        # assert
        jobs = self.db.get_jobs(query=query)
        layers = self.db.get_layers(query=query)
        tasks = self.db.get_tasks(query=query)

        queued_tasks = [t for t in tasks if t.state == enums.State.QUEUED.value]
        running_layers = [l for l in layers if l.state == enums.State.RUNNING.value]
        running_jobs = [j for j in jobs if j.state == enums.State.RUNNING.value]

        assert running_jobs
        assert running_layers
        assert queued_tasks
        assert self.runner.queued_tasks() == len(queued_tasks)


class TestSimpleImpl(SchedulerTest):

    CONTAINER_START_WAIT_TIME = 1
    TEST_QUEUE = "test"

    def clear_db(cls):
        from igor.database.postgres_impl import _Transaction

        cur = cls.db._conn.cursor()

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

        cls.db._conn.commit()

    @classmethod
    def setup_class(cls):
        # start a new postgres
        client = utils.Client()
        cls.db_container = client.postgres_container()

        # start a new redis
        cls.redis_container = client.redis_container()
        time.sleep(cls.CONTAINER_START_WAIT_TIME)

        cls.svc = service.IgorDBService()
        cls.db = cls.svc._db
        cls.runner = DefaultRunner(cls.svc, host="localhost")
        cls.queue = cls.runner._queue

        cls.scheduler = Simple(
            cls.svc,
            cls.runner,
            queued_tasks_min=10000  # high min just makes test writing easier
        )

