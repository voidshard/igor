import threading
import time
import pytest
import uuid

from igor import domain
from igor import enums
from igor.runner import DefaultRunner
from igor import service
from igor.api.gateway import IgorGateway
from igor.api.transport import http

from pyigor.transport.http import IgorClient
from pyigor.domain import JobSpec, LayerSpec, TaskSpec
from pyigor.transport import exceptions as exc

from integration import utils


class TestHttpTransport:

    CONTAINER_START_WAIT_TIME = 1
    PORT = 9025
    USER = "admin"
    PASS = "admin"

    def test_set_task_env(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        value = {"hello": "there"}
        _, _, tasks = self._job(tasks=1)
        t = tasks[-1]

        # act
        changed = client.set_task_env(t.id, t.etag, value)

        results = list(client.get_tasks(task_ids=[t.id]))

        # assert
        assert len(results) == 1
        assert changed.id == t.id
        assert changed.etag != t.etag  # it's been updated
        assert results[-1].env == value

    def test_set_task_result(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        value = "hello"
        _, _, tasks = self._job(tasks=1)
        t = tasks[-1]

        # act
        changed = client.set_task_result(t.id, t.etag, value)

        results = list(client.get_tasks(task_ids=[t.id]))

        # assert
        assert len(results) == 1
        assert changed.id == t.id
        assert changed.etag != t.etag  # it's been updated
        assert results[-1].result == value

    def test_retry_job(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        job, layer, tasks = self._job(tasks=10)
        changed_task_ids = []

        for t in tasks[5:]:
            w = self._worker()
            changed_task_ids.append(t.id)
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.ERRORED.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        self.svc.update_layer(
            layer.id, layer.etag, state=enums.State.ERRORED.value
        )
        tag = self.svc.update_job(
            job.id, job.etag, state=enums.State.ERRORED.value
        )

        # act
        changed = client.retry_job(job.id, tag)

        result = list(client.get_jobs(job_ids=[job.id]))
        result_tasks = list(client.get_tasks(job_ids=[job.id]))
        result_layers = list(client.get_layers(layer_ids=[layer.id]))

        # assert
        assert len(result) == 1
        assert not result[0].paused
        assert sorted(list(changed.keys())) == sorted(changed_task_ids)
        assert result[-1].state == enums.State.RUNNING.value
        assert result_layers[-1] == enums.State.PENDING.value

        for t in result_tasks:
            if t.id in changed_task_ids:
                assert t.state == enums.State.PENDING.value

        assert len(tasks) == len(result_tasks)
        assert len(changed_task_ids) < len(tasks)

    def test_retry_layer(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        job, layer, tasks = self._job(tasks=10)
        changed_task_ids = []

        for t in tasks[5:]:
            w = self._worker()
            changed_task_ids.append(t.id)
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.ERRORED.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        tag = self.svc.update_layer(
            layer.id, layer.etag, state=enums.State.ERRORED.value
        )

        # act
        changed = client.retry_layer(layer.id, tag)

        result = list(client.get_layers(layer_ids=[layer.id]))
        result_tasks = list(client.get_tasks(job_ids=[job.id]))

        # assert
        assert len(result) == 1
        assert result[-1].state == enums.State.PENDING.value
        assert sorted(list(changed.keys())) == sorted(changed_task_ids)

        for t in result_tasks:
            assert t.state == enums.State.PENDING.value

        assert len(tasks) == len(result_tasks)
        assert len(changed_task_ids) < len(tasks)

    def test_retry_task(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, _, tasks = self._job(tasks=1)
        t = tasks[-1]
        tag = self.svc.update_task(
            t.id,
            None,
            state=enums.State.ERRORED.value,
            worker_id=None,
        )

        # act
        changed = client.retry_task(t.id, tag)

        results = list(client.get_tasks(task_ids=[t.id]))

        # assert
        assert len(results) == 1
        assert not results[-1].paused
        assert results[-1].state == enums.State.PENDING.value
        assert t.id in changed

    def test_unpause_job(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        job, _, tasks = self._job(tasks=10)

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        client.pause_job(job.id, job.etag)
        job = list(client.get_jobs(job_ids=[job.id]))[0]

        # act
        client.pause_job(job.id, job.etag)

        result = list(client.get_jobs(job_ids=[job.id]))

        # assert
        assert len(result) == 1
        assert not result[0].paused

    def test_unpause_layer(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, layer, tasks = self._job(tasks=10)

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        client.pause_layer(layer.id, layer.etag)
        layer = list(client.get_layers(layer_ids=[layer.id]))[0]

        # act
        client.pause_layer(layer.id, layer.etag)

        result = list(client.get_layers(layer_ids=[layer.id]))

        # assert
        assert len(result) == 1
        assert not result[0].paused

    def test_upause_task(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, _, tasks = self._job(tasks=1)
        client.pause_task(tasks[-1].id, tasks[-1].etag)
        tasks = list(client.get_tasks(task_ids=[tasks[-1].id]))

        # act
        client.pause_task(tasks[-1].id, tasks[-1].etag)

        results = list(client.get_tasks(task_ids=[tasks[-1].id]))

        # assert
        assert len(results) == 1
        assert not results[-1].paused

    def test_pause_job(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        job, _, tasks = self._job(tasks=10)

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        # act
        client.pause_job(job.id, job.etag)

        result = list(client.get_jobs(job_ids=[job.id]))

        # assert
        assert len(result) == 1
        assert result[0].paused

    def test_pause_layer(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, layer, tasks = self._job(tasks=10)

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        # act
        client.pause_layer(layer.id, layer.etag)

        result = list(client.get_layers(layer_ids=[layer.id]))

        # assert
        assert len(result) == 1
        assert result[0].paused

    def test_pause_task(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, _, tasks = self._job(tasks=1)

        # act
        client.pause_task(tasks[-1].id, tasks[-1].etag)

        results = list(client.get_tasks(task_ids=[tasks[-1].id]))

        # assert
        assert len(results) == 1
        assert results[-1].paused

    def test_skip_job(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        job, _, tasks = self._job(tasks=10)

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        # act
        client.skip_job(job.id, job.etag)

        results = {
            t.id: t.state for t in client.get_tasks(job_ids=[job.id])
        }

        # assert
        assert len(results) == len(tasks)
        for k, v in results.items():
            assert v == enums.State.SKIPPED.value

    def test_skip_layer(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, layer, tasks = self._job(tasks=10)

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)

        # act
        client.skip_layer(layer.id, layer.etag)

        results = {
            t.id: t.state for t in client.get_tasks(layer_ids=[layer.id])
        }

        # assert
        assert len(results) == len(tasks)
        for k, v in results.items():
            assert v == enums.State.SKIPPED.value

    def test_skip_task(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, _, tasks = self._job(tasks=1)

        # act
        client.skip_task(tasks[-1].id, tasks[-1].etag)

        results = list(client.get_tasks(task_ids=[tasks[-1].id]))

        # assert
        assert len(results) == 1
        assert results[-1].state == enums.State.SKIPPED.value

    def test_delete_job(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        job, _, tasks = self._job(tasks=10)
        expect = {}

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)
            expect[t.id] = enums.State.ERRORED.value

        # act
        client.kill_job(job.id, job.etag)

        results = {
            t.id: t.state for t in client.get_tasks(job_ids=[job.id])
        }

        # assert
        assert len(results) == len(tasks)
        for k, v in results.items():
            assert v == results.get(k, enums.State.PENDING.value)

    def test_delete_layer(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, layer, tasks = self._job(tasks=10)
        expect = {}

        for t in tasks[5:]:
            w = self._worker()
            self.svc.update_task(
                t.id,
                None,
                state=enums.State.RUNNING.value,
                worker_id=w.id,
            )
            self.svc.update_worker(w.id, None, task_id=t.id)
            expect[t.id] = enums.State.ERRORED.value

        # act
        client.kill_layer(layer.id, layer.etag)

        results = {
            t.id: t.state for t in client.get_tasks(layer_ids=[layer.id])
        }

        # assert
        assert len(results) == len(tasks)
        for k, v in results.items():
            assert v == results.get(k, enums.State.PENDING.value)

    def test_delete_task_when_not_running(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        _, _, tasks = self._job(tasks=1)

        # act
        client.kill_task(tasks[-1].id, tasks[-1].etag)

        results = list(client.get_tasks(task_ids=[tasks[-1].id]))

        # assert
        assert len(results) == 1
        assert results[-1].state == enums.State.PENDING.value

    def test_delete_task_when_running(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        w = self._worker()
        _, _, tasks = self._job(tasks=1)

        self.svc.update_task(
            tasks[-1].id,
            None,
            state=enums.State.RUNNING.value,
            worker_id=w.id,
        )
        self.svc.update_worker(w.id, None, task_id=tasks[-1].id)

        # act
        client.kill_task(tasks[-1].id, tasks[-1].etag)

        results = list(client.get_tasks(task_ids=[tasks[-1].id]))

        # assert
        assert len(results) == 1
        assert results[-1].state == enums.State.ERRORED.value

    def test_get_workers(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )
        workers = [
            self._worker(),
            self._worker(),
            self._worker(),
            self._worker(),
            self._worker(),
        ]
        self._worker()
        self._worker()

        # act
        result = list(client.get_workers(worker_ids=[w.id for w in workers]))

        # assert
        assert len(result) == len(workers)
        assert sorted([r.id for r in result]) == sorted([w.id for w in workers])

    def test_create_task(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )
        _, layer, _ = self._job()

        # act
        result = client.create_tasks(
            layer.id,
            [
                TaskSpec(["ls"], name="dynamic_task01"),
                TaskSpec(["cat", "/etc/fstab"], name="dynamic_task02"),
            ]
        )

        # assert
        tasks = list(client.get_tasks(
            layer_ids=[layer.id],
            names=["dynamic_task01", "dynamic_task02"],
        ))

        assert len(result) == 2
        assert len(tasks) == 2

    def test_create_job(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        # act
        result = client.create_job(JobSpec(
            [
                LayerSpec(
                    tasks=[
                        TaskSpec(
                            ["sleep", "10"],
                            name='sleep_task1',
                            max_attempts=7,
                        ),
                        TaskSpec(
                            ["sleep", "2"],
                            name='sleep_task2',
                            paused=True,
                        ),
                    ],
                    order=0,
                    name='layer1',
                    paused=True,
                ),
                LayerSpec(
                    order=10,
                    name='layer2'
                ),
                LayerSpec(
                    order=10,
                    name='layer3'
                ),
            ],
            name="myjob",
            paused=True,
        ))

        # assert
        assert result.id
        assert result.etag

        jobs = list(client.get_jobs(job_ids=[result.id]))
        layers = list(client.get_layers(job_ids=[result.id]))
        tasks = list(client.get_tasks(job_ids=[result.id]))

        assert len(jobs) == 1
        assert len(tasks) == 2
        assert len(layers) == 3

        l1 = [l for l in layers if l.name == "layer1"][0]
        l2 = [l for l in layers if l.name == "layer2"][0]
        l3 = [l for l in layers if l.name == "layer3"][0]

        t1 = [t for t in tasks if t.name == "sleep_task1"][0]
        t2 = [t for t in tasks if t.name == "sleep_task2"][0]

        assert jobs[0].paused
        assert jobs[0].name == 'myjob'
        assert l1.children == [l2.id, l3.id]
        assert l2.parents == [l1.id]
        assert l3.parents == [l1.id]
        assert l2.siblings == [l3.id]
        assert l3.siblings == [l2.id]
        assert l1.paused
        assert t1.max_attempts == 7
        assert t1.cmd == ["sleep", "10"]
        assert t2.cmd == ["sleep", "2"]
        assert t2.paused

    def test_authenticate_with_invalid_data(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        # act & assert
        with pytest.raises(exc.MalformedRequestError):
            list(client._get(client._jobs, "haduwhd"))

    def test_authenticate_with_invalid_user(self):
        # arrange
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username="thisuserdoesntexist",
            password='lolwut',
        )

        # act & assert
        with pytest.raises(exc.AuthError):
            list(client.get_jobs())

    def test_get_others_job_as_non_admin(self):
        # arrange
        self._user(name="foo1", password="bar2", is_admin=False)
        job, layer, tasks = self._job()

        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username="foo1",
            password='bar2',
        )

        # act
        jobs = list(client.get_jobs(job_ids=[job.id]))
        layers = list(client.get_layers(job_ids=[job.id], layer_ids=[layer.id]))
        remote_tasks = list(client.get_tasks(job_ids=[job.id], layer_ids=[layer.id]))

        # assert
        assert not jobs
        assert not layers
        assert not remote_tasks

    def test_get_own_job_as_non_admin(self):
        # arrange
        user = self._user(name="foo", password="bar", is_admin=False)
        job, layer, tasks = self._job(user_id=user.id)

        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username="foo",
            password='bar',
        )

        # act
        jobs = list(client.get_jobs(job_ids=[job.id]))
        layers = list(client.get_layers(job_ids=[job.id], layer_ids=[layer.id]))
        remote_tasks = list(client.get_tasks(job_ids=[job.id], layer_ids=[layer.id]))

        # assert
        assert len(jobs) == 1
        assert jobs[0].id == job.id
        assert len(layers) == 1
        assert layers[0].id == layer.id
        assert len(remote_tasks) == len(tasks)
        assert sorted([t.id for t in remote_tasks]) == sorted([t.id for t in tasks])

    def test_get_as_admin(self):
        # arrange
        # nb the admin doesn't own this job but should be able to fetch
        job, layer, tasks = self._job()
        client = IgorClient(
            url=f"https://localhost:{self.PORT}/v1/",
            username=self.USER,
            password=self.PASS,
        )

        # act
        jobs = list(client.get_jobs(job_ids=[job.id]))
        layers = list(client.get_layers(job_ids=[job.id], layer_ids=[layer.id]))
        remote_tasks = list(client.get_tasks(job_ids=[job.id], layer_ids=[layer.id]))

        # assert
        assert len(jobs) == 1
        assert jobs[0].id == job.id
        assert len(layers) == 1
        assert layers[0].id == layer.id
        assert len(remote_tasks) == len(tasks)
        assert sorted([t.id for t in remote_tasks]) == sorted([t.id for t in tasks])

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
    def teardown_class(cls):
        cls.server.stop()

        try:
            cls.db_container.stop()
        except:
            pass

        try:
            cls.redis_container.stop()
        except:
            pass

    @classmethod
    def setup_class(cls):
        client = utils.Client()
        cls.db_container = client.postgres_container()

        # start a new redis
        cls.redis_container = client.redis_container()
        time.sleep(cls.CONTAINER_START_WAIT_TIME)

        cls.svc = service.IgorDBService()
        cls.db = cls.svc._db
        cls.runner = DefaultRunner(cls.svc, host="localhost")

        cls.gate = IgorGateway(cls.svc, cls.runner)
        cls.server = http.HttpTransport(port=cls.PORT)

        def run():
            cls.server.serve(cls.gate)

        cls.thread = threading.Thread(target=run)
        cls.thread.setDaemon(True)
        cls.thread.start()
        time.sleep(cls.CONTAINER_START_WAIT_TIME)

        cls.admin = cls._user(name=cls.USER, password=cls.PASS, is_admin=True)

        # create some random jobs so that we can be sure our filters work
        cls._job()
        cls._job()
        cls._job()
        time.sleep(cls.CONTAINER_START_WAIT_TIME)

    @classmethod
    def _user(cls, name=None, password="test", is_admin=False):
        """Create test user

        :return: User

        """
        u = domain.User()
        u.name = name or uuid.uuid4().hex
        u.password = password
        u.is_admin = is_admin
        cls.db.create_user(u)
        return u

    @classmethod
    def _worker(cls, key=None):
        # arrange
        w = domain.Worker(key=key)
        cls.db.create_worker(w)
        return w

    @classmethod
    def _job(
        cls,
        user_id=False,
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
        if not user_id:
            user_id = cls._user().id

        l = domain.Layer()
        l.user_id = user_id
        l.state = layer_state
        l.paused = time.time() if layer_paused else None
        l.metadata = {}

        task_list = []
        for i in range(0, tasks):
            t = domain.Task()
            t.user_id = user_id
            t.paused = time.time() if tasks_paused else None
            t.layer_id = l.id
            t.type = "sometype"
            t.state = task_state
            t.metadata = {}
            t.max_attempts = task_max_attempts
            task_list.append(t)

        j = domain.Job()
        j.user_id = user_id
        j.metadata = {}
        j.paused = time.time() if job_paused else None

        l.job_id = j.id

        cls.db.create_job(j, [l], task_list)
        return j, l, task_list
