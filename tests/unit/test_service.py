import pytest
import time
import random
import uuid

from unittest import mock

from igor import service
from igor import domain
from igor import enums
from igor import exceptions as exc


class TestIgorService:
    """
    """

    def setup(self):
        service.IgorDBService._open_db = mock.MagicMock  # shove mock into db
        self.svc = service.IgorDBService()
        self.db = self.svc._db
        self.query = domain.Query(
            filters=[domain.Filter(job_ids=[str(uuid.uuid4())])],
        )

    def test_stop_work_task_raises_worker_mismatch(self):
        # arrange
        worker = domain.Worker()
        task = domain.Task()
        task.job_id = str(uuid.uuid4())
        task.layer_id = str(uuid.uuid4())
        task.worker_id = str(uuid.uuid4())

        self.svc.one_worker = lambda *args: worker
        self.svc.one_task = lambda *args: task

        # act & assert
        with pytest.raises(exc.WorkerMismatch):
            self.svc.stop_work_task(worker.id, task.id, "COMPLETED")

    def test_start_work_task_raises_worker_mismatch(self):
        # arrange
        worker = domain.Worker()
        task = domain.Task()
        task.job_id = str(uuid.uuid4())
        task.layer_id = str(uuid.uuid4())
        task.worker_id = str(uuid.uuid4())

        self.svc.one_worker = lambda *args: worker
        self.svc.one_task = lambda *args: task

        # act & assert
        with pytest.raises(exc.WorkerMismatch):
            self.svc.start_work_task(worker.id, task.id)

    def test_start_work_task(self, monkeypatch):
        # arrange
        worker = domain.Worker()
        task = domain.Task()
        task.job_id = str(uuid.uuid4())
        task.layer_id = str(uuid.uuid4())

        class FakeTxn:
            def __init__(self, t):
                self.txn = t

            def __enter__(self):
                return self.txn

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        txn = mock.MagicMock()
        f = FakeTxn(txn)

        self.svc.one_worker = lambda *args: worker
        self.svc.one_task = lambda *args: task
        self.db.transaction = lambda *args: f

        time_now = time.time()

        def t():
            return time_now

        monkeypatch.setattr("igor.service.time.time", t)

        # act
        self.svc.start_work_task(worker.id, task.id)

        # assert
        txn.update_worker.assert_called_with(
            worker.id,
            worker.etag,
            task_started=time_now,
            job_id=task.job_id,
            layer_id=task.layer_id,
            task_id=task.id,
        )

        txn.update_task.assert_called_with(
            task.id,
            task.etag,
            state=enums.State.RUNNING.value,
            worker_id=worker.id,
        )

    def test_stop_work_task(self, monkeypatch):
        # arrange
        worker = domain.Worker()
        task = domain.Task()
        task.job_id = str(uuid.uuid4())
        task.layer_id = str(uuid.uuid4())

        class FakeTxn:
            def __init__(self, t):
                self.txn = t

            def __enter__(self):
                return self.txn

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        txn = mock.MagicMock()
        f = FakeTxn(txn)

        self.db.transaction = lambda *args: f
        self.svc.one_worker = lambda *args: worker
        self.svc.one_task = lambda *args: task

        time_now = time.time()

        def t():
            return time_now

        monkeypatch.setattr("igor.service.time.time", t)

        new_state = "COMPLETED"
        new_reason = "BLAH : {} fobar"
        new_attempts = 26

        # act
        self.svc.stop_work_task(
            worker.id,
            task.id,
            new_state,
            reason=new_reason,
            attempts=new_attempts,
        )

        # assert
        txn.update_worker.assert_called_with(
            worker.id,
            worker.etag,
            task_finished=time_now,
            job_id=None,
            layer_id=None,
            task_id=None,
        )

        txn.update_task.assert_called_with(
            task.id,
            task.etag,
            state=new_state,
            worker_id=None,
            attempts=new_attempts,
        )

    def test_worker_ping(self):
        # arrange
        wid = str(uuid.uuid4())
        stats = {
            "foo": random.randint(1, 1000),
            "bar": random.randint(1, 1000),
            "baz": random.randint(1, 1000),
            "moo": random.randint(1, 1000),
        }

        # act
        self.svc.worker_ping(wid, stats)

        # assert
        assert self.db.update_worker.called

    def test_update_job(self):
        # arrange
        test_args = str(uuid.uuid4()), str(uuid.uuid4())
        test_kwargs = {
            "runner_id": str(uuid.uuid4()),
            "metadata": {
                "foo": "bar",
                "yes": False,
                "huh": 18,
                str(uuid.uuid4()): str(uuid.uuid4()),
            },
            "state": "SOME_STATE",
        }

        # act
        self.svc.update_job(
            *test_args,
            **test_kwargs,
        )

        # assert
        self.db.update_job.assert_called_with(
            *test_args,
            **test_kwargs,
        )

    def test_update_layer(self):
        # arrange
        test_args = str(uuid.uuid4()), str(uuid.uuid4())
        test_kwargs = {
            "priority": random.randint(0, 1000),
            "runner_id": str(uuid.uuid4()),
            "metadata": {
                "foo": "bar",
                "yes": False,
                "huh": 18,
                str(uuid.uuid4()): str(uuid.uuid4()),
            },
            "state": "SOME_STATE",
        }

        # act
        self.svc.update_layer(
            *test_args,
            **test_kwargs,
        )

        # assert
        self.db.update_layer.assert_called_with(
            *test_args,
            **test_kwargs,
        )

    def test_update_task(self):
        # arrange
        test_args = str(uuid.uuid4()), str(uuid.uuid4())
        test_kwargs = {
            "worker_id": str(uuid.uuid4()),
            "runner_id": str(uuid.uuid4()),
            "attempts": None,
            "result": "foobar",
            "metadata": {
                "foo": "bar",
                "yes": False,
                "huh": 18,
                str(uuid.uuid4()): str(uuid.uuid4()),
            },
            "state": "SOME_STATE",
            "env": "SOME_RESULT",
        }

        # act
        self.svc.update_task(
            *test_args,
            **test_kwargs,
        )

        # assert
        self.db.update_task.assert_called_with(
            *test_args,
            **test_kwargs,
        )

    def test_update_worker(self):
        # arrange
        test_args = str(uuid.uuid4()), str(uuid.uuid4())
        test_kwargs = {
            "metadata": {
                "foo": "bar",
                "yes": False,
                "huh": 18,
                str(uuid.uuid4()): str(uuid.uuid4()),
            },
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
            "task_started": time.time(),
            "task_finished": time.time(),
            "last_ping": time.time(),
        }

        # act
        self.svc.update_worker(
            *test_args,
            **test_kwargs,
        )

        # assert
        self.db.update_worker.assert_called_with(
            *test_args,
            **test_kwargs,
        )

    def test_deregister_worker(self):
        # arrange
        w = domain.Worker()

        # act
        self.svc.delete_worker(w.id)

        # assert
        self.db.delete_worker.assert_called_with(w.id)

    def test_register_worker(self):
        # arrange
        w = domain.Worker()

        # act
        self.svc.create_worker(w)

        # assert
        self.db.create_worker.assert_called_with(w)

    def test_get_layers(self):
        # arrange

        # act
        self.svc.get_layers(self.query)

        # assert
        self.db.get_layers.assert_called_with(self.query)

    def test_idle_worker_count(self):
        # arrange

        # act
        self.svc.idle_worker_count()

        # assert
        self.db.idle_worker_count.assert_called_with()

    def test_get_workers(self):
        # arrange

        # act
        self.svc.get_workers(self.query)

        # assert
        self.db.get_workers.assert_called_with(self.query)

    def test_get_tasks(self):
        # arrange

        # act
        self.svc.get_tasks(self.query)

        # assert
        self.db.get_tasks.assert_called_with(self.query)
