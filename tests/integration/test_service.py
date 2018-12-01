import pytest
import time
import uuid

from igor import domain
from igor import service
from igor import exceptions as exc
from igor.database.mongo_impl import MongoDB

from integration import utils


class TestService:

    db = None
    svc = None
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
        cls.db = MongoDB(host="localhost")

        def hack(*args):
            return cls.db

        service.IgorDBService._open_db = hack
        cls.svc = service.IgorDBService()

    @classmethod
    def teardown_class(cls):
        try:
            cls.db_container.stop()
        except:
            pass

    def _job(self, key=None, meta=None):
        """Create test job

        :param key:
        :return: Job

        """
        l = domain.Layer()

        j = domain.Job(key=key)
        j.metadata = meta or {}
        self.db.create_job(j, [l], [])
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

    def test_one_worker_ok(self):
        # arrange
        i = self._worker()

        # act
        result = self.svc.one_worker(i.id)

        # assert
        assert isinstance(result, domain.Worker)
        assert result.id == i.id

    def test_one_worker_raises_if_not_found(self):
        # arrange

        # act & assert
        with pytest.raises(exc.WorkerNotFound):
            self.svc.one_worker(str(uuid.uuid4()))

    def test_one_task_ok(self):
        # arrange
        i = self._task()

        # act
        result = self.svc.one_task(i.id)

        # assert
        assert isinstance(result, domain.Task)
        assert result.id == i.id

    def test_one_task_raises_if_not_found(self):
        # arrange

        # act & assert
        with pytest.raises(exc.TaskNotFound):
            self.svc.one_task(str(uuid.uuid4()))

    def test_one_layer_ok(self):
        # arrange
        i = self._layer()

        # act
        result = self.svc.one_layer(i.id)

        # assert
        assert isinstance(result, domain.Layer)
        assert result.id == i.id

    def test_one_layer_raises_if_not_found(self):
        # arrange

        # act & assert
        with pytest.raises(exc.LayerNotFound):
            self.svc.one_layer(str(uuid.uuid4()))

    def test_one_job_ok(self):
        # arrange
        i = self._job()

        # act
        result = self.svc.one_job(i.id)

        # assert
        assert isinstance(result, domain.Job)
        assert result.id == i.id

    def test_one_job_raises_if_not_found(self):
        # arrange

        # act & assert
        with pytest.raises(exc.JobNotFound):
            self.svc.one_job(str(uuid.uuid4()))
