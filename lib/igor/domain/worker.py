import time

from igor.domain.base import Stateful
from igor import utils


class Worker(Stateful):
    """Represents a worker capable of running task(s)
    """

    def __init__(self, key: str=None):
        super(Worker, self).__init__()

        # stuff about the machine
        self.host = utils.hostname()
        self.last_ping = int(time.time())

        self.key = key
        if key is None:
            self.key = utils.random_name_worker()

        # data about the current task this worker is performing
        self.job_id = None
        self.layer_id = None
        self.task_id = None
        self.task_started = None
        self.task_finished = None

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "worker_id": self.id,
            "last_ping": self.last_ping,
            "host": self.host,
            "key": self.key,
            "etag": self.etag,
            "job_id": self.job_id,
            "layer_id": self.layer_id,
            "task_id": self.task_id,
            "task_started": self.task_started,
            "task_finished": self.task_finished,
            "metadata": self.metadata,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Worker
        :raises ValueError: if data represents an invalid worker

        """
        me = cls()
        me.id = data.get("worker_id")
        me.last_ping = data.get("last_ping", 0)
        me.key = data.get("key")
        me.etag = data.get("etag")
        me.host = data.get("host")
        me.job_id = data.get("job_id")
        me.layer_id = data.get("layer_id")
        me.task_id = data.get("task_id")
        me.task_started = data.get("task_started")
        me.task_finished = data.get("task_finished")
        me.metadata = data.get("metadata", {})

        if not all([me.id, me.etag]):
            raise ValueError('require id & etag')

        return me
