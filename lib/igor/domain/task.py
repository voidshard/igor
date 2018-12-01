import time

from igor.domain.base import Runnable
from igor import enums


class Task(Runnable):

    _DEFAULT_RETRY_ATTEMPTS = 2

    def __init__(self, key: str=None):
        super(Task, self).__init__()

        self.job_id = None
        self.key = key
        self.layer_id = None
        self.worker_id = None
        self.worker_id = None
        self.type = None
        self.cmd = []
        self.env = {}
        self.attempts = 0
        self.max_attempts = self._DEFAULT_RETRY_ATTEMPTS

    def work_record_update(self, worker_id, worker_host, task_state, reason="") -> dict:
        """Create a new work record with standard metadata.

        :param worker_id:
        :param worker_host:
        :param task_state:
        :param reason:
        :return: dict

        """
        records = self.metadata.get("records", [])
        if not isinstance(records, list):
            records = []

        records.append({
            "worker": worker_id,
            "host": worker_host,
            "time": time.time(),
            "state": task_state,
            "reason": reason,
        })

        self.metadata['records'] = records
        return self.metadata

    @classmethod
    def new(cls, type_, cmd: list, env: dict, metadata: dict=None, key=None):
        """

        :param type_:
        :param cmd:
        :param env:
        :param metadata:
        :param key: user supplied key
        :return: Task

        """
        me = cls()
        me.state = enums.State.PENDING.value
        me.metadata = metadata or {}
        me.type = type_
        me.cmd = cmd
        me.env = env
        me.key = key
        return me

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "job_id": self.job_id,
            "state": self.state,
            "user_id": self.user_id,
            "layer_id": self.layer_id,
            "task_id": self.id,
            "key": self.key,
            "paused": self.paused,
            "etag": self.etag,
            "type": self.type,
            "cmd": self.cmd,
            "env": self.env,
            "worker_id": self.worker_id,
            "runner_id": self.runner_id,
            "metadata": self.metadata,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Task
        :raises ValueError: if data represents an invalid task

        """
        me = cls()
        me.job_id = data.get("job_id")
        me.layer_id = data.get("layer_id")
        me.worker_id = data.get("worker_id")
        me.id = data.get("task_id")
        me.paused = data.get("paused", None)
        me.user_id = data.get("user_id")
        me.key = data.get("key")
        me.etag = data.get("etag")
        me.state = data.get("state")
        me.type = data.get("type")
        me.cmd = data.get("cmd", [])
        me.env = data.get("env", {})
        me.runner_id = data.get("runner_id")
        me.metadata = data.get("metadata", {})
        me.attempts = data.get("attempts", 0)
        me.max_attempts = data.get("max_attempts", cls._DEFAULT_RETRY_ATTEMPTS)

        if not all([me.job_id, me.layer_id, me.id, me.etag]):
            raise ValueError(
                'require all of job_id, layer_id, id, etag to inflate obj'
            )

        return me
