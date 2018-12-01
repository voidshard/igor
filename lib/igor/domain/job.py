from igor.domain.base import Runnable


class Job(Runnable):

    def __init__(self, key: str=None):
        super(Job, self).__init__()
        self.key = key

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "job_id": self.id,
            "user_id": self.user_id,
            "etag": self.etag,
            "state": self.state,
            "paused": self.paused,
            "key": self.key,
            "runner_id": self.runner_id,
            "metadata": self.metadata,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Job
        :raises ValueError: if data represents an invalid job

        """
        me = cls()
        me.id = data.get("job_id")
        me.paused = data.get("paused", None)
        me.user_id = data.get("user_id")
        me.etag = data.get("etag")
        me.key = data.get("key")
        me.state = data.get("state")
        me.runner_id = data.get("runner_id")
        me.metadata = data.get("metadata", {})

        if not all([me.id, me.etag]):
            raise ValueError('require all of id, etag to inflate obj')

        return me
