
from igor.domain.base import Serializable


class Filter(Serializable):

    def __init__(
        self,
        job_ids=None,
        layer_ids=None,
        task_ids=None,
        worker_ids=None,
        states=None,
        keys=None,
    ):
        super(Filter, self).__init__()

        self.job_ids = self._remove_empty(job_ids)
        self.layer_ids = self._remove_empty(layer_ids)
        self.task_ids = self._remove_empty(task_ids)
        self.worker_ids = self._remove_empty(worker_ids)
        self.states = self._remove_empty(states)
        self.keys = self._remove_empty(keys)

    @staticmethod
    def _remove_empty(some_list) -> list:
        if not some_list:
            return []
        return [i for i in some_list if i]

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "job_ids": self.job_ids,
            "layer_ids": self.layer_ids,
            "task_ids": self.task_ids,
            "worker_ids": self.worker_ids,
            "states": self.states,
            "keys": self.keys,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Filter

        """
        me = cls()

        me.job_ids = data.get("job_ids", [])
        me.layer_ids = data.get("layer_ids", [])
        me.task_ids = data.get("task_ids", [])
        me.worker_ids = data.get("worker_ids", [])
        me.states = data.get("states", [])
        me.keys = data.get("keys", [])

        return me


class Query(Serializable):

    _D_LIMIT = 500
    _D_OFFSET = 0

    def __init__(self, filters=None, limit=_D_LIMIT, offset=_D_OFFSET, sorting=None, user_id=None):
        super(Query, self).__init__()

        self.filters = filters or []
        self.limit = limit
        self.offset = offset
        self.sorting = sorting
        self.user_id = user_id

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "filters": [f.encode() for f in self.filters],
            "limit": self.limit,
            "offset": self.offset,
            "sorting": self.sorting,
            "user_id": self.user_id,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Query

        """
        me = cls()
        me.filters = [Filter.decode(f) for f in data.get("filters", [])]
        me.limit = data.get("limit", cls._D_LIMIT)
        me.offset = data.get("offset", cls._D_OFFSET)
        me.sorting = data.get("sorting")
        me.user_id = data.get("user_id")

        return me
