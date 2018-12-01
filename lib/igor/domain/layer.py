from igor.domain.base import Runnable


class Layer(Runnable):

    def __init__(self, key: str=None):
        super(Layer, self).__init__()
        self.key = key
        self.job_id = None
        self.order = 0
        self.priority = 0
        self.parents = []
        self.siblings = []
        self.children = []

    def encode(self) -> dict:
        """

        :return: dict

        """
        return {
            "job_id": self.job_id,
            "user_id": self.user_id,
            "layer_id": self.id,
            "key": self.key,
            "state": self.state,
            "etag": self.etag,
            "runner_id": self.runner_id,
            "metadata": self.metadata,
            "paused": self.paused,
            "priority": self.priority,
            "order": self.order,
            "parents": self.parents,
            "siblings": self.siblings,
            "children": self.children,
        }

    @classmethod
    def decode(cls, data: dict):
        """

        :param data:
        :return: Layer
        :raises ValueError: if data represents an invalid layer

        """
        me = cls()
        me.job_id = data.get("job_id")
        me.id = data.get("layer_id")
        me.paused = data.get("paused", None)
        me.user_id = data.get("user_id")
        me.key = data.get("key")
        me.state = data.get("state")
        me.priority = data.get("priority", 0)
        me.etag = data.get("etag")
        me.order = data.get("order", 0)
        me.runner_id = data.get("runner_id")
        me.metadata = data.get("metadata", {})
        me.parents = data.get("parents", [])
        me.siblings = data.get("siblings", [])
        me.children = data.get("children", [])

        if not all([me.id, me.job_id, me.etag]):
            raise ValueError('require all of job_id, id, etag to inflate obj')

        return me
