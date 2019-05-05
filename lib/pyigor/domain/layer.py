from pyigor.domain import utils


class Layer:

    def __init__(self, data: dict):
        self.job_id = data.get("job_id")
        self.id = data.get("layer_id")
        self.name = self.key = data.get("key")
        self.paused = utils.human_time(data.get("paused"))
        self.user_id = data.get("user_id")
        self.state = data.get("state")
        self.priority = data.get("priority", 0)
        self.etag = data.get("etag")
        self.order = data.get("order", 0)
        self.runner_id = data.get("runner_id")
        self.metadata = data.get("metadata", {})
        self.parents = data.get("parents", [])
        self.siblings = data.get("siblings", [])
        self.children = data.get("children", [])
