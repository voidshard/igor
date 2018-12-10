from pyigor.domain import utils


class Job:

    def __init__(self, data: dict):
        self.id = data.get("job_id")
        self.paused = utils.human_time(data.get("paused"))
        self.user_id = data.get("user_id")
        self.etag = data.get("etag")
        self.key = data.get("key")
        self.state = data.get("state")
        self.runner_id = data.get("runner_id")
        self.metadata = data.get("metadata", {})
