from pyigor.domain import utils


class Task:

    def __init__(self, data: dict):
        self.job_id = data.get("job_id")
        self.name = self.key = data.get("key")
        self.layer_id = data.get("layer_id")
        self.worker_id = data.get("worker_id")
        self.id = data.get("task_id")
        self.paused = utils.human_time(data.get("paused"))
        self.user_id = data.get("user_id")
        self.etag = data.get("etag")
        self.state = data.get("state")
        self.type = data.get("type")
        self.cmd = data.get("cmd", [])
        self.cmd_string = " ".join(self.cmd)
        self.env = data.get("env", {})
        self.result = data.get("result")
        self.runner_id = data.get("runner_id")
        self.metadata = data.get("metadata", {})
        self.attempts = data.get("attempts", 0)
        self.max_attempts = data.get("max_attempts", 0)
        self.records = data.get("records", [])
