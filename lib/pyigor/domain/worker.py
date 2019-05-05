from pyigor.domain import utils


class Worker:
    
    def __init__(self, data: dict):
        self.id = data.get("worker_id")
        self.last_ping = data.get("last_ping", 0)
        self.last_ping_time = utils.human_time(self.last_ping)
        self.key = self.name = data.get("key")
        self.etag = data.get("etag")
        self.host = data.get("host")
        self.job_id = data.get("job_id")
        self.layer_id = data.get("layer_id")
        self.task_id = data.get("task_id")
        self.task_started = data.get("task_started")
        self.task_finished = data.get("task_finished")
        self.metadata = data.get("metadata", {})
