from pyigor.domain import utils


class Task:

    def __init__(self, data: dict):
        self.type = data.get("type")
        self.args = data.get("args")
        self.name = self.key = data.get("name")
        self.paused = data.get("paused_at") > 0
        self.paused_at = utils.human_time(data.get("paused_at")) if self.paused else ""
        self.retries = data.get("retries", 0)
        self.id = data.get("id")
        self.status = data.get("status")
        self.etag = data.get("etag")
        self.job_id = data.get("job_id")
        self.layer_id = data.get("layer_id")
        self.created_at = utils.human_time(data.get("created_at"))
        self.updated_at = utils.human_time(data.get("updated_at"))
        self.queue_task_id = data.get("queue_task_id")
        self.message = data.get("message")
