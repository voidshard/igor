from pyigor.domain import utils


class Layer:

    def __init__(self, data: dict):
        self.name = self.key = data.get("name")
        self.paused = data.get("paused_at") > 0
        self.paused_at = utils.human_time(data.get("paused_at")) if self.paused else ""
        self.priority = data.get("priority", 0)
        self.id = data.get("id")
        self.status = data.get("status")
        self.etag = data.get("etag")
        self.job_id = data.get("job_id")
        self.created_at = utils.human_time(data.get("created_at"))
        self.updated_at = utils.human_time(data.get("updated_at"))
