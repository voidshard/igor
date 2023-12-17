from pyigor.domain import utils


class Job:

    def __init__(self, data: dict):
        self.name = self.key = data.get("name")
        self.id = data.get("id")
        self.status = data.get("status")
        self.etag = data.get("etag")
        self.created_at = utils.human_time(data.get("created_at"))
        self.updated_at = utils.human_time(data.get("updated_at"))
