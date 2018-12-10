from igorUI import config

from pyigor.http import IgorClient
from pyigor import enums


class _Service:
    def __init__(self):
        _cfg = config.read_default_config()
        self._service = IgorClient(**_cfg.get("client", {}))

    def pause_job(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.pause_job(id_, tag)

    def pause_layer(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.pause_layer(id_, tag)

    def pause_task(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.pause_task(id_, tag)

    def set_task_result(self, id_: str, tag: str, result):
        """

        :param id_:
        :param tag:
        :param result:
        :return:

        """
        return self._service.set_task_result(id_, tag, result)

    def set_task_env(self, id_: str, tag: str, env: dict):
        """

        :param id_:
        :param tag:
        :param env:
        :return:

        """
        return self._service.set_task_env(id_, tag, env)

    def retry_task(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.retry_task(id_, tag)

    def kill_job(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.kill_job(id_, tag)

    def kill_layer(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.kill_layer(id_, tag)

    def kill_task(self, id_: str, tag: str):
        """

        :param id_:
        :param tag:

        """
        return self._service.kill_task(id_, tag)

    def get_jobs(self, **kwargs) -> list:
        """

        :return: []domain.Job

        """
        for o in self._service.get_jobs(**kwargs):
            yield o

    def get_layers(self, **kwargs) -> list:
        """

        :return: []domain.Layer

        """
        for o in self._service.get_layers(**kwargs):
            yield o

    def get_tasks(self, **kwargs) -> list:
        """

        :return: []domain.Task

        """
        for o in self._service.get_tasks(**kwargs):
            yield o

    def get_workers(self, **kwargs) -> list:
        """

        :return: []domain.Worker

        """
        for o in self._service.get_workers(**kwargs):
            yield o

    @staticmethod
    def _one_or_none(result):
        result = list(result)
        if not result:
            return None
        return result[0]

    def one_job(self, id_: str):
        return self._one_or_none(self.get_jobs(job_ids=[id_], states=enums.ALL))

    def one_layer(self, id_: str):
        return self._one_or_none(self.get_layers(layer_ids=[id_], states=enums.ALL))

    def one_task(self, id_: str):
        return self._one_or_none(self.get_tasks(task_ids=[id_], states=enums.ALL))

    def one_worker(self, id_: str):
        return self._one_or_none(self.get_workers(worker_ids=[id_]))


enums = enums
Service = _Service()
