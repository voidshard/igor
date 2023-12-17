from pyigor.transport.http import IgorClient
from pyigor import enums


class Service:
    def __init__(
        self,
        url="https://localhost:8100/api/v1/",
        username=None,
        password=None,
        ssl_verify=False,
        ssl_pem=None,
    ):
        self._service = IgorClient(
            url=url,
            ssl_verify=ssl_verify,
            ssl_pem=ssl_pem,
        )

    def unpause_layer(self, id_: str, tag: str):
        """Unpause a single layer.

        This means *new* work belonging to the layer will be launched, running tasks are
        not affected.

        :param id_:
        :param tag:

        """
        return self._service.unpause_layer(id_, tag)

    def pause_layer(self, id_: str, tag: str):
        """Pause a single layer.

        This means *new* work belonging to the layer will not be launched, running tasks are
        not affected.

        :param id_:
        :param tag:

        """
        return self._service.pause_layer(id_, tag)

    def unpause_task(self, id_: str, tag: str):
        """Unpause a single task.

        This means the task will be re-launched. It does not start the task if it's not running
        already.

        :param id_:
        :param tag:

        """
        return self._service.unpause_task(id_, tag)

    def pause_task(self, id_: str, tag: str):
        """Pause a single task.

        This means the task will not be re-launched. It does not stop the task if it's running
        already.

        :param id_:
        :param tag:

        """
        return self._service.pause_task(id_, tag)

    def retry_task(self, id_: str, tag: str):
        """Retry a currently task that is currently not running.

        This is intended for errored / skipped tasks.

        :param id_:
        :param tag:

        """
        return self._service.retry_task(id_, tag)

    def kill_task(self, id_: str, tag: str):
        """Kill the given task.

        :param id_:
        :param tag:

        """
        return self._service.kill_task(id_, tag)

    def skip_layer(self, id_: str, tag: str):
        """Skip all tasks of the current layer.

        Nb. This means the tasks are considered 'successful' without having run.

        :param id_:
        :param tag:

        """
        return self._service.skip_layer(id_, tag)

    def skip_task(self, id_: str, tag: str):
        """Skill the given task.

        Nb. This means the task is considered 'successful' without having run.

        :param id_:
        :param tag:

        """
        return self._service.skip_task(id_, tag)

    def get_jobs(self, **kwargs) -> list:
        """Get all jobs matching the given arguments.

        :return: []domain.Job

        """
        for o in self._service.get_jobs(**kwargs):
            yield o

    def get_layers(self, **kwargs) -> list:
        """Get all layers matching the given arguments.

        :return: []domain.Layer

        """
        for o in self._service.get_layers(**kwargs):
            yield o

    def get_tasks(self, **kwargs) -> list:
        """Get all tasks matching the given arguments.

        :return: []domain.Task

        """
        for o in self._service.get_tasks(**kwargs):
            yield o

    @staticmethod
    def _one_or_none(result: list):
        """Lazy util to return one thing from a list of things or nothing.

        :param result:
        :return: ?

        """
        result = list(result)
        if not result:
            return None
        return result[0]

    def one_job(self, id_: str):
        """Fetch one job by it's id.

        Nb. this is a lazy convenience function. It's far more efficient to fetch a batch of
        things at once & process them than go back-and-forth lots of times.

        :param id_:
        :return: domain.Job

        """
        return self._one_or_none(self.get_jobs(job_ids=[id_], states=enums.ALL))

    def one_layer(self, id_: str):
        """Fetch one layer by it's id.

        Nb. this is a lazy convenience function. It's far more efficient to fetch a batch of
        things at once & process them than go back-and-forth lots of times.

        :param id_:
        :return: domain.Layer

        """
        return self._one_or_none(self.get_layers(layer_ids=[id_], states=enums.ALL))

    def one_task(self, id_: str):
        """Fetch one task by it's id.

        Nb. this is a lazy convenience function. It's far more efficient to fetch a batch of
        things at once & process them than go back-and-forth lots of times.

        :param id_:
        :return: domain.Layer

        """
        return self._one_or_none(self.get_tasks(task_ids=[id_], states=enums.ALL))
