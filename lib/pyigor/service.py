from pyigor.transport.http import IgorClient
from pyigor import enums


class Service:
    def __init__(
        self,
        url="https://localhost:9025/v1/",
        username=None,
        password=None,
        ssl_verify=False,
        ssl_pem=None,
    ):
        self._service = IgorClient(
            url=url,
            username=username,
            password=password,
            ssl_verify=ssl_verify,
            ssl_pem=ssl_pem,
        )

    def pause_job(self, id_: str, tag: str):
        """Pause a single job.

        This means *new* work belonging to the job will not be launched, running tasks are
        not affected.

        :param id_:
        :param tag:
        :returns

        """
        return self._service.pause_job(id_, tag)

    def pause_layer(self, id_: str, tag: str):
        """Pause a single layer.

        This means *new* work belonging to the layer will not be launched, running tasks are
        not affected.

        :param id_:
        :param tag:

        """
        return self._service.pause_layer(id_, tag)

    def pause_task(self, id_: str, tag: str):
        """Pause a single task.

        This means the task will not be re-launched. It does not stop the task if it's running
        already.

        :param id_:
        :param tag:

        """
        return self._service.pause_task(id_, tag)

    def set_task_result(self, id_: str, tag: str, result):
        """Set the result of a task.

        :param id_:
        :param tag:
        :param result:

        """
        return self._service.set_task_result(id_, tag, result)

    def set_task_env(self, id_: str, tag: str, env: dict):
        """Set environment variables for a task.

        Nb. This does not force a running process to re-read it's env.

        :param id_:
        :param tag:
        :param env:

        """
        return self._service.set_task_env(id_, tag, env)

    def retry_task(self, id_: str, tag: str):
        """Retry a currently task that is currently not running.

        This is intended for errored / skipped tasks.

        :param id_:
        :param tag:

        """
        return self._service.retry_task(id_, tag)

    def kill_job(self, id_: str, tag: str):
        """Kill all currently running tasks of this job.

        :param id_:
        :param tag:

        """
        return self._service.kill_job(id_, tag)

    def kill_layer(self, id_: str, tag: str):
        """Kill all currently running tasks of this layer.

        :param id_:
        :param tag:

        """
        return self._service.kill_layer(id_, tag)

    def kill_task(self, id_: str, tag: str):
        """Kill the given task.

        :param id_:
        :param tag:

        """
        return self._service.kill_task(id_, tag)

    def skip_job(self, id_: str, tag: str):
        """Skip all tasks of the given job.

        Nb. This means the tasks are considered 'successful' without having run.

        :param id_:
        :param tag:

        """
        return self._service.skip_job(id_, tag)

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

    def get_workers(self, **kwargs) -> list:
        """Get all workers matching the given arguments.

        :return: []domain.Worker

        """
        for o in self._service.get_workers(**kwargs):
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

    def one_worker(self, id_: str):
        """Fetch one worker by it's id.

        Nb. this is a lazy convenience function. It's far more efficient to fetch a batch of
        things at once & process them than go back-and-forth lots of times.

        :param id_:
        :return: domain.Layer

        """
        return self._one_or_none(self.get_workers(worker_ids=[id_]))
