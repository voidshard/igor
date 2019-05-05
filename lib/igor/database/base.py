import abc

from igor import domain


class TransactionBase(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def create_job(self, job: domain.Job, layers: list, tasks: list):
        """

        :param job:
        :param layers:
        :param tasks:

        """
        pass

    @abc.abstractmethod
    def update_job(
        self,
        job_id: str,
        etag: str,
        runner_id=None,
        metadata=None,
        state=None,
        paused=None
    ) -> str:
        """

        :param job_id:
        :param etag:
        :param runner_id:
        :param metadata:
        :param state:
        :param paused:
        :return: str

        """
        pass

    @abc.abstractmethod
    def force_delete_job(self, job_id: str):
        """Delete job, it's layers & tasks.

        :param job_id:

        """
        pass

    @abc.abstractmethod
    def get_jobs(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        pass

    @abc.abstractmethod
    def delete_jobs(self, job_ids: list):
        """

        :param job_ids:

        """
        pass

    @abc.abstractmethod
    def update_layer(
        self,
        layer_id: str,
        etag: str,
        priority=None,
        state=None,
        runner_id=None,
        metadata=None,
        paused=None,
    ) -> str:
        """

        :param layer_id:
        :param etag:
        :param priority:
        :param state:
        :param runner_id:
        :param metadata:
        :param paused:
        :return: str

        """
        pass

    @abc.abstractmethod
    def get_layers(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        pass

    @abc.abstractmethod
    def create_tasks(self, layer_id: str, tasks: list):
        """

        :param layer_id:
        :param tasks:

        """
        pass

    @abc.abstractmethod
    def update_task(
        self,
        task_id: str,
        etag: str,
        worker_id=None,
        runner_id=None,
        metadata=None,
        state=None,
        attempts=None,
        paused=None,
    ) -> str:
        """

        :param task_id:
        :param etag:
        :param worker_id:
        :param runner_id:
        :param metadata:
        :param state:
        :param attempts:
        :param paused:
        :return: str

        """
        pass

    @abc.abstractmethod
    def get_tasks(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        pass

    @abc.abstractmethod
    def get_workers(self, query: domain.Query) -> list:
        """

        :param query:
        :return: list

        """
        pass

    # scheduler functions
    @abc.abstractmethod
    def idle_worker_count(self) -> int:
        """

        :return: list

        """
        pass

    @abc.abstractmethod
    def idle_workers(self, limit: int=0, offset: int=0) -> list:
        """

        :param limit:
        :param offset:
        :return: list

        """
        pass

    # daemon functions
    @abc.abstractmethod
    def create_worker(self, worker: domain.Worker):
        """Create worker in system with given data.

        :param worker:

        """
        pass

    @abc.abstractmethod
    def delete_worker(self, id_: str):
        """Remove worker with matching id.

        :param id_:

        """
        pass

    @abc.abstractmethod
    def update_worker(
        self,
        worker_id: str,
        etag: str,
        job_id=None,
        layer_id=None,
        task_id=None,
        task_started=None,
        task_finished=None,
        last_ping=None,
        metadata=None,
    ) -> str:
        """

        :param worker_id:
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:
        :param task_started:
        :param task_finished:
        :param last_ping:
        :param metadata:
        :return: str

        """
        pass

    @abc.abstractmethod
    def create_user(self, user: domain.User):
        """Create user.

        :param user:

        """
        pass

    @abc.abstractmethod
    def delete_user(self, user_id: str):
        """Delete user.

        :param user_id:

        """
        pass

    @abc.abstractmethod
    def update_user(self, user_id: str, etag: str, name=None, password=None, metadata=None):
        """Update single user instance.

        :param user_id:
        :param etag:
        :param name:
        :param password: this should be the *hashed* password.
        :param metadata:

        """
        pass

    @abc.abstractmethod
    def get_users(self, query: domain.Query) -> list:
        """Get users workers matching query.

        :param query:
        :return: list

        """
        pass

    @abc.abstractmethod
    def create_task_record(self, tsk: domain.Task, wkr_id: str, state, reason=""):
        pass


class Base(TransactionBase):

    _DEFAULT_LIMIT = 500

    @abc.abstractmethod
    def transaction(self):
        """

        """
        pass
