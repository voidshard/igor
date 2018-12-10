import time

from igor import database
from igor import domain
from igor import enums
from igor import utils
from igor import exceptions as exc


logger = utils.logger()


class IgorDBService:
    """Used within Igor to add some db sugar.

    """

    def __init__(self, config=None):
        self._config = config or {}
        self._db = self._open_db()

    def _open_db(self):
        """Open connection to DB.

        """
        return database.PostgresDB(
            host=self._config.get("database", {}).get("host", "localhost"),
            port=int(self._config.get("database", {}).get("port", 5432)),
        )

    def get_tasks(self, q: domain.Query):
        """Return list of tasks matching Query.

        :param q:
        :return: []domain.Task

        """
        return self._db.get_tasks(q)

    def idle_worker_count(self) -> int:
        """Return number of workers not currently working on a task.

        :return: int

        """
        return self._db.idle_worker_count()

    def idle_workers(self, limit, offset: int=0) -> list:
        """Return workers not currently working on a task.

        :param limit:
        :param offset:
        :return: []domain.Worker

        """
        return self._db.idle_workers(limit=limit, offset=offset)

    def get_jobs(self, query: domain.Query) -> list:
        """Get all jobs matching the given query.

        :param query:
        :return: []domain.Job

        """
        return self._db.get_jobs(query)

    def get_layers(self, query: domain.Query) -> list:
        """Get all layers matching the given query.

        :param query:
        :return: []domain.Layer

        """
        return self._db.get_layers(query)

    def worker_ping(self, worker_id: str, stats: dict):
        """Update system to update worker ping time & provide some stats.

        :param worker_id:
        :param stats:

        """
        wkr = self.one_worker(worker_id)

        meta = wkr.metadata
        meta['stats'] = stats

        self.update_worker(worker_id, None, last_ping=time.time(), metadata=meta)

    def start_work_task(self, wkr_id: str, tsk_id: str):
        """Worker tells system it's starting a task

        :param wkr_id:
        :param tsk_id:

        :raises WorkerMismatch: the task cannot be updated by this worker
        :raises TaskNotFound:
        :raises WorkerNotFound:

        """
        wkr = self.one_worker(wkr_id)
        tsk = self.one_task(tsk_id)

        if tsk.worker_id and tsk.worker_id != wkr.id:
            result = self._db.get_workers(
                domain.Query([domain.Filter(worker_ids=[tsk.worker_id])], limit=1)
            )
            if result:
                # the task is already being worked on
                raise exc.WorkerMismatch(f"mismatched worker {wkr.id} to task {tsk.id}")

        tsk_update = {
            "state": enums.State.RUNNING.value,
            "worker_id": wkr.id,
        }
        wkr_update = {
            'task_started': time.time(),
            'job_id': tsk.job_id,
            'layer_id': tsk.layer_id,
            'task_id': tsk.id,
        }

        # update task
        self.update_task(
            tsk.id,
            tsk.etag,
            **tsk_update
        )

        # update worker
        self.update_worker(
            wkr.id,
            wkr.etag,
            **wkr_update
        )

    def stop_work_task(
        self, wkr_id: str, tsk_id: str, w_state: str, reason: str="", attempts=None
    ):
        """Worker tells system it's stopping a task

        :param wkr_id:
        :param tsk_id:
        :param w_state: state to set task to
        :param reason: reason for update
        :param attempts: attempts to set on task

        :raises IllegalOp: if the task cannot be set to this state
        :raises WorkerMismatch: the task cannot be updated by this worker
        :raises TaskNotFound:
        :raises WorkerNotFound:

        """
        wkr = self.one_worker(wkr_id)
        tsk = self.one_task(tsk_id)

        if tsk.worker_id and tsk.worker_id != wkr.id:
            raise exc.WorkerMismatch(f"mismatched worker {wkr.id} to task {tsk.id}")

        tsk_update = {
            "state": w_state,
            "metadata": tsk.work_record_update(
                wkr.id, wkr.host, w_state, reason=reason
            ),
            "worker_id": None,
            "attempts": attempts,
        }

        wkr_update = {
            'task_finished': time.time(),
            'job_id': None,
            'layer_id': None,
            'task_id': None,
        }

        # update task
        self.update_task(
            tsk.id,
            tsk.etag,
            **tsk_update
        )

        # update worker
        self.update_worker(
            wkr.id,
            wkr.etag,
            **wkr_update
        )

    def update_job(
        self,
        job_id: str,
        etag: str,
        metadata=None,
        state=None,
        **kwargs
    ):
        """Update given job with various settings.

        :param job_id:
        :param etag:
        :param metadata:
        :param state:
        :param retries:
        :param runner_id:
        :param paused:

        """
        update_kwargs = {
            "metadata": metadata,
            "state": state,
        }

        for arg in ["runner_id", "paused"]:
            if arg in kwargs:
                update_kwargs[arg] = kwargs.get(arg)

        return self._db.update_job(
            job_id,
            etag,
            **update_kwargs,
        )

    def update_layer(
        self,
        layer_id: str,
        etag: str,
        priority=None,
        state=None,
        metadata=None,
        **kwargs
    ):
        """Update given layer with various settings.

        :param layer_id:
        :param etag:
        :param priority:
        :param state:
        :param runner_id:
        :param metadata:
        :param paused:

        """
        update_kwargs = {
            "priority": priority,
            "state": state,
            "metadata": metadata,
        }

        for arg in ["runner_id", "paused"]:
            if arg in kwargs:
                update_kwargs[arg] = kwargs.get(arg)

        return self._db.update_layer(
            layer_id,
            etag,
            **update_kwargs,
        )

    def update_task(
        self,
        task_id: str,
        etag: str,
        metadata=None,
        result=None,
        state=None,
        attempts=None,
        **kwargs
    ):
        """Update given task with various settings.

        :param task_id:
        :param etag:
        :param worker_id:
        :param runner_id:
        :param metadata:
        :param result:
        :param state:
        :param attempts:
        :param paused:

        """
        update_kwargs = {
            "state": state,
            "metadata": metadata,
            "result": result,
            "attempts": attempts,
        }

        for arg in ["runner_id", "paused", "worker_id"]:
            if arg in kwargs:
                update_kwargs[arg] = kwargs.get(arg)

        return self._db.update_task(
            task_id,
            etag,
            **update_kwargs,
        )

    def update_worker(
        self,
        worker_id: str,
        etag: str,
        task_started=None,
        task_finished=None,
        last_ping=None,
        metadata=None,
        **kwargs
    ):
        """Update given worker with various settings

        :param worker_id:
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:
        :param task_started:
        :param task_finished:
        :param last_ping:
        :param metadata:

        """
        update_kwargs = {
            "task_started": task_started,
            "task_finished": task_finished,
            "last_ping": last_ping,
            "metadata": metadata,
        }

        for arg in ["job_id", "layer_id", "task_id"]:
            if arg in kwargs:
                update_kwargs[arg] = kwargs.get(arg)

        return self._db.update_worker(
            worker_id,
            etag,
            **update_kwargs,
        )

    def get_workers(self, query: domain.Query):
        """Return workers matching the given query.

        :param query:
        :return: []Worker

        """
        return self._db.get_workers(query)

    def delete_worker(self, worker_id):
        """Delete the given worker.

        :param worker_id:

        """
        return self._db.delete_worker(worker_id)

    def create_worker(self, worker: domain.Worker):
        """Worker is created / registered on the system.

        :param worker:

        """
        self._db.create_worker(worker)

    def create_user(self, user: domain.User):
        """User is created on the system.

        :param user:

        """
        self._db.create_user(user)

    def delete_user(self, user_id: str):
        """User is removed from the system.

        :param user_id:

        """
        self._db.delete_user(user_id)

    def create_job(self, job: domain.Job, layers: list, tasks: list):
        """Job is created on the system.

        :param job:
        :param layers: at least one layer. All layers must belong to this job.
        :param tasks: a list of tasks, each task MUST belong to a layer in 'layers'

        """
        self._db.create_job(job, layers, tasks)

    def create_tasks(self, layer_id: str, tasks: list):
        """Tasks are created on the system.

        :param layer_id:
        :param tasks:

        """
        self._db.create_tasks(layer_id, tasks)

    def one_user(self, id_: str):
        """Retrieve one user or raise.

        :param id_:
        :return: User
        :raises: UserNotFound

        """
        result = self._db.get_users(
            domain.Query(user_id=id_, limit=1)
        )
        if not result:
            raise exc.UserNotFound(id_)
        return result[0]

    def one_user_by_name(self, name: str):
        """Retrieve one user or raise.

        :param name:
        :return: User
        :raises: UserNotFound

        """
        result = self._db.get_users(
            domain.Query(limit=1, filters=[domain.Filter(keys=[name])])
        )
        if not result:
            raise exc.UserNotFound(name)
        return result[0]

    def one_worker(self, id_: str):
        """Retrieve one worker or raise.

        :param id_:
        :return: Worker
        :raises: WorkerNotFound

        """
        result = self._db.get_workers(
            domain.Query([domain.Filter(worker_ids=[id_])], limit=1)
        )
        if not result:
            raise exc.WorkerNotFound(id_)
        return result[0]

    def one_task(self, id_: str):
        """Retrieve one task or raise.

        :param id_:
        :return: Task
        :raises: TaskNotFound

        """
        result = self._db.get_tasks(
            domain.Query([domain.Filter(task_ids=[id_])], limit=1)
        )
        if not result:
            raise exc.TaskNotFound(id_)
        return result[0]

    def one_layer(self, id_: str):
        """Retrieve one layer or raise.

        :param id_:
        :return: Layer
        :raises: LayerNotFound

        """
        result = self._db.get_layers(
            domain.Query([domain.Filter(layer_ids=[id_])], limit=1)
        )
        if not result:
            raise exc.LayerNotFound(id_)
        return result[0]

    def one_job(self, id_: str):
        """Retrieve one job or raise.

        :param id_:
        :return: Job
        :raises: JobNotFound

        """
        result = self._db.get_jobs(
            domain.Query([domain.Filter(job_ids=[id_])], limit=1)
        )
        if not result:
            raise exc.JobNotFound(id_)
        return result[0]

    def force_delete_job(self, id_: str):
        """

        :param id_:
        :return:

        """
        self._db.force_delete_job(id_)
