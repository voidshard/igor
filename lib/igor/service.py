import time

from igor import database
from igor import domain
from igor import enums
from igor import utils
from igor import exceptions as exc


logger = utils.logger()


def _retry(
    update_fn, get_one_fn, id_, tag, kwargs=None, retries=0, catch=(exc.WriteFailError,)
):
    """Retries a DB function, catching errors raised if a Write Fails.

    This write deliberately clobbers, essentially ignoring the etag. Hence this is intended
    only for internal callers that know what they're doing .. external clients should actually
    respect & check why we failed to write - refetching the latest obj and resetting the tag.
    Internally however, what we say goes!

    :param update_fn: func that takes args (id, etag) and some kwargs to update
    :param get_one_fn: func that takes an ID and returns the corresponding obj (or None)
    :param id_: id of obj we want to update
    :param tag: current etag
    :param kwargs: kwargs corresponding to fields on the obj we wish to update
    :param retries: number of times to retry the operation
    :param catch: error(s) to catch (by default, we only catch WriteFail)

    """
    kwargs = kwargs or {}

    err = None
    for i in range(0, max(1, retries)):
        try:
            return update_fn(id_, tag, **kwargs)
        except catch as e:
            logger.warn(
                f"retrying db write: error:{e} id:{id_} etag:{tag}"
            )
            err = e

            obj = get_one_fn(id_)
            if not obj:
                raise e

            tag = obj.etag
            continue
    raise err


class IgorDBService:
    """Used within Igor to add some db sugar.

    """

    def __init__(self, config=None):
        self._config = config or {}
        self._db = self._open_db()

    def _open_db(self):
        """Open connection to DB.

        """
        driver = self._config.get("database", {}).get("driver", "postgres")
        if driver == "mongo":
            return database.MongoDB(
                host=self._config.get("database", {}).get("host", "localhost"),
                port=int(self._config.get("database", {}).get("port", 27017)),
            )
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

        self.update_worker(worker_id, wkr.etag, last_ping=time.time(), metadata=meta, retries=3)

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
            retries=3,
            **tsk_update
        )

        # update worker
        self.update_worker(
            wkr.id,
            wkr.etag,
            retries=3,
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
            retries=3,
            **tsk_update
        )

        # update worker
        self.update_worker(
            wkr.id,
            wkr.etag,
            retries=3,
            **wkr_update
        )

    def update_job(
        self,
        job_id: str,
        etag: str,
        metadata=None,
        state=None,
        retries=0,
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

        return _retry(
            self._db.update_job,
            self.one_job,
            job_id,
            etag,
            kwargs=update_kwargs,
            retries=retries,
        )

    def update_layer(
        self,
        layer_id: str,
        etag: str,
        priority=None,
        state=None,
        metadata=None,
        retries=0,
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
        :param retries:

        """
        update_kwargs = {
            "priority": priority,
            "state": state,
            "metadata": metadata,
        }

        for arg in ["runner_id", "paused"]:
            if arg in kwargs:
                update_kwargs[arg] = kwargs.get(arg)

        return _retry(
            self._db.update_layer,
            self.one_layer,
            layer_id,
            etag,
            kwargs=update_kwargs,
            retries=retries,
        )

    def update_task(
        self,
        task_id: str,
        etag: str,
        metadata=None,
        state=None,
        attempts=None,
        retries=0,
        **kwargs
    ):
        """Update given task with various settings.

        :param task_id:
        :param etag:
        :param worker_id:
        :param runner_id:
        :param metadata:
        :param state:
        :param attempts:
        :param paused:
        :param retries:

        """
        update_kwargs = {
            "state": state,
            "metadata": metadata,
            "attempts": attempts,
        }

        for arg in ["runner_id", "paused", "worker_id"]:
            if arg in kwargs:
                update_kwargs[arg] = kwargs.get(arg)

        return _retry(
            self._db.update_task,
            self.one_task,
            task_id,
            etag,
            kwargs=update_kwargs,
            retries=retries,
        )

    def update_worker(
        self,
        worker_id: str,
        etag: str,
        task_started=None,
        task_finished=None,
        last_ping=None,
        metadata=None,
        retries=0,
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
        :param retries:

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

        return _retry(
            self._db.update_worker,
            self.one_worker,
            worker_id,
            etag,
            kwargs=update_kwargs,
            retries=retries,
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
