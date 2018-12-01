import collections
import uuid

import pymongo
from pymongo import errors as mongo_err

from igor import domain
from igor import exceptions as exc
from igor.database.base import Base


class MongoDB(Base):

    _DEFAULT_LIMIT = 500
    _MAX_LIMIT = 10000

    # database
    _DB = "igor"

    # tables
    _T_JOB = "jobs"
    _T_LYR = "layers"
    _T_TSK = "tasks"

    _A_JOB = "archived_jobs"
    _A_LYR = "archived_layers"
    _A_TSK = "archived_tasks"

    _T_WKR = "workers"
    _T_USR = "users"

    # queries
    _Q_IDLE_WORKERS = {"task_id": None}

    def __init__(self, host='database', port=27017, archive_mode=False):
        self._host = host
        self._port = port

        self.archive_mode = archive_mode

        self._conn = pymongo.MongoClient(host=host, port=port)

        self._jobs = self._conn[self._DB][self._T_JOB]
        self._lyrs = self._conn[self._DB][self._T_LYR]
        self._tsks = self._conn[self._DB][self._T_TSK]

        self._archived_jobs = self._conn[self._DB][self._A_JOB]
        self._archived_lyrs = self._conn[self._DB][self._A_LYR]
        self._archived_tsks = self._conn[self._DB][self._A_TSK]

        self._wkrs = self._conn[self._DB][self._T_WKR]
        self._usrs = self._conn[self._DB][self._T_USR]

    def create_user(self, user: domain.User):
        """Create user.

        :param user:

        """
        data = user.encode()

        data["id"] = user.id
        data["_id"] = user.name  # unique by the user name
        data["user_id"] = user.id

        try:
            result = self._usrs.insert_one(data)
        except mongo_err.DuplicateKeyError:
            raise exc.WriteFailError(f"failed to create user: {user.id} {user.name}, user exists")

        if not result.inserted_id:
            raise exc.WriteFailError(f"failed to create user: {user.id}")

    def delete_user(self, user_id: str):
        """Delete single user.

        :param user_id:

        """
        self._usrs.delete_one({"id": user_id})

    def update_user(self, user_id: str, etag: str, name=None, password=None, metadata=None):
        """Update single user instance.

        :param user_id:
        :param etag:
        :param name:
        :param password: this should be the *hashed* password.
        :param metadata:

        """
        new_tag = str(uuid.uuid4())
        update = {"etag": new_tag}

        for n, v in [
            ("name", name),
            ("password", password),
            ("metadata", metadata),
        ]:
            if v is None:
                continue

            update[n] = v

        result = self._usrs.update_one(
            {"id": user_id, "etag": etag},
            {"$set": update}
        )

        if result.modified_count != 1:
            raise exc.WriteFailError(f"failed to update user: {user_id} {etag}")

        return new_tag

    def get_users(self, query: domain.Query) -> list:
        """Get users workers matching query.

        :param query:
        :return: list

        """
        q = self._filter(
            query,
            user_id=True,
            key=True,
        )
        if query.sorting:
            results = self._usrs.find(q).sort(query.sorting).limit(query.limit).skip(query.offset)
        else:
            results = self._usrs.find(q).limit(query.limit).skip(query.offset)
        return [domain.User.decode(d) for d in results]

    def create_job(self, job: domain.Job, layers: list, tasks: list):
        """Create job.

        :param job:
        :param layers: list of layers
        :param tasks: list of tasks. All tasks must belong to a layer from given "layers"

        """
        layer_ids = [l.id for l in layers]
        task_map = collections.defaultdict(list)

        for task in tasks:
            if task.layer_id not in layer_ids:
                raise exc.WriteFailError(
                    f"layer for layer_id {task.layer_id} not supplied in job {job.id}"
                )

            task_map[task.layer_id].append(task)

        try:
            self._create_job(job)

            self._create_layers(job.id, layers)

            for layer_id, layer_tasks in task_map.items():
                if layer_tasks:
                    self._create_tasks(
                        job.id,
                        layer_id,
                        layer_tasks,
                    )
        except Exception as e:
            # if anything goes wrong remove the whole lot.
            # Ideally we'd have transactions.. but .. you know .. mongodb is too webscale for that
            self._force_delete_job(job.id)
            raise e

    def _create_job(self, job: domain.Job):
        """Create job

        :param job:

        """
        data = job.encode()
        data["_id"] = job.id  # override mongo creating us an ID
        data["job_id"] = job.id

        result = self._jobs.insert_one(data)
        if not result.inserted_id:
            raise exc.WriteFailError(f"failed to create job: {job.id}")

    def update_job(
        self,
        job_id: str,
        etag: str,
        runner_id=None,
        metadata=None,
        state=None,
        **kwargs
    ) -> str:
        """Update job with given fields

        :param job_id:
        :param etag:
        :param runner_id:
        :param metadata:
        :param state:
        :param paused:
        :return: str

        """
        new_tag = str(uuid.uuid4())
        update = {"etag": new_tag}

        for n, v in [
            ("runner_id", runner_id),
            ("metadata", metadata),
            ("state", state),
        ]:
            if v is None:
                continue

            update[n] = v

        if 'paused' in kwargs:
            update["paused"] = kwargs['paused']

        result = self._jobs.update_one(
            {"id": job_id, "etag": etag},
            {"$set": update}
        )

        if result.modified_count != 1:
            raise exc.WriteFailError(f"failed to update job: {job_id} {etag}")

        return new_tag

    def get_jobs(self, query: domain.Query) -> list:
        """Get all jobs matching query.

        :param query:
        :return: list

        """
        q = self._filter(query, job_id=True, state=True, key=True, user_id=True)
        if query.sorting:
            results = self._jobs.find(q).sort(query.sorting).limit(query.limit).skip(query.offset)
        else:
            results = self._jobs.find(q).limit(query.limit).skip(query.offset)
        return [domain.Job.decode(d) for d in results]

    def force_delete_job(self, job_id: str):
        """Forces deletion of job & all children.

        :param job_id:

        """
        if self.archive_mode:
            self._move_documents({"job_id": job_id}, self._tsks, self._archived_tsks)
            self._move_documents({"job_id": job_id}, self._lyrs, self._archived_lyrs)
            self._move_documents({"job_id": job_id}, self._jobs, self._archived_jobs)
        else:
            self._force_delete_job(job_id)

    @staticmethod
    def _move_documents(fltr, source, destination):
        """Move documents matching given filter from source to destination.

        Nb.
         - We upsert into destination
         - Deletion (from source) is aborted if we fail to write

        :param fltr: a mongo query filter
        :param source:
        :param destination:

        """
        limit = 1000
        offset = 0
        found = limit + 1

        while found >= limit:
            write_bulk = destination.initialize_unordered_bulk_op()
            ids = []

            for doc in source.find(fltr).limit(limit).skip(offset):
                doc_id = doc["_id"]
                write_bulk.find({"_id": doc_id}).upsert().replace_one(doc)
                ids.append(doc_id)

            result = write_bulk.execute()

            if result.get("nUpserted", 0) + result.get("nInserted", 0) != len(ids):
                raise exc.WriteFailError(f"error archiving: failed to write data")

            source.delete_many({"_id": {"$in": ids}})

            found = len(ids)
            offset += found

    def _force_delete_job(self, job_id: str):
        """Forces deletion of job & all children.

        :param job_id:

        """
        self._tsks.delete_many({"job_id": job_id})
        self._lyrs.delete_many({"job_id": job_id})
        self._jobs.delete_many({"job_id": job_id})

    def delete_jobs(self, job_ids: list):
        """Delete jobs by ID

        :param job_ids:

        """
        children = self.get_layers(
            domain.Query([domain.Filter(job_ids=[job_ids])], limit=1)
        )
        if children:
            raise exc.ChildExists("child layer(s) exist: unable to delete")

        self._jobs.delete_many({"id": {"$in": job_ids}})

    def _one_job(self, id_: str):
        """Return one job by ID or None

        :param id_:
        :return: Job or None

        """
        result = self._jobs.find_one({"id": id_})
        if result:
            return domain.Job.decode(result)
        return None

    def _one_layer(self, id_: str):
        """Return one layer by ID or None

        :param id_:
        :return: Layer or None

        """
        result = self._lyrs.find_one({"id": id_})
        if result:
            return domain.Layer.decode(result)
        return None

    def _one_worker(self, id_: str):
        """Return one worker by ID or None

        :param id_:
        :return: Worker or None

        """
        result = self._wkrs.find_one({"id": id_})
        if result:
            return domain.Worker.decode(result)
        return None

    def _create_layers(self, job_id: str, layers: list):
        """Create layers.

        :param job_id:
        :param layers:

        """
        ls = []
        for l in layers:
            l.job_id = job_id

            data = l.encode()
            data["_id"] = l.id  # override mongo creating us an ID
            data["layer_id"] = l.id

            ls.append(data)

        result = self._lyrs.insert_many(ls)
        if len(result.inserted_ids) != len(ls):
            raise exc.WriteFailError(
                f"some layers not created: created {result.inserted_ids}"
            )

    def update_layer(
        self,
        layer_id: str,
        etag: str,
        priority=None,
        state=None,
        runner_id=None,
        metadata=None,
        **kwargs
    ):
        """Update given layer with settings.

        :param layer_id:
        :param etag:
        :param priority:
        :param state:
        :param runner_id:
        :param metadata:

        """
        new_tag = str(uuid.uuid4())
        update = {"etag": new_tag}

        if priority:
            priority = abs(priority)

        for n, v in [
            ("runner_id", runner_id),
            ("metadata", metadata),
            ("state", state),
            ("priority", priority),
        ]:
            if v is None:
                continue

            update[n] = v

        if 'paused' in kwargs:
            update["paused"] = kwargs['paused']

        result = self._lyrs.update_one(
            {"id": layer_id, "etag": etag},
            {"$set": update}
        )
        if result.modified_count != 1:
            raise exc.WriteFailError(f"failed to update layer: {layer_id} {etag}")

        return new_tag

    def get_layers(self, query: domain.Query) -> list:
        """Get all layers that match query.

        :param query:
        :return: list

        """
        q = self._filter(
            query,
            job_id=True,
            layer_id=True,
            state=True,
            key=True,
            user_id=True,
        )
        if query.sorting:
            results = self._lyrs.find(q).sort(query.sorting).limit(query.limit).skip(query.offset)
        else:
            results = self._lyrs.find(q).limit(query.limit).skip(query.offset)
        return [domain.Layer.decode(d) for d in results]

    def create_tasks(self, layer_id: str, tasks: list):
        """Create tasks

        :param layer_id:
        :param tasks:

        """
        lyr = self._one_layer(layer_id)
        if not lyr:
            raise exc.LayerNotFound(layer_id)

        job = self._one_job(lyr.job_id)
        if not job:
            raise exc.JobNotFound(lyr.job_id)

        self._create_tasks(job.id, layer_id, tasks)

    def _create_tasks(self, job_id: str, layer_id: str, tasks: list):
        """Create tasks

        :param job_id:
        :param layer_id:
        :param tasks:

        """

        ls = []
        for t in tasks:
            t.job_id = job_id
            t.layer_id = layer_id

            data = t.encode()
            data["_id"] = t.id  # override mongo creating us an ID
            data["task_id"] = t.id

            ls.append(data)

        result = self._tsks.insert_many(ls)
        if len(result.inserted_ids) != len(ls):
            raise exc.WriteFailError(
                f"some tasks not created: created {result.inserted_ids}"
            )

    def update_task(
        self,
        task_id: str,
        etag: str,
        runner_id=None,
        metadata=None,
        state=None,
        attempts=None,
        **kwargs
    ):
        """Update task with given settings.

        :param task_id:
        :param etag:
        :param worker_id:
        :param runner_id:
        :param metadata:
        :param state:
        :param attempts:

        """
        new_tag = str(uuid.uuid4())
        update = {"etag": new_tag}

        worker_id = kwargs.get("worker_id")

        if worker_id:
            if not self._one_worker(worker_id):
                raise exc.WorkerNotFound(worker_id)

        for n, v in [
            ("runner_id", runner_id),
            ("metadata", metadata),
            ("state", state),
            ("attempts", attempts),
        ]:
            if v is None:
                continue

            update[n] = v

        if "worker_id" in kwargs:  # allow setting to none
            update["worker_id"] = kwargs["worker_id"]

        result = self._tsks.update_one(
            {"id": task_id, "etag": etag},
            {"$set": update}
        )
        if result.modified_count != 1:
            raise exc.WriteFailError(f"failed to update task: {task_id} {etag}")

        return new_tag

    def get_tasks(self, query: domain.Query) -> list:
        """Get all tasks matching query.

        :param query:
        :return: list

        """
        q = self._filter(
            query,
            job_id=True,
            layer_id=True,
            task_id=True,
            state=True,
            key=True,
            user_id=True,
        )
        if query.sorting:
            results = self._tsks.find(q).sort(query.sorting).limit(query.limit).skip(query.offset)
        else:
            results = self._tsks.find(q).limit(query.limit).skip(query.offset)
        return [domain.Task.decode(d) for d in results]

    def delete_tasks(self, task_ids: list):
        """Delete tasks by id.

        :param task_ids:

        """
        if not task_ids:
            return

        children = self.get_workers(
            domain.Query([domain.Filter(task_ids=[task_ids])], limit=1)
        )
        if children:
            raise exc.ChildExists("child worker(s) exist: unable to delete")

        self._tsks.delete_many({"id": {"$in": task_ids}})

    def get_workers(self, query: domain.Query) -> list:
        """Get all workers matching query.

        :param query:
        :return: list

        """
        q = self._filter(
            query,
            job_id=True,
            layer_id=True,
            task_id=True,
            worker_id=True,
        )
        if query.sorting:
            results = self._wkrs.find(q).sort(query.sorting).limit(query.limit).skip(query.offset)
        else:
            results = self._wkrs.find(q).limit(query.limit).skip(query.offset)
        return [domain.Worker.decode(d) for d in results]

    # scheduler functions
    def idle_worker_count(self) -> int:
        """Return the number of idle workers

        :return: int

        """
        return self._wkrs.count(self._Q_IDLE_WORKERS)

    def idle_workers(self, limit: int=_DEFAULT_LIMIT, offset: int=0) -> list:
        """Return idle workers.

        :param limit:
        :param offset:
        :return: list

        """
        cur = self._wkrs.find(self._Q_IDLE_WORKERS).limit(limit).skip(offset)
        return [domain.Worker.decode(doc) for doc in cur]

    # daemon functions
    def create_worker(self, worker: domain.Worker):
        """Create worker in system with given data.

        :param worker:

        """
        data = worker.encode()
        data["_id"] = worker.id  # override mongo creating us an ID
        data["worker_id"] = worker.id

        result = self._wkrs.insert_one(data)

        if not result.inserted_id:
            raise exc.WriteFailError(f"failed to create worker: {worker.id}")

    def delete_worker(self, id_: str):
        """Remove worker with matching id.

        :param id_:

        """
        me = self._one_worker(id_)
        if not me:
            raise exc.WorkerNotFound(id_)

        self._wkrs.delete_one({"id": id_})

    def update_worker(
        self,
        worker_id: str,
        etag: str,
        task_started=None,
        task_finished=None,
        last_ping=None,
        metadata=None,
        **kwargs
    ) -> str:
        """Update worker with given settings.

        :param worker_id:
        :param etag:
        :param task_started:
        :param task_finished:
        :param last_ping:
        :param metadata:
        :param job_id:
        :param layer_id:
        :param task_id:
        :return: str

        """
        new_tag = str(uuid.uuid4())
        update = {"etag": new_tag}

        for n, v in [
            ("task_started", task_started),
            ("task_finished", task_finished),
            ("last_ping", last_ping),
            ("metadata", metadata),
        ]:
            if v is None:
                continue

            update[n] = v

        for n in ["job_id", "layer_id", "task_id"]:  # allow setting of None
            if n in kwargs:
                update[n] = kwargs[n]

        result = self._wkrs.update_one(
            {"id": worker_id, "etag": etag},
            {"$set": update}
        )
        if result.modified_count != 1:
            raise exc.WriteFailError(f"failed to update worker: {worker_id} {etag}")

        return new_tag

    @classmethod
    def _filter(
        cls,
        q: domain.Query,
        job_id=False,
        layer_id=False,
        task_id=False,
        worker_id=False,
        state=False,
        key=False,
        user_id=False,
    ) -> dict:
        """Return a query dict that builds a mongo query to match the given
        query obj.

        :param q:
        :param job_id: include checks for job ids
        :param layer_id: include checks for layer ids
        :param task_id: include checks for task ids
        :param worker_id: include checks for worker ids
        :param state: include checks for object state(s)
        :param key: include checks for object key(s)
        :param key: include checks for user_id
        :return: dict

        """
        if not q.filters:
            if user_id and q.user_id:
                return {"user_id": q.user_id}
            return {}

        ors = []
        for f in q.filters:
            ands = {}

            if user_id and q.user_id:
                ands["user_id"] = q.user_id

            if job_id and f.job_ids:
                ands["job_id"] = {"$in": f.job_ids}

            if layer_id and f.layer_ids:
                ands["layer_id"] = {"$in": f.layer_ids}

            if task_id and f.task_ids:
                ands["task_id"] = {"$in": f.task_ids}

            if worker_id and f.worker_ids:
                ands["worker_id"] = {"$in": f.worker_ids}

            if state and f.states:
                ands["state"] = {"$in": f.states}

            if key and f.keys:
                ands["key"] = {"$in": f.keys}

            ors.append(ands)
        return {"$or": ors}
