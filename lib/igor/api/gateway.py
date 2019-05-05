import time

from collections import defaultdict

from igor.api.domain import spec
from igor import domain
from igor import enums
from igor import utils
from igor.api import exceptions as exc
from igor import exceptions as igor_exc


logger = utils.logger()


class IgorGateway:
    """All publicly accessible funcs come through here. This 'gateway' is what is 'served'
    up by the various transport mechanisms in api/transport.

    That is, where the IgorDbService is the main gateway to the db for internal code, the gateway
    class provides things like Authorization, sanity checking & translating external API objects
    (object creation & query specs) to internal domain objects before calling the main service(s).

    """

    _INVALID_RETRY_STATES = [
        enums.State.PENDING.value,
        enums.State.QUEUED.value,
        enums.State.RUNNING.value,
    ]

    def __init__(self, service, runner):
        self._svc = service
        self._rnr = runner

    @staticmethod
    def _start_meta(req: domain.User) -> dict:
        """Return generic data about obj creation.

        :param req: requesting user
        :return: dict

        """
        return {
            "created": time.time(),
            "created_by": req.id,
        }

    def authenticate_user(self, user_name: str, password: str) -> domain.User:
        """Return user matching ID, providing the password matches.

        :param user_name:
        :param password:
        :return: domain.User

        """
        # TODO: probably authentication should be managed by a plugin
        obj = self._svc.one_user_by_name(user_name)

        if not obj.is_password(password):
            logger.warn(f"auth failure for user: user_id:{user_name}")
            raise igor_exc.UserNotFound(user_name)

        logger.info(f"auth success for user: user_id:{user_name}")
        return obj

    def set_task_result(self, req: domain.User, etag, task_id, result):
        """Set the result attribute of a Task obj.

        :param req:
        :param etag:
        :param task_id:
        :param result:
        :return: str

        """
        return self._apply_task_update(req, etag, task_id, result=result)

    def set_task_env(self, req: domain.User, etag, task_id, env):
        """Set the env attribute of a Task obj.

        :param req:
        :param etag:
        :param task_id:
        :param env:
        :return: str

        """
        if not isinstance(env, dict):
            raise exc.InvalidSpec(f"task env must be dict type, got {env}")

        return self._apply_task_update(
            req, etag, task_id, env={str(k): str(v) for k, v in env.items()}
        )

    def _apply_task_update(self, req: domain.User, etag, task_id, **update):
        """Apply some update to a task & return the new etag & task obj

        :param req:
        :param etag:
        :param task_id:
        :param update:
        :return: str, domain.Task

        """
        if not task_id:
            return

        if not etag:
            raise exc.WriteConflict(f"no etag given")

        obj = self._svc.one_task(task_id)
        if obj.user_id != req.id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not update object {task_id}")

        return {task_id: self._svc.update_task(obj.id, etag, **update)}

    def perform_pause(self, req: domain.User, etag, job_id=None, layer_id=None, task_id=None):
        """A pause order sets the paused state on given tasks.

        "Pause" means that the task is *not* (re)scheduled by the system. It does *not* stop
        currently running tasks.

        Ie. a running task "foo" that is paused is not stopped. It is marked paused so that
        it will not be launched (if not already running) and/or will not be re-launched in the
        event that it fails.

        One of job_id, layer_id, task_id must be given for this to do anything.

        :param req: requesting user
        :param etag: tag of the object one wishes to update
        :param job_id:
        :param layer_id:
        :param task_id:
        :raises Forbidden:
        :raises WriteConflict:

        """
        if not etag:
            raise exc.WriteConflict(f"no etag given")

        obj_id = job_id or layer_id or task_id
        if not obj_id:
            return

        if job_id:
            fetch_one = self._svc.one_job
            update_one = self._svc.update_job
        elif layer_id:
            fetch_one = self._svc.one_layer
            update_one = self._svc.update_layer
        else:
            fetch_one = self._svc.one_task
            update_one = self._svc.update_task

        obj = fetch_one(obj_id)
        if obj.user_id != req.id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not update object {obj_id}")

        logger.info(f"performing pause toggle: requester:{req.id} object:{obj_id}")

        if obj.paused:
            return {obj.id: update_one(obj.id, etag, paused=None)}
        else:
            return {obj.id: update_one(obj.id, etag, paused=time.time())}

    def perform_skip(self, req: domain.User, etag, job_id=None, layer_id=None, task_id=None):
        """A skip order makes currently running matching tasks 'skip'.

        One of job_id, layer_id, task_id must be given for this to do anything.

        Skip is basically an end state that the system essentially does nothing with.

        :param req: requesting user
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:
        :raises Forbidden:

        """
        if not etag:
            raise exc.WriteConflict(f"no etag given")

        if not any([job_id, layer_id, task_id]):
            return

        query = domain.Query(
            filters=[
                domain.Filter(job_ids=[job_id], layer_ids=[layer_id], task_ids=[task_id])
            ]
        )

        updated = {}
        for t in self._svc.get_tasks(query):
            if t.user_id != req.id and not req.is_admin:
                raise exc.Forbidden(f"user {req.id} may not skip task {t.id}")

            if t.worker_id:
                logger.info(f"skipping task: requester:{req.id} task:{t.id} worker:{t.worker_id}")
                self._rnr.send_kill(task_id=t.id, worker_id=t.worker_id)
            else:
                logger.info(f"skipping task: requester:{req.id} task:{t.id}")

            tag = self._svc.force_task_state(
               t,
               None,
               attempts=t.attempts + 1,
               reason=f"skipped by user:{req.id}",
               final_state=enums.State.SKIPPED.value,
            )
            updated[t.id] = tag
        return updated

    def perform_retry(self, req: domain.User, etag, job_id=None, layer_id=None, task_id=None):
        """Perform a retry on a job, layer or task.

        Retry sets the matching Tasks & Layer to PENDING, matching Jobs to RUNNING.

        One of the three must be given ..

        :param req:
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:

        """
        if not etag:
            raise exc.WriteConflict(f"no etag given")

        if job_id:
            job, tasks_updated = self._retry_job(req, etag, job_id)
            return tasks_updated

        elif layer_id:
            layer, tasks_updated = self._retry_layer(req, etag, layer_id)
            self._svc.update_job(layer.job_id, None, state=enums.State.RUNNING.value)
            return tasks_updated

        elif task_id:
            task = self._retry_task(req, etag, task_id)
            self._svc.update_job(task.job_id, None, state=enums.State.RUNNING.value)
            self._svc.update_layer(task.layer_id, None, state=enums.State.PENDING.value)
            return {task.id: task.etag}

    def _retry_job(self, req: domain.User, etag, job_id):
        """Retry all COMPLETED, ERRORED and SKIPPED tasks of a Job.

        - Job set to RUNNING
        - Layers are returned to PENDING
        - Tasks are returned to PENDING

        :param req:
        :param etag:
        :param job_id:

        """
        job = self._svc.one_job(job_id)
        if job.user_id != req.id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not retry job {job.id}")

        if job.state in self._INVALID_RETRY_STATES:
            raise exc.InvalidState("cannot retry job in current state")

        if job.etag != etag:
            raise exc.WriteConflict(f"etag mismatch")

        query = domain.Query(filters=[
            domain.Filter(
                job_ids=[job_id],
                states=[
                    enums.State.ERRORED.value,
                    enums.State.COMPLETED.value,
                    enums.State.SKIPPED.value,
                ]),
            ]
        )
        layers = self._svc.get_layers(query)

        updated = {}

        for layer in layers:
            _, tasks_retried = self._retry_layer(req, layer.etag, layer.id)
            updated.update(tasks_retried)

        tag = self._svc.update_job(job.id, job.etag, state=enums.State.RUNNING.value)
        job.etag = tag
        return job, updated

    def _retry_layer(self, req: domain.User, etag, layer):
        """Retry all COMPLETED, ERRORED and SKIPPED tasks of a Layer.

        - Job set to RUNNING
        - Layer is returned to PENDING
        - Tasks are returned to PENDING

        :param req:
        :param etag:
        :param layer:

        """
        layer = self._svc.one_layer(layer) if isinstance(layer, str) else layer

        if layer.user_id != req.id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not retry layer {layer.id}")

        if layer.state in self._INVALID_RETRY_STATES:
            raise exc.InvalidState("cannot retry layer in current state")

        if layer.etag != etag:
            raise exc.WriteConflict(f"etag mismatch")

        query = domain.Query(filters=[domain.Filter(layer_ids=[layer.id])])
        tasks = self._svc.get_tasks(query)

        updated = {}

        for task in tasks:
            try:
                t = self._retry_task(req, task.etag, task)
                updated[t.id] = t.etag
            except exc.InvalidState:
                continue

        tag = self._svc.update_layer(layer.id, etag, state=enums.State.PENDING.value)
        layer.etag = tag
        return layer, updated

    def _retry_task(self, req: domain.User, etag, task):
        """Retry a task by returning it to the PENDING state.

        :param req:
        :param task:

        """
        task = self._svc.one_task(task) if isinstance(task, str) else task

        if task.user_id != req.id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not retry task {task.id}")

        if task.state in self._INVALID_RETRY_STATES:
            raise exc.InvalidState("cannot retry task in current state")

        if task.etag != etag:
            raise exc.WriteConflict(f"etag mismatch")

        tag = self._svc.force_task_state(
            task,
            None,
            task.attempts + 1,
            f"retried by user:{req.id}",
            final_state=enums.State.PENDING.value
        )
        task.etag = tag
        return task

    def perform_kill(self, req: domain.User, etag, job_id=None, layer_id=None, task_id=None):
        """A kill order makes currently running matching tasks die, and allows them to retry
        normally.

        One of job_id, layer_id, task_id must be given for this to do anything.

        :param req: requesting user
        :param etag:
        :param job_id:
        :param layer_id:
        :param task_id:
        :return dict:
        :raises Forbidden:

        """
        if not etag:
            raise exc.WriteConflict(f"no etag given")

        if not any([job_id, layer_id, task_id]):
            return

        query = domain.Query(
            filters=[
                domain.Filter(job_ids=[job_id], layer_ids=[layer_id], task_ids=[task_id])
            ]
        )

        workers = {w.task_id: w for w in self._svc.get_workers(query)}
        updated = {}

        for t in self._svc.get_tasks(query):
            if t.user_id != req.id and not req.is_admin:
                raise exc.Forbidden(f"user {req.id} may not kill task {t.id}")

            worker = workers.get(t.id)
            if not worker:
                logger.info(f"cannot kill task, not running: requester:{req.id} task:{t.id}")
                continue

            logger.info(f"killing task: requester:{req.id} task:{t.id} worker:{worker.id}")

            self._rnr.send_kill(task_id=t.id, worker_id=worker.id)
            tag = self._svc.stop_work_task(
                worker,
                t,
                enums.State.ERRORED.value,
                reason=f"killed by user:{req.id}",
                attempts=t.attempts + 1,
            )
            updated[t.id] = tag

        return updated

    @classmethod
    def _build_query(cls, qspec: spec.QuerySpec) -> domain.Query:
        """Build a domain.Query from a QuerySpec object.

        :param qspec:
        :return: domain.Query

        """
        q = domain.Query()

        q.limit = qspec.limit
        q.offset = qspec.offset
        q.filters = []

        for fspec in qspec.filters:
            f = domain.Filter()

            f.keys = fspec.keys
            f.job_ids = fspec.job_ids
            f.layer_ids = fspec.layer_ids
            f.worker_ids = fspec.worker_ids
            f.task_ids = fspec.task_ids
            f.states = fspec.states

            q.filters.append(f)

        return q

    def get_jobs(self, req: domain.User, qspec: spec.QuerySpec) -> list:
        """Get jobs given some query spec.

        Non admins have the query search for only their objects.

        :param req: user requesting information
        :param qspec: query
        :return: list

        """
        query = self._build_query(qspec)
        if not req.is_admin:  # non admins can only search for their own stuff.
            query.user_id = req.id

        logger.info(f"fetch jobs: requester:{req.id} admin:{req.is_admin}")
        return self._svc.get_jobs(query)

    def get_layers(self, req: domain.User, qspec: spec.QuerySpec) -> list:
        """Get layers given some query spec.

        Non admins have the query search for only their objects.

        :param req: user requesting information
        :param qspec: query
        :return: list

        """
        query = self._build_query(qspec)

        if not req.is_admin:  # non admins can only search for their own stuff.
            query.user_id = req.id

        logger.info(f"fetch layers: requester:{req.id} admin:{req.is_admin}")
        return self._svc.get_layers(query)

    def get_tasks(self, req: domain.User, qspec: spec.QuerySpec) -> list:
        """Get tasks given some query spec.

        Non admins have the query search for only their objects.

        :param req: user requesting information
        :param qspec: query
        :return: list

        """
        query = self._build_query(qspec)

        if not req.is_admin:  # non admins can only search for their own stuff.
            query.user_id = req.id

        logger.info(f"fetch tasks: requester:{req.id} admin:{req.is_admin}")
        return self._svc.get_tasks(query)

    def get_workers(self, req: domain.User, qspec: spec.QuerySpec) -> list:
        """Get workers that match the given query.

        Nb. only admins can call this.

        :param req: user requesting the information
        :param qspec: query
        :return: list
        :raises Forbidden:

        """
        if not req.is_admin:  # non admins cannot search for workers
            raise exc.Forbidden(f"user {req.id} is not permitted to call get_workers")

        query = self._build_query(qspec)
        logger.info(f"fetch workers: requester:{req.id}")
        return self._svc.get_workers(query)

    def _build_layer(self, job_id: str, user_id: str, layer_spec: spec.LayerSpec):
        """Build single layer & child tasks.

        :param job_id:
        :param user_id:
        :param layer_spec:
        :return: domain.Layer, []domain.Task

        """
        layer = domain.Layer(key=layer_spec.name)

        layer.job_id = job_id
        layer.key = layer_spec.name
        layer.user_id = user_id
        layer.order = layer_spec.order
        layer.priority = layer_spec.priority

        if layer_spec.paused:
            layer.paused = time.time()

        tasks = [self._build_task(layer, tsk) for tsk in layer_spec.tasks]

        return layer, tasks

    def _build_layers(self, job_id: str, job: spec.JobSpec) -> (list, list):
        """Builds a suite of layers.

        Also responsible for setting inter layer dependencies: (siblings, parents, children)

        :param job_id:
        :param job:
        :return: list, list

        """
        all_layers = []
        lyrs = defaultdict(list)
        tasks = []

        for layer_spec in job.layers:  # build & bucket layers by order
            layer, child_tasks = self._build_layer(job_id, job.user_id, layer_spec)

            lyrs[layer.order].append(layer)
            all_layers.append(layer)

            if child_tasks:
                tasks.extend(child_tasks)

        order_keys = sorted(list(lyrs.keys()))

        # run through the layers and sort out their parents / children / siblings
        for i in range(0, len(order_keys)):
            parents = []
            if i > 0:
                parents = [l.id for l in lyrs[order_keys[i - 1]]]

            children = []
            if i < len(order_keys) - 1:
                children = [l.id for l in lyrs[order_keys[i + 1]]]

            siblings = lyrs[order_keys[i]]
            for layer in siblings:
                layer.siblings = [l.id for l in siblings if l.id != layer.id]
                layer.children = children
                layer.parents = parents

                if layer.parents:
                    continue

                # if the layer has no parents then we can consider starting it immediately
                layer.state = enums.State.QUEUED.value

        return all_layers, tasks

    @staticmethod
    def _build_task(layer: domain.Layer, task: spec.TaskSpec) -> domain.Task:
        """Build domain.Task from a task spec.

        :param layer:
        :param task:
        :return: domain.Task

        """
        mod = domain.Task(key=task.name)

        mod.job_id = layer.job_id
        mod.layer_id = layer.id
        mod.key = task.name
        mod.user_id = layer.user_id
        mod.cmd = task.cmd
        mod.env = task.env
        mod.max_attempts = task.max_attempts
        mod.metadata = {}
        if task.paused:
            mod.paused = time.time()

        return mod

    def create_task(self, req: domain.User, layer_id: str, task: spec.TaskSpec) -> dict:
        """Create a task for an existing layer.

        The layer must be PENDING.

        Returns id and tag of created task.

        :param req: user making request
        :param layer_id:
        :param task:
        :return: dict
        :raises Forbidden:
        :raises InvalidState: if layer is not pending

        """
        layer = self._svc.one_layer(layer_id)
        if req.id != layer.user_id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot create task as user {layer.user_id}")

        if layer.state != enums.State.PENDING.value:
            raise exc.InvalidState(f"layer {layer_id} not in PENDING state.")

        t = self._build_task(layer, task)
        t.metadata.update(self._start_meta(req))

        self._svc.create_tasks(layer_id, [t])

        logger.info(f"task created: requester:{req.id} task_id:{t.id}")

        return {t.id: t.etag}

    def create_job(self, req: domain.User, job: spec.JobSpec) -> dict:
        """Create job given spec with layers & tasks.

        Returns id and etag of created job.

        :param req: requester
        :param job:
        :return: dict

        """
        if not job.user_id:
            job.user_id = req.id

        if req.id != job.user_id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot create job as user {job.user_id}")

        mod = domain.Job(key=job.name)

        mod.user_id = job.user_id
        mod.key = job.name
        meta = self._start_meta(req)
        mod.metadata = meta
        if job.paused:
            mod.paused = time.time()

        layers, tasks = self._build_layers(mod.id, job)

        self._svc.create_job(mod, layers, tasks)

        n_layers = len(layers)
        n_tasks = len(tasks)
        logger.info(
            f"job created: requester:{req.id} layers:{n_layers} tasks:{n_tasks} job_id:{mod.id}"
        )
        return {mod.id: mod.etag}

    def create_user(self, req: domain.User, user: spec.UserSpec) -> dict:
        """Create a new user.

        Nb. only admins may use this.

        Return id and tag of created user.

        :param req:
        :param user:
        :return: dict
        :raises Forbidden:

        """
        if not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot create users")

        mod = domain.User()
        mod.name = user.name
        mod.password = user.password
        mod.is_admin = user.is_admin
        mod.metadata = self._start_meta(req)

        logger.info(f"created user: requester:{req.id} user_id:{user.id}")

        return {mod.id: mod.etag}

    def one_job(self, req: domain.User, id_: str) -> domain.Job:
        """Return one job by id or raise.

        :param req: requesting user
        :param id_:
        :return: domain.Job
        :raises Forbidden:

        """
        obj = self._svc.one_job(id_)
        if req.id != obj.user_id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot view {id_}")
        logger.info(f"job fetch: requester:{req.id} job_id:{id_}")
        return obj

    def one_layer(self, req: domain.User, id_: str) -> domain.Layer:
        """Return one layer by id or raise.

        :param req: requesting user
        :param id_:
        :returns: domain.Layer
        :raises Forbidden:

        """
        obj = self._svc.one_layer(id_)
        if req.id != obj.user_id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot view {id_}")
        logger.info(f"layer fetch: requester:{req.id} layer_id:{id_}")
        return obj

    def one_task(self, req: domain.User, id_: str) -> domain.Task:
        """Return one task by id or raise.

        :param req: requesting user
        :param id_:
        :raises Forbidden:

        """
        obj = self._svc.one_task(id_)
        if req.id != obj.user_id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot view {id_}")
        logger.info(f"task fetch: requester:{req.id} task_id:{id_}")
        return obj

    def one_worker(self, req: domain.User, id_: str):
        """Return one task by id or raise.

        :param req: requesting user
        :param id_:
        :rasies Forbidden:

        """
        if not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not view workers")
        logger.info(f"worker fetch: requester:{req.id} worker_id:{id_}")
        return self._svc.one_worker(id_)
