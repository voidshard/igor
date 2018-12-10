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

    _default_states = [
        enums.State.PENDING,
        enums.State.QUEUED,
        enums.State.RUNNING,
        enums.State.ERRORED,
        enums.State.SKIPPED,
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
        """

        :param req:
        :param etag:
        :param task_id:
        :param result:
        :return: str

        """
        etag, _ = self._apply_task_update(req, etag, task_id, result=result)
        return etag

    def set_task_env(self, req: domain.User, etag, task_id, env):
        """

        :param req:
        :param etag:
        :param task_id:
        :param env:
        :return: str

        """
        if not isinstance(env, dict):
            raise exc.InvalidSpec(f"task env must be dict type, got {env}")

        etag, _ = self._apply_task_update(req, etag, task_id, env={str(k): str(v) for k, v in env})
        return etag

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

        return self._svc.update_task(obj.id, etag, **update), obj

    def retry_task(self, req: domain.User, etag, task_id):
        """Retry a single task & return it's update etag.

        - sets task to pending, allowing it to be queued
        - sets parent layer to queued so the task can be kicked off again

        :param req:
        :param etag:
        :param task_id:
        :return: str

        """
        new_tag, task = self._apply_task_update(
            req,
            etag,
            task_id,
            state=enums.State.PENDING.value,
        )
        self._rnr.send_kill(
            task_id=task_id
        )

        parent = self._svc.one_layer(task.layer_id)
        if parent.state not in [enums.State.QUEUED.value, enums.State.RUNNING.value]:
            self._svc.update_layer(
                parent.id,
                parent.etag,
                state=enums.State.QUEUED.value,
            )

        return new_tag

    def perform_pause(self, req: domain.User, etag, job_id=None, layer_id=None, task_id=None):
        """A kill order makes currently running matching tasks die, and
        allows them to retry normally.

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
            return update_one(obj.id, etag, paused=None)
        else:
            return update_one(obj.id, etag, paused=time.time())

    def perform_kill(self, req: domain.User, etag, job_id=None, layer_id=None, task_id=None):
        """A kill order makes currently running matching tasks die, and
        allows them to retry normally.

        One of job_id, layer_id, task_id must be given for this to do anything.

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

        kill_meta = {
            "killed_at": time.time(),
            "killed_by": req.id,
        }

        for t in self._svc.get_tasks(query):
            if t.user_id != req.id and not req.is_admin:
                raise exc.Forbidden(f"user {req.id} may not kill task {t.id}")

            meta = t.metadata
            meta.update(kill_meta)

            logger.info(f"killing task: requester:{req.id} task_id:{t.id}")

            self._svc.update_task(
                t.id,
                etag,
                state=enums.State.ERRORED.value,
                worker_id=None,
                meta=meta,
                attempts=t.attempts + 1,
            )
            self._rnr.send_kill(
                task_id=t.id
            )

    @classmethod
    def _build_query(cls, qspec: spec.QuerySpec) -> domain.Query:
        """Build a domain.Query from a QuerySpec object.

        :param qspec:
        :return: domain.Query

        """
        q = domain.Query()

        q.user_id = qspec.user_id
        q.limit = qspec.limit
        q.offset = qspec.offset
        q.filters = []

        for fspec in qspec.filters:
            f = domain.Filter()

            f.keys = fspec.keys
            f.job_ids = fspec.job_ids
            f.layer_ids = fspec.layer_ids
            f.task_ids = fspec.task_ids
            f.states = fspec.states

            if not f.states:
                f.states = cls._default_states

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

        logger.info(f"fetch jobs: requester:{req.id}")
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

        logger.info(f"fetch layers: requester:{req.id}")
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

        logger.info(f"fetch tasks: requester:{req.id}")
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
        mod.user_id = layer.user_id
        mod.cmd = task.cmd
        mod.env = task.env
        mod.metadata = {"max_attempts": task.max_attempts}
        if task.paused:
            mod.paused = time.time()

        return mod

    def create_task(self, req: domain.User, layer_id: str, task: spec.TaskSpec) -> (str, str):
        """Create a task for an existing layer.

        The layer must be PENDING.

        Returns id and tag of created task.

        :param req: user making request
        :param layer_id:
        :param task:
        :return: str, str
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

        logger.info("task created: requester:{req.id} task_id:{t.id}")

        return t.id, t.etag

    def create_job(self, req: domain.User, job: spec.JobSpec) -> (str, str):
        """Create job given spec with layers & tasks.

        Returns id and etag of created job.

        :param req: requester
        :param job:
        :return: str, str

        """
        if not job.user_id:
            job.user_id = req.id

        if req.id != job.user_id and not req.is_admin:
            raise exc.Forbidden(f"user {req.id} cannot create job as user {job.user_id}")

        mod = domain.Job(key=job.name)

        mod.user_id = job.user_id
        meta = self._start_meta(req)
        meta["logger"] = job.logger
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
        return mod.id, mod.etag

    def create_user(self, req: domain.User, user: spec.UserSpec) -> (str, str):
        """Create a new user.

        Nb. only admins may use this.

        Return id and tag of created user.

        :param req:
        :param user:
        :return: str, str
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

        return mod.id, mod.etag

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
        logger.info("job fetch: requester:{req.id} job_id:{id_}")
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
        logger.info("layer fetch: requester:{req.id} layer_id:{id_}")
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
        logger.info("task fetch: requester:{req.id} task_id:{id_}")
        return obj

    def one_worker(self, req: domain.User, id_: str):
        """Return one task by id or raise.

        :param req: requesting user
        :param id_:
        :rasies Forbidden:

        """
        if not req.is_admin:
            raise exc.Forbidden(f"user {req.id} may not view workers")
        logger.info("worker fetch: requester:{req.id} worker_id:{id_}")
        return self._svc.one_worker(id_)
