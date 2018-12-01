import time

from igor.scheduler.base import Base
from igor import enums
from igor import domain
from igor import utils
from igor import exceptions as exc


logger = utils.logger()


class Simple(Base):
    """The most straight forward kind of scheduler.

    """

    def __init__(
        self,
        service,
        runner,
        iteration_time=10,
        fetch_size_min=1000,
        fetch_size_max=5000,
        worker_orphan_time=5 * 60,  # 5 mins
        queued_tasks_min=10,
    ):
        """A simple scheduler service.

        Nb. YOU will need to ensure only one scheduler is running at a time. There is no code
        in the simple implementation here that accounts for this madness.

        The subtasks here (orphaning workers, checking running layers etc) could easily (and more
        ideally) be their own service(s) to free up the scheduler so it's always pushing out more
        work. That's left as an exercise for the future if required.

        :param service: igor db service
        :param runner: some 'runner' service for task management
        :param fetch_size_min: the normal number of entries we'll fetch from the db in one go.
        :param fetch_size_max: the maximum number of entries we'll ever fetch from the db in
            one go.
        :param worker_orphan_time: how long (in seconds) between ping times a worker should be
            before we decide it's dead. If we decide it's gone then we will; remove it, order it's
            task(s) to be killed, order the worker to pause & requeue the task(s) it was working
            on to be processed (this doesn't count as a task failure). This time is not exact -
            we might take more time to remove old workers if we're too busy pushing out more work.
        :param iteration_time: time we should spend between 'scheduler' loops.
        :param queued_tasks_min: the minimum number of tasks that should be waiting for processing.
            The understanding here is that queued tasks can (most likely) no longer have their
            priority decided as they're already in the system & could start getting processed at
            any moment.

        """
        self.iteration_time = iteration_time
        self._fetch_size_min = fetch_size_min
        self._fetch_size_max = fetch_size_max
        self._worker_orphan_time = worker_orphan_time
        self._queued_tasks_min = queued_tasks_min

        self._run = True

        self._svc = service
        self._rnr = runner

    def stop(self):
        """Order scheduler to stop.

        """
        self._run = False

    def _get_layer_tasks(self, layer_id, states=None):
        """Get child tasks of a given layer.

        :param layer_id:
        :param states: states to filter by, defaults to pending.
        :return: []domain.Task

        """
        if not states:
            states = [enums.State.PENDING.value]

        return self._svc.get_tasks(domain.Query(
            filters=[
                domain.Filter(
                    layer_ids=[layer_id],
                    states=states,
                )
            ]
        ))

    def _queue_work(self):
        """Queue up tasks from queued layers (if any) assuming we have workers that are idle OR
        our number of queued tasks is too low.

        Nb. This probably isn't the most efficient at all, but this IS the simple implementation..

        """
        tasks_queued = self._rnr.queued_tasks()
        idle = self._svc.idle_worker_count()

        logger.info(f"queuing work: tasks_queued:{tasks_queued} idle_workers:{idle}")

        if tasks_queued >= self._queued_tasks_min and idle == 0:
            # there are no idle workers and we've got enough tasks queued .. move on.
            return

        # iterate through waiting layers and launch tasks, until we've launched enough (ish)
        launched = 0
        offset = 0
        limit = max(self._fetch_size_max, idle + self._fetch_size_min)
        found = limit + 1

        set_jobs_running = set()

        while found >= limit:
            layers = self._svc.get_layers(
                query=domain.Query(
                    filters=[
                        domain.Filter(
                            states=[enums.State.QUEUED.value],
                        ),
                    ],
                    limit=limit,
                    offset=offset,
                    sorting="priority",
                )
            )
            found = len(layers)
            offset += found

            if not layers:
                continue

            all_jobs = self._svc.get_jobs(
                query=domain.Query(
                    filters=[
                        domain.Filter(job_ids=[l.job_id for l in layers]),
                    ],
                )
            )
            jobs = {j.id: j for j in all_jobs}

            for l in layers:
                if l.paused:
                    logger.info(f"skipping layer {l.id}, currently paused.")
                    continue

                parent = jobs[l.job_id]
                if parent.paused:
                    logger.info(f"skipping layer {l.id}, job {parent.id}, currently paused.")
                    continue

                tasks = [
                    t for t in self._get_layer_tasks(l.id, states=[enums.State.PENDING.value])
                    if not t.paused
                ]
                tasks_fired = self._rnr.queue_tasks(l, tasks)

                self._svc.update_layer(
                    l.id,
                    l.etag,
                    state=enums.State.RUNNING.value,
                    retries=3,
                )
                if not l.parents:
                    # ie: no layers from this job have run before if we're starting this layer
                    set_jobs_running.add(l.job_id)

                launched += tasks_fired

                logger.info(f"launched layer: layer_id:{l.id} tasks:{tasks_fired}")

                if launched >= idle:
                    break

        for job_id in set_jobs_running:
            self._update_job(job_id, enums.State.RUNNING.value)

    def _stop_worker(self, worker: domain.Worker):
        """Kills worker.

        This implies
         - (incase the worker is listening) the worker is ordered to pause (not pick up more work)
         - (incase the worker is listening) task(s) the worker are doing are killed
         - the worker is removed from the db
         - the task is returned to the pending state (to be re-queued)

        A worker that *was* processing and finds itself removed from the system is expected to
        exit or otherwise restart the daemon & possibly rejoin later.

        :param worker:

        """
        logger.info(f"stopping worker: worker:{worker.id} task: {worker.task_id}")
        self._rnr.send_worker_pause(worker.id)

        if not worker.task_id:
            self._svc.delete_worker(worker.id)
            return  # the worker wasn't running a task so we're ok

        self._rnr.send_kill(worker_id=worker.id, task_id=worker.task_id)

        tsk = self._svc.one_task(worker.task_id)
        if not tsk:
            return

        self._svc.update_task(
            tsk.id,
            tsk.etag,
            state=enums.State.PENDING.value,
            worker_id=None,
            metadata=tsk.work_record_update(
                worker.id,
                worker.host,
                enums.State.PENDING.value,
                reason=f"task rescheduled: worker {worker.id} exceeded max ping time",
            ),
            retries=3,
        )

        self._svc.delete_worker(worker.id)

    def _orphan_workers(self):
        """Simple task to find all workers that haven't pinged us within some time period
        and remove them.

        """
        logger.info("running: orphan_workers")

        offset = 0
        limit = self._fetch_size_min
        found = limit + 1

        while found >= limit:
            workers = self._svc.get_workers(
                domain.Query(limit=limit, offset=offset, sorting="worker_id")
            )
            now = time.time()

            for w in workers:
                if now > w.last_ping + self._worker_orphan_time:
                    try:
                        self._stop_worker(w)
                    except Exception as e:
                        logger.error(f"unable to stop worker: {w.id} {e}")

            found = len(workers)
            offset += found

    def _check_running_layer(self, layer):
        """Check all tasks of the layer
            - if anything is RUNNING, the layer is still 'running'

            - if everything is SKIPPED or COMPLETED: mark completed
            - if any tasks are PENDING / QUEUED: launch them
            - if any tasks are ERRORED:
                - if the task has not had enough retries, relaunch it
                - otherwise mark the layer ERRORED

            - If the layer becomes COMPLETED, check if child layer(s) can be QUEUED

        :param layer:

        """
        tasks = self._get_layer_tasks(
            layer.id,
            states=[
                enums.State.RUNNING.value,
                enums.State.PENDING.value,
                enums.State.QUEUED.value,
                enums.State.ERRORED.value,
            ],
        )

        num_tasks = len(tasks)
        logger.info(f"checking layer: layer_id:{layer.id} tasks:{num_tasks}")
        if num_tasks == 0:
            # all tasks are either "COMPLETED" or "SKIPPED" so layer should be marked COMPLETED
            self._complete_layer(layer)
            return

        to_launch = []
        still_going = []
        errored = []

        for t in tasks:
            if t.state in [
                enums.State.RUNNING.value, t.state == enums.State.QUEUED
            ]:
                still_going.append(t)

            elif t.state == enums.State.PENDING:
                to_launch.append(t)
                still_going.append(t)

            else:  # state == ERRORED
                if t.attempts >= t.max_attempts:
                    errored.append(t)
                else:
                    to_launch.append(t)
                    still_going.append(t)

        if errored and not still_going:
            # Something is errored & we've done all we can: mark layer as errored.
            self._svc.update_layer(
                layer.id, layer.etag, state=enums.State.ERRORED.value, retries=3
            )

            # since we're given up on completing this layer, we know that the job too is stuck :(
            self._update_job(layer.job_id, state=enums.State.ERRORED.value)
            return

        if to_launch:
            # We've decided to queue some tasks up again ..
            self._rnr.queue_tasks(layer, tasks)

    def _complete_layer(self, layer: domain.Layer):
        """This layer is done - we need to figure out what other layer(s) can be launched
        and mark them queued so the scheduler finds them.

        :param layer:

        """
        logger.info(f"layer complete: layer_id:{layer.id}")

        self._svc.update_layer(
            layer.id, layer.etag, state=enums.State.COMPLETED.value, retries=3
        )

        if layer.siblings:
            siblings_not_finished = self._svc.get_layers(
                domain.Query(
                    filters=[
                        domain.Filter(
                            layer_ids=layer.siblings,
                            states=[
                                enums.State.PENDING.value,
                                enums.State.QUEUED.value,
                                enums.State.ERRORED.value,
                                enums.State.RUNNING.value,
                            ]
                        )
                    ]
                )
            )

            if siblings_not_finished:
                return

        if not layer.children:
            self._update_job(layer.job_id, enums.State.COMPLETED.value)
            return

        next_layers = self._svc.get_layers(
            domain.Query(
                filters=[
                    domain.Filter(
                        layer_ids=layer.children,
                        states=[enums.State.PENDING.value]
                    )
                ]
            )
        )
        if not next_layers:
            self._update_job(layer.job_id, enums.State.COMPLETED.value)
            return

        for l in next_layers:
            self._svc.update_layer(l.id, l.etag, state=enums.State.QUEUED.value, retries=3)

    def _update_job(self, job_id: str, state: str):
        """Update state of the job with the given ID.

        :param job_id:
        :param state:

        """
        job = self._svc.one_job(job_id)
        if not job:
            return
        self._svc.update_job(job.id, job.etag, state=state, retries=3)

    def _check_running_layers(self):
        """Iterate through running layers & check their tasks for errors / completion etc.

        """
        logger.info("checking running layers")

        offset = 0
        limit = self._fetch_size_min
        found = limit + 1

        while found >= limit:
            layers = self._svc.get_layers(
                query=domain.Query(
                    filters=[
                        domain.Filter(
                            states=[enums.State.RUNNING.value],
                        )
                    ],
                    limit=limit,
                    offset=offset,
                    sorting="layer_id",
                )
            )

            for l in layers:
                self._check_running_layer(l)

            found = len(layers)
            offset += found

    def _delete_job(self, job: domain.Job):
        """Delete a job & all child objects.

        """
        logger.info(f"deleting job: job_id:{job.id}")
        self._rnr.send_kill(job_id=job.id)
        self._svc.force_delete_job(job.id)

    def _delete_completed_jobs(self):
        """Remove jobs that are now completed from the DB.

        """
        logger.info("deleting completed jobs")
        offset = 0
        limit = self._fetch_size_min
        found = limit + 1

        while found >= limit:
            jobs = self._svc.get_jobs(
                query=domain.Query(
                    filters=[
                        domain.Filter(
                            states=[enums.State.COMPLETED.value],
                        )
                    ],
                    limit=limit,
                    offset=offset,
                    sorting="job_id",
                )
            )
            for j in jobs:
                self._delete_job(j)

            found = len(jobs)
            offset += found

    def _create_default_admin_users(self):
        """Provides a simple way for default users to be created.

        """
        logger.info("creating default users")
        for username, password in utils.default_admin_users():
            user = domain.User(name=username)
            user.password = password
            user.is_admin = True

            try:
                self._svc.create_user(user)
            except exc.WriteFailError:
                logger.warn(f"failed to create admin: {username}, assuming user already exists")
                pass

            logger.info(f"created admin: {username}")

    def run(self):
        """Run required tasks to ensure
         - our queue is always feed new tasks (_queue_work)
         - our running layers need help (_check_running_layers)
         - workers that have stopped replying to us are removed (_orphan_workers)
         - old data is archived (_delete_completed_jobs)

        Nb. Ideally all of these would be run in their own processes / threads / hosts etc.
        But for the simple implementation they're all run here.

        """
        self._create_default_admin_users()

        self._orphan_workers()

        self._delete_completed_jobs()

        while self._run:
            start_time = time.time()
            logger.info(f"running scheduler {start_time}")

            # these things need to get done
            self._queue_work()

            self._check_running_layers()

            # things things we'd like to do if we've got time
            time_left = self.iteration_time - (time.time() - start_time)
            if time_left > 0:
                self._orphan_workers()
                self._delete_completed_jobs()

            # finally, if there's still time left let's just twiddle our thumbs for a bit
            time_left = self.iteration_time - (time.time() - start_time)
            if time_left > 0:
                logger.info(f"sleeping {time_left}")
                time.sleep(time_left)
