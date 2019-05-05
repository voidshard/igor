import copy
import threading
import time
import os
import random

from queue import Queue
from redis import Redis

from igor import domain
from igor import enums
from igor import exceptions as exc
from igor import utils
from igor.runner.base import Base
from igor.runner import default as runner


logger = utils.logger()


class DefaultRunner(Base):

    _DEFAULT_ENV_VARS = ["PYTHONPATH", "PATH", "GOPATH"]

    _QUEUE_MAIN = "main"

    _CHAN_WORKERS = "workers"  # channel all workers subscribe to

    def __init__(
        self, svc, host: str="redis", port: int=6379, ping_time=30, max_unexpected_errors=10
    ):
        self._worker = None
        self._svc = svc
        self._default_env = None
        self._max_unexpected_errors = max_unexpected_errors

        # connections to redis for queuing / messaging
        self._conn = Redis(decode_responses=True, host=host, port=port)
        self._queue = runner.TaskQueue(host=host, port=port)

        # ping thread: updates system with information every so often
        self._ping_thread = None
        self._do_ping = True
        self._ping_time = ping_time

        # pubsub thread: listens for commands from system
        self._pubsub_thread = None
        self._do_pubsub = True
        self._pubsub = self._conn.pubsub()

        # internals to control execution / pass information between threads
        self._run_worker = True
        self._process_manager = None
        self._worker_queue = Queue()

    def queued_tasks(self, name=None) -> int:
        """Return the current number of queued tasks.

        :param name: queue name
        :return: int

        """
        return self._queue.count(self._QUEUE_MAIN)

    def _thread_ping_system(self, worker_id, ping_fn, ping_time):
        """Update our worker in the db every so often with some stats.

        :param worker_id: id of this worker
        :param ping_fn: function to call when pinging
        :param ping_time: sleep time between pings

        """
        while self._do_ping:
            # collect stats that we'll send with our ping
            stats = {"tasks": self._process_manager.status()}

            logger.info(f"ping: worker_id:{worker_id} stats:{stats}")

            # do the ping
            try:
                ping_fn(worker_id, stats)
            except exc.WorkerNotFound as e:
                # catch case where deregister (delete worker) gets run before the thread exits
                if self._do_ping:
                    raise e
            except Exception as e:
                logger.warn(f"failed to ping service: {e}")

            # sleep for a bit
            time.sleep(ping_time)

    def queue_tasks(self, layer, tasks) -> int:
        """Queue a list of tasks.

        :param layer: parent layer
        :param tasks: tasks to queue
        :returns int: number of tasks actually queued

        """
        count = 0

        for t in tasks:
            self._queue.add(
                self._QUEUE_MAIN,
                self._make_queue_key(t.job_id, t.layer_id, t.id),
                layer.priority
            )
            count += 1

        logger.info(f"tasks queued: layer_id:{layer.id} tasks:{count}")

        if count:
            # tell workers that new work has been put in the queue
            self._conn.publish(
                self._CHAN_WORKERS,
                runner.encode_message(
                    runner.MSG_ANNOUNCE,
                    event=runner.EVENT_WORK_QUEUED,
                )
            )

        return count

    def _do_task(self, job_id, layer_id, task_id) -> bool:
        """Do the grunt work of actually running the given task.

        Intended to block waiting for task to quit (one way or another).

        :param job_id:
        :param layer_id:
        :param task_id:
        :returns bool: value indicating if we should continue running

        """
        logger.info(f"running task: job_id:{job_id} layer_id:{layer_id} task_id:{task_id}")
        try:
            tsk = self._svc.one_task(task_id)
        except exc.TaskNotFound as e:
            logger.warn(f"dropping task: task:{task_id} error:{e}")
            return True

        if tsk.state != enums.State.QUEUED.value:
            # Task is in invalid state
            logger.warn(f"dropping task:{task_id} state:{tsk.state}")
            return True

        env = copy.copy(self._default_env)

        if tsk.env:
            env.update(tsk.env)

        env.update({
            "IGOR_JOB_ID": job_id,
            "IGOR_LAYER_ID": layer_id,
            "IGOR_TASK_ID": task_id,
            "IGOR_WORKER_ID": self._worker.id,
            "IGOR_WORKER_KEY": self._worker.key,
        })

        try:
            # tell the system we're starting this task
            self._svc.start_work_task(self._worker.id, tsk.id)
            tsk.attempts += 1

            # log what we're doing
            print("-" * 10 + "loading" + "-" * 10)
            print(f"attempt={tsk.attempts}")
            print(f"command={tsk.cmd}")
            print("environment:")
            for k, v in env.items():
                print(f"{k}={v}")
            print("-" * 10 + "starting" + "-" * 10)

            # run the cmd
            exit_code, pm_code, err_message = self._process_manager.run(
                [job_id, layer_id, task_id],
                tsk.cmd,
                env=env
            )

            # log the result
            print("-" * 10 + "process finished" + "-" * 10)
            print(f"exit_code={exit_code}")
            print(f"message={err_message}")

            # There are 3 outcomes
            #  The task completes successfully
            #  The task is killed (by another thread in the daemon)
            #  The task fails

            if pm_code == self._process_manager.CODE_KILLED:
                # since it was ordered killed, the task is already updated (by the system),
                # we can just update our worker to reflect that we stopped work on the task
                wkr = self._svc.one_worker(self._worker.id)
                if not wkr:
                    return False  # we've been removed ??

                self._svc.update_worker(
                    self._worker.id,
                    None,
                    task_finished=time.time(),
                    job_id=None,
                    layer_id=None,
                    task_id=None,
                )
                return True

            next_state = enums.State.COMPLETED.value
            if pm_code == self._process_manager.CODE_ERROR:
                next_state = enums.State.ERRORED.value

            self._svc.stop_work_task(
                self._worker.id,
                task_id,
                next_state,
                reason=f"exit_code: {exit_code}  message: {err_message}",
                attempts=tsk.attempts,
            )
            logger.info(
                f"task complete: task_id:{task_id} exit_code:{exit_code} state:{next_state}"
            )
        except Exception as e:
            logger.info(
                f"task failed: task_id:{task_id} error:{e}"
            )
            # ensure the process is killed
            self._process_manager.kill(task_id)

            # decide whether to requeue the task or not ..
            self._svc.stop_work_task(
                self._worker.id,
                task_id,
                enums.State.ERRORED.value,
                reason=str(e),
                attempts=tsk.attempts,
            )

        return True

    def _stop(self):
        """Stop all of the daemons.

        """
        self._run_worker = False
        self._do_ping = False
        self._do_pubsub = False
        self._worker_queue.put(False)

    @staticmethod
    def _make_queue_key(job_id, layer_id, task_id, attempt=0):
        """Create a key string holding the given data.

        :param job_id:
        :param layer_id:
        :param task_id:
        :param attempt:
        :return: str

        """
        return ":".join([job_id, layer_id, task_id, str(attempt)])

    @staticmethod
    def _read_queue_key(value):
        """Read data back from key string (see _make_queue_key)

        Returns job_id, layer_id, task_id, times_queued

        :param value:
        :return: str, str, str, int

        """
        if value.count(":") != 3:
            logger.warning(f"discarding invalid work request: {value}")
            return None, None, None, None

        job_id, layer_id, task_id, times_queued = value.split(":", 3)

        try:
            times_queued = int(times_queued)
        except ValueError:
            times_queued = 0

        return job_id, layer_id, task_id, times_queued

    def _start(self):
        """Run the main worker. This should continue to pull tasks from the queue
        and do them, so long as it's unpaused.

        """
        accept_work = True

        while self._run_worker:
            logger.info(f"looking for work: accept_work:{accept_work}")

            while not self._worker_queue.empty():  # update our value with the latest queued value
                accept_work = self._worker_queue.get()

            if not accept_work:  # oh .. last value was false, so block & wait for the green light
                accept_work = self._worker_queue.get()
                continue

            value, job_id, layer_id, task_id = "", "", "", ""
            try:
                value = self._queue.pop(self._QUEUE_MAIN)

                if not value:
                    time.sleep(random.randint(1, 5))  # no work? sleep for a few seconds
                    continue

                logger.info(f"work received: value:{value}")

                job_id, layer_id, task_id, times_queued = self._read_queue_key(value)

                accept_work = self._do_task(job_id, layer_id, task_id)
            except Exception as e:
                logger.error(
                    f"unexpected exception during task processing: "
                    f"value:{value} error:{e} attempts: {times_queued}"
                )

                if times_queued >= self._max_unexpected_errors:
                    logger.warning(f"discarding task from queue, too many requeue errors: {value}")

                    if task_id:
                        try:
                            self._svc.force_task_state(task_id, times_queued, str(e))
                        except Exception as e:
                            logger.warning(f"failed to mark task errored: {e} {task_id}")

                    continue

                logger.info(f"attempting requeue of task: {value}")

                if all([job_id, layer_id, task_id]):
                    self._queue.add(
                        self._QUEUE_MAIN,
                        self._make_queue_key(job_id, layer_id, task_id, attempt=times_queued + 1),
                        0
                    )
                elif value:
                    self._queue.add(self._QUEUE_MAIN, value, 1)

    def _thread_subscribe(self):
        """Thread to listen for & handle incoming messages.

        """
        self._pubsub.subscribe(self._worker_chan(self._worker.id), self._CHAN_WORKERS)

        # allows us to ignore duplicate message delivered within some time frame
        buffer = runner.MessageBuffer()

        ordered_to_pause = False

        for received in self._pubsub.listen():
            try:
                msg = buffer.decode_message(received)
            except (runner.DuplicateMessage, runner.MalformedMessage, runner.UnknownMessage) as e:
                logger.warn(f"message dropped: message:{received} error:{e}")
                continue
            except Exception as e:
                logger.warn(f"unexpected exception: message:{received} error:{e}")
                raise e

            if msg.type == runner.MSG_KILL:
                # we've been ordered to kill matching jobs / layers / tasks
                try:
                    self._perform_kill(**msg.data)
                except Exception as e:
                    logger.error(f"unable to perform kill: message:{received} error:{e}")

            elif msg.type == runner.MSG_ANNOUNCE:
                # a general announcement event has arrived
                event = msg.data.get("event")

                if event == runner.EVENT_WORK_QUEUED and not ordered_to_pause:
                    # new work has been published to be done. So long as we haven't been
                    # told NOT to work, we'll tell the main thread to hop to it.
                    self._worker_queue.put(True)

            elif msg.type == runner.MSG_PAUSE:
                # we've been ordered to stop accepting new tasks until notified.
                ordered_to_pause = True
                self._worker_queue.put(False)

            elif msg.type == runner.MSG_UNPAUSE:
                # we've been ordered to accept new tasks until notified (this is the default).
                ordered_to_pause = False
                self._worker_queue.put(True)

            if not self._do_pubsub:
                break  # we've been ordered to exit

    def _perform_kill(self, job_id=None, layer_id=None, task_id=None):
        """Kill a running process by the job, layer or task it represents.

        :param job_id:
        :param layer_id:
        :param task_id:

        """
        if not any([job_id, layer_id, task_id]):
            return

        logger.info(f"killing: job_id:{job_id} layer_id:{layer_id} task_id:{task_id}")

        for i in [task_id, layer_id, job_id]:
            if i:
                self._process_manager.kill(i)

    @staticmethod
    def _worker_chan(worker_id: None) -> str:
        """The channel one should use to reach a single worker.

        :param worker_id:
        :return: str

        """
        return f"worker:{worker_id}"

    def send_kill(self, worker_id=None, job_id=None, layer_id=None, task_id=None):
        """Order tasks of the given job / layer / task killed

        :param worker_id: send only to the given worker.
        :param job_id:
        :param layer_id:
        :param task_id:
        :raises ValueError: If no kwargs given.

        """
        if not any([job_id, layer_id, task_id]):
            raise ValueError("one of job_id, layer_id, task_id must be given")

        logger.info(f"sending kill: job_id:{job_id} layer_id:{layer_id} task_id:{task_id}")
        chan = self._CHAN_WORKERS

        if worker_id:
            chan = self._worker_chan(worker_id)

        msg = runner.encode_message(
            runner.MSG_KILL,
            job_id=job_id,
            layer_id=layer_id,
            task_id=task_id,
        )

        self._conn.publish(chan, msg)

    def send_worker_pause(self, worker_id):
        """Order the given worker to stop accepting new tasks.

        This does *NOT* stop the currently running task(s), it only tells the worker to stop
        accepting MORE tasks.

        :param worker_id:

        """
        logger.info(f"sending pause: worker_id:{worker_id}")
        chan = self._worker_chan(worker_id)

        msg = runner.encode_message(
            runner.MSG_PAUSE,
        )

        self._conn.publish(chan, msg)

    def send_worker_unpause(self, worker_id):
        """Order the given worker to start accepting new tasks.

        Ie this is the inverse of "worker_pause()" - a worker must be sent this before it will
        begin processing new tasks after being set to drain.

        :param worker_id:

        """
        logger.info(f"sending unpause: worker_id:{worker_id}")
        chan = self._worker_chan(worker_id)

        msg = runner.encode_message(
            runner.MSG_UNPAUSE,
        )

        self._conn.publish(chan, msg)

    def stop(self):
        """Order daemon & worker threads to exit.

        """
        # let worker threads exit loop
        self._stop()

        # unsubscribe from everything
        self._pubsub.unsubscribe()

        # tell the system we're going away
        try:
            self._svc.delete_worker(self._worker.id)
        except exc.WorkerNotFound:
            pass

    def start_daemon(self, *args, **kwargs):
        """Kick of TaskTiger worker.

        Nb. this call is intended to block forever.

        :param args:
        :param kwargs:

        """
        self._default_env = {k: os.environ.get(k) for k in self._DEFAULT_ENV_VARS}

        self._process_manager = runner.ProcessManager()

        self._worker = domain.Worker()
        self._worker.key = utils.random_name_worker(suffix=self._worker.host)

        self._pubsub_thread = threading.Thread(
            target=self._thread_subscribe,
        )
        self._pubsub_thread.setDaemon(True)

        self._ping_thread = threading.Thread(
            target=self._thread_ping_system,
            args=(self._worker.id, self._svc.worker_ping, self._ping_time)
        )
        self._ping_thread.setDaemon(True)

        try:
            # tell the system we're alive & about ready
            self._svc.create_worker(self._worker)

            # periodically being pinging the system to tell them we're (still) alive
            self._ping_thread.start()

            # listen to the system for information
            self._pubsub_thread.start()

            # call into task tiger to kick us off
            self._start()
        finally:
            self.stop()
