import base64
import json

import requests

from pyigor import domain
from pyigor.transport import exceptions as exc
from pyigor import utils


logger = utils.logger()


class IgorClient:
    """Super simple HTTP client for talking to an igor gateway.

    """

    _action_pause = "pause"
    _action_kill = "kill"
    _action_skip = "skip"
    _action_retry = "retry"

    _jobs = "jobs/"
    _layers = "layers/"
    _tasks = "tasks/"
    _workers = "workers/"

    def __init__(
        self,
        url="https://localhost:9025/v1/",
        username=None,
        password=None,
        ssl_verify=False,
        ssl_pem=None,
    ):
        self._url = url
        self._token = str(base64.b64encode(
            bytes(f"{username}:{password}", encoding="utf8")
        ), encoding="utf8")
        self._pem = ssl_pem
        self._verify_ssl = self._pem if ssl_verify else False

    def create_job(self, job_spec):
        """Create a job given a spec.

        :param job_spec: domain.JobSpec
        :return: domain.UpdateReply

        """
        data = job_spec.encode()
        url = self._url + self._jobs

        logger.info(f"    --> POST {url}")
        result = requests.post(
            url=url,
            cert=self._pem,
            json=data,
            headers=self._headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")

        self._check_status(result)

        res = result.json()
        id_ = list(res.keys())[0]
        return domain.UpdateReply(id_, res[id_])

    def create_tasks(self, layer_id: str, tasks: list):
        """Create a task given a spec, on a layer that exists already.

        :param layer_id:
        :param tasks:
        :return: list

        """
        url = self._url + self._layers + layer_id + "/tasks"

        logger.info(f"    --> POST {url}")
        result = requests.post(
            url=url,
            cert=self._pem,
            json=[t.encode() for t in tasks],
            headers=self._headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")

        self._check_status(result)

        return {
            id_: domain.UpdateReply(id_, etag) for id_, etag in result.json().items()
        }

    @classmethod
    def _query(
        cls,
        states=None,
        job_ids=None,
        layer_ids=None,
        task_ids=None,
        worker_ids=None,
        keys=None,
        limit=500,
        offset=0
    ) -> dict:
        """Build an Igor query obj from the given params.

        :param limit:
        :param offset:
        :param states:
        :return: str

        """
        filt = {}

        if states:
            filt["states"] = states
        if job_ids:
            filt["job_ids"] = job_ids
        if layer_ids:
            filt["layer_ids"] = layer_ids
        if task_ids:
            filt["task_ids"] = task_ids
        if worker_ids:
            filt["worker_ids"] = worker_ids
        if keys:
            filt["keys"] = keys

        query = {
            "limit": limit,
            "offset": offset,
        }
        if filt:
            query["filters"] = [filt]

        return json.dumps(query)

    def _get(self, endpoint, data: dict):
        """Perform a GET request.

        :param endpoint:
        :param data:
        :return: requests.Response

        """
        url = self._url + endpoint

        logger.info(f"    --> GET {url}")
        result = requests.get(
            url,
            cert=self._pem,
            data=data,
            headers=self._headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- GET {url}")

        self._check_status(result)
        return result

    def _yield_all(self, func, endpoint: str, result_cls, **kwargs):
        """Call some endpoint & yield all results as constructed objects of the given result_cls
        class.

        :param func:
        :param endpoint:
        :param result_cls:
        :param kwargs: query filters
        :return: ?

        """
        limit = 500
        offset = 0
        found = limit + 1

        while found >= limit:
            found = 0

            query = self._query(limit=limit, offset=offset, **kwargs)
            result = func(endpoint, query)

            for data in result.json():
                found += 1
                yield result_cls(data)

    def _perform_action(self, endpoint, obj_id, obj_etag, action):
        """Perform some http action (POST), return standard UpdateReply obj.

        :param endpoint:
        :param obj_id:
        :param obj_etag:

        """
        headers = self._headers
        headers["Etag"] = obj_etag

        url = self._url + endpoint + obj_id

        logger.info(f"    --> POST {url}")
        result = requests.post(
            url,
            data=json.dumps({"action": action}),
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")

        self._check_status(result)

        return {x: domain.UpdateReply(x, y) for x, y in result.json().items()}

    def _kill(self, endpoint, obj_id, obj_etag):
        """Perform a KILL action (using the DELETE method).

        :param endpoint:
        :param obj_id:
        :param obj_etag:

        """
        headers = self._headers
        headers["Etag"] = obj_etag

        url = self._url + endpoint + obj_id

        logger.info(f"    --> DELETE {url}")
        result = requests.delete(
            self._url + endpoint + obj_id,
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- DELETE {url}")

        self._check_status(result)

        return {x: domain.UpdateReply(x, y) for x, y in result.json().items()}

    def retry_job(self, id_: str, tag: str):
        """Retry the given job by ID.

        - Retry skipped, errored or completed tasks of this job.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._jobs, id_, tag, self._action_retry)

    def retry_layer(self, id_: str, tag: str):
        """Retry the given layer by ID.

        - Retry skipped, errored or completed tasks of this layer.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._layers, id_, tag, self._action_retry)

    def retry_task(self, id_: str, tag: str):
        """Retry the given task by ID.

        Must be skipped, errored or completed

        :param id_:
        :param tag:

        """
        return self._perform_action(self._tasks, id_, tag, self._action_retry)

    def pause_job(self, id_: str, tag: str):
        """Pause the given job by ID.

        This doesn't stop already running tasks, it only stops Igor from (re)scheduling tasks
        belonging to this job.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._jobs, id_, tag, self._action_pause)

    def pause_layer(self, id_: str, tag: str):
        """Pause the given layer by ID.

        This doesn't stop already running tasks, it only stops Igor from (re)scheduling tasks
        belonging to this layer.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._layers, id_, tag, self._action_pause)

    def pause_task(self, id_: str, tag: str):
        """Pause the given task by ID.

        This doesn't stop the task if it's running, it only stops Igor from (re)scheduling it.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._tasks, id_, tag, self._action_pause)

    def set_task_result(self, id_: str, tag: str, result):
        """Set the result param of the given Task.

        The result can be anything the caller wants. Size limits *do* apply .. this isn't
        intended to store *massive* results though.

        :param id_:
        :param tag:
        :param result:
        :return: UpdateReply

        """
        headers = self._headers
        headers["Etag"] = tag
        headers["Content-Type"] = "application/octet-stream"

        url = self._url + self._tasks + id_ + "/result"

        logger.info(f"    --> POST {url}")
        result = requests.post(
            url,
            data=result,  # careful: this isn't JSON
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")

        self._check_status(result)

        res = result.json()
        id_ = list(res.keys())[0]
        return domain.UpdateReply(id_, res[id_])

    def set_task_env(self, id_: str, tag: str, env: dict):
        """Set the task Env param.

        This is a map[string]string that is set as ENVIRONMENT variables before the task is
        kicked off. Setting this has no effect on an already running Task.

        :param id_:
        :param tag:
        :param env:
        :return:

        """
        headers = self._headers
        headers["Etag"] = tag

        url = self._url + self._tasks + id_ + "/environment"

        logger.info(f"    --> POST {url}")
        result = requests.post(
            url,
            data=json.dumps(env),
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")

        self._check_status(result)

        res = result.json()
        id_ = list(res.keys())[0]
        return domain.UpdateReply(id_, res[id_])

    def kill_job(self, id_: str, tag: str):
        """Kill running tasks of this job.

        :param id_:
        :param tag:

        """
        return self._kill(self._jobs, id_, tag)

    def kill_layer(self, id_: str, tag: str):
        """Kill running tasks of this layer.

        :param id_:
        :param tag:

        """
        return self._kill(self._layers, id_, tag)

    def kill_task(self, id_: str, tag: str):
        """Kill a running task.

        :param id_:
        :param tag:

        """
        return self._kill(self._tasks, id_, tag)

    def skip_job(self, id_: str, tag: str):
        """Mark all tasks in this job as 'skipped'

        Skipped tasks are regarded as finished. This state is not used by the system.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._jobs, id_, tag, self._action_skip)

    def skip_layer(self, id_: str, tag: str):
        """Mark all tasks in this layer as 'skipped'

        Skipped tasks are regarded as finished. This state is not used by the system.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._layers, id_, tag, self._action_skip)

    def skip_task(self, id_: str, tag: str):
        """Mark the given task as 'skipped'

        Skipped tasks are regarded as finished. This state is not used by the system.

        :param id_:
        :param tag:

        """
        return self._perform_action(self._tasks, id_, tag, self._action_skip)

    def get_jobs(self, names=None, states=None, job_ids=None) -> list:
        """Get jobs from the API.

        :return: []domain.Job

        """
        for o in self._yield_all(
            self._get,
            self._jobs,
            domain.Job,
            keys=names,
            states=states,
            job_ids=job_ids,
        ):
            yield o

    def get_layers(self, names=None, job_ids=None, layer_ids=None, states=None) -> list:
        """Get layers from the API

        :return: []domain.Layer

        """
        for o in self._yield_all(
            self._get,
            self._layers,
            domain.Layer,
            keys=names,
            states=states,
            job_ids=job_ids,
            layer_ids=layer_ids,
        ):
            yield o

    def get_tasks(
        self, names=None, job_ids=None, layer_ids=None, task_ids=None, states=None
    ) -> list:
        """Get tasks from the API

        :return: []domain.Task

        """
        for o in self._yield_all(
            self._get,
            self._tasks,
            domain.Task,
            keys=names,
            states=states,
            job_ids=job_ids,
            layer_ids=layer_ids,
            task_ids=task_ids,
        ):
            yield o

    def get_workers(
        self, job_ids=None, layer_ids=None, task_ids=None, worker_ids=None, names=None
    ) -> list:
        """Get workers from the API

        :return: []domain.Worker

        """
        for o in self._yield_all(
            self._get,
            self._workers,
            domain.Worker,
            keys=names,
            worker_ids=worker_ids,
            job_ids=job_ids,
            layer_ids=layer_ids,
            task_ids=task_ids,
        ):
            yield o

    @classmethod
    def _check_status(cls, request_result):
        """Raises appropriate errors if required

        :param request_result:

        """
        status = request_result.status_code
        if status in [200, 201, 203]:
            return

        err = request_result.json().get("error", f"{status}")

        if status == 400:
            raise exc.MalformedRequestError(err)
        elif status == 401:
            raise exc.AuthError(err)
        elif status == 404:
            raise exc.NotFoundError(err)
        elif status == 409:
            raise exc.EtagMismatchError(err)
        elif status == 500:
            raise exc.ServerError(err)
        raise Exception(err)

    @property
    def _headers(self) -> dict:
        """Return standard headers required on our http calls.

        :return: dict

        """
        return {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self._token}",
        }
