import base64
import json

import requests

from urllib.parse import urlencode

from pyigor import domain
from pyigor.transport import exceptions as exc
from pyigor import utils


logger = utils.logger()


class IgorClient:
    """Super simple HTTP client for talking to an igor gateway.

    """

    _action_pause = "pause"
    _action_unpause = "unpause"
    _action_kill = "kill"
    _action_skip = "skip"
    _action_retry = "retry"

    _jobs = "jobs"
    _layers = "layers"
    _tasks = "tasks"

    def __init__(
        self,
        url="http://localhost:8100/api/v1",
        ssl_pem=None,
        ssl_verify=False,
    ):
        self._url = url.rstrip("/")
        self._pem = ssl_pem
        self._verify_ssl = ssl_verify

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
        }
    @classmethod
    def _query(
        cls,
        states=None,
        job_ids=None,
        layer_ids=None,
        task_ids=None,
        limit=500,
        offset=0
    ) -> dict:
        """Build an Igor query obj from the given params.
        """
        query = [
            f"limit={limit}",
            f"offset={offset}",
        ]

        if states:
            for state in states:
                query.append(f"statuses={state}")
        if job_ids:
            for job_id in job_ids:
                query.append(f"job_ids={job_id}")
        if layer_ids:
            for layer_id in layer_ids:
                query.append(f"layer_ids={layer_id}")
        if task_ids:
            for task_id in task_ids:
                query.append(f"task_ids={task_id}")

        return "&".join(query)

    def _get(self, endpoint, query: dict):
        """Perform a GET request.

        :param endpoint:
        :param query:
        :return: requests.Response

        """
        url = self._url + "/" + endpoint
        if query:
            url += "?" + query

        result = requests.get(
            url,
            headers=self._headers,
            verify=self._verify_ssl,
        )
        if result.status_code != 200:
            logger.info(f"    --> GET {url}")
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

    def _perform_action(self, kind, obj_id, obj_etag, action):
        """Perform some http action (PATCH), return standard UpdateReply obj.

        :param obj_id:
        :param obj_etag:

        """
        headers = self._headers
        headers["Etag"] = obj_etag

        url = self._url + "/" + action

        logger.info(f"    --> PATCH {url}")
        result = requests.patch(
            url,
            data=json.dumps([{"kind": kind, "id": obj_id, "etag": obj_etag}]),
            cert=self._pem,
            headers=headers,
            verify=self._verify_ssl,
        )
        logger.info(f"{result.status_code} <-- POST {url}")

        self._check_status(result)
        return result.json()

    def retry_layer(self, id_: str, tag: str):
        """Retry the given layer by ID.

        - Retry skipped, errored or completed tasks of this layer.

        :param id_:
        :param tag:

        """
        return self._perform_action("Layer", id_, tag, self._action_retry)

    def retry_task(self, id_: str, tag: str):
        """Retry the given task by ID.

        Must be skipped, errored or completed

        :param id_:
        :param tag:

        """
        return self._perform_action("Task", id_, tag, self._action_retry)

    def pause_layer(self, id_: str, tag: str):
        """Pause the given layer by ID.

        This doesn't stop already running tasks, it only stops Igor from (re)scheduling tasks
        belonging to this layer.

        :param id_:
        :param tag:

        """
        return self._perform_action("Layer", id_, tag, self._action_pause)

    def unpause_layer(self, id_: str, tag: str):
        """Unpause the given layer by ID.

        This doesn't start already running tasks, it only allows Igor to (re)schedule tasks
        belonging to this layer.

        :param id_:
        :param tag:

        """
        return self._perform_action("Layer", id_, tag, self._action_unpause)

    def pause_task(self, id_: str, tag: str):
        """Pause the given task by ID.

        This doesn't stop the task if it's running, it only stops Igor from (re)scheduling it.

        :param id_:
        :param tag:

        """
        return self._perform_action("Task", id_, tag, self._action_pause)

    def unpause_task(self, id_: str, tag: str):
        """Unpause the given task by ID.

        This doesn't start the task if it's not running, it only allows Igor to (re)schedule it.

        :param id_:
        :param tag:

        """
        return self._perform_action("Task", id_, tag, self._action_unpause)

    def kill_task(self, id_: str, tag: str):
        """Kill a running task.

        :param id_:
        :param tag:

        """
        return self._perform_action("Task", id_, tag, self._action_kill)

    def skip_layer(self, id_: str, tag: str):
        """Mark all tasks in this layer as 'skipped'

        Skipped tasks are regarded as finished. This state is not used by the system.

        :param id_:
        :param tag:

        """
        return self._perform_action("Layer", id_, tag, self._action_skip)

    def skip_task(self, id_: str, tag: str):
        """Mark the given task as 'skipped'

        Skipped tasks are regarded as finished. This state is not used by the system.

        :param id_:
        :param tag:

        """
        return self._perform_action("Task", id_, tag, self._action_skip)

    def get_jobs(self, states=None, job_ids=None) -> list:
        """Get jobs from the API.

        :return: []domain.Job

        """
        for o in self._yield_all(
            self._get,
            self._jobs,
            domain.Job,
            states=states,
            job_ids=job_ids,
        ):
            yield o

    def get_layers(self, job_ids=None, layer_ids=None, states=None) -> list:
        """Get layers from the API

        :return: []domain.Layer

        """
        for o in self._yield_all(
            self._get,
            self._layers,
            domain.Layer,
            states=states,
            job_ids=job_ids,
            layer_ids=layer_ids,
        ):
            yield o

    def get_tasks(
        self, job_ids=None, layer_ids=None, task_ids=None, states=None
    ) -> list:
        """Get tasks from the API

        :return: []domain.Task

        """
        for o in self._yield_all(
            self._get,
            self._tasks,
            domain.Task,
            states=states,
            job_ids=job_ids,
            layer_ids=layer_ids,
            task_ids=task_ids,
        ):
            yield o
