"""Requests / responses are sent over HTTP.

"""
import json

from flask import Flask, request, redirect
from functools import wraps

from igor import domain
from igor import exceptions as exc
from igor import utils
from igor.api.transport.base import Base
from igor.api.gateway import IgorGateway
from igor.api.domain import spec
from igor.api import exceptions as api_exc


logger = utils.logger()

_action_pause = "pause"
_action_retry = "retry"


def request_tag():
    """Fetch Etag header from request

    :return: str

    """
    return request.headers.get("Etag", None, str)


def requires_auth(f):
    """Sugar to enforce http basic auth

    :param f:

    """
    @wraps(f)
    def decorated(*args, **kwargs):
        self = args[0]
        auth = request.authorization
        if not auth:
            raise api_exc.Unauthorized("authentication required")

        usr = self._gate.authenticate_user(auth.username, auth.password)

        kwargs["user"] = usr  # stomps "user" kwarg .. so route functions can't use 'user'

        return f(*args, **kwargs)
    return decorated


def error_wrap(f):
    """Sugar to handle known errors and return the correct code(s)

    :param f:

    """
    @wraps(f)
    def decorated(*args, **kwargs):
        self = args[0]

        try:
            return f(*args, **kwargs)
        except (exc.IllegalOp, api_exc.InvalidSpec, exc.InvalidArg, exc.InvalidState) as e:
            return self._err(e), 400  # you sent something unexpected
        except (api_exc.Unauthorized, api_exc.Forbidden) as e:
            return self._err(e), 401  # .. who are you again?
        except exc.NotFound as e:
            return self._err(e), 404  # nope can't find that
        except (exc.WriteConflictError, exc.WorkerMismatch, api_exc.WriteConflict) as e:
            return self._err(e), 409  # sorry someone beat you to it
        except Exception as e:
            return self._err(e), 500  # ???

    return decorated


class HttpTransport(Base):
    """The role here is simple: We're to serve the functions on the main Gateway class.

    We must ensure that:
     - we only pass down authenticated user objects (the Gateway does authorization)
     - we decode data coming over HTTP to API 'spec' objects from JSON
     - we encode data coming from the Gateway to JSON

    We also want some sugar around returning the correct HTTP error codes for when
    things go wrong & enforcing HTTPS.

    """

    _V = "/v1"

    def __init__(self, port=8080, ssl_cert=None, ssl_key=None):
        self._app = Flask(__name__)
        self._gate = None
        self._port = port

        self._ssl_context = "adhoc"  # tell flask to make it's own ssl certs
        if ssl_cert and ssl_key:
            self._ssl_context = (ssl_cert, ssl_key)  # supply certs

    def stop(self):
        """Kill whatever the listener process is.

        """
        # http://flask.pocoo.org/snippets/67/
        try:
            func = request.environ.get('werkzeug.server.shutdown')
            if func:
                func()
        except Exception:
            pass

    @staticmethod
    def _err(reason) -> dict:
        """Return json error message

        :param reason:
        :return: dict

        """
        return json.dumps({"error": str(reason)})

    @staticmethod
    def _id(id_, tag) -> dict:
        """Return generic reply from a create call.

        :param id_:
        :param tag:
        :return: dict

        """
        return json.dumps({"id": id_, "etag": tag})

    @error_wrap
    @requires_auth
    def handle_jobs(self, user: domain.User=None):
        """

        if method is GET we expect a QuerySpec
        if method is POST we expect a JobSpec

        :return:

        """
        try:
            data = json.loads(request.data or "{}")
        except Exception as e:
            return self._err(e), 400

        method = request.method
        if method == "POST":
            result, tag = self._gate.create_job(user, spec.JobSpec.decode(data))
            return self._id(result, tag), 203
        else:
            result = self._gate.get_jobs(user, spec.QuerySpec.decode(data or {}))
            return json.dumps([r.encode() for r in result]), 200

    @error_wrap
    @requires_auth
    def handle_layers(self, user: domain.User=None):
        """

        :return:

        """
        try:
            data = json.loads(request.data or "{}")
        except Exception as e:
            return self._err(e), 400

        result = self._gate.get_layers(user, spec.QuerySpec.decode(data or {}))
        return json.dumps([r.encode() for r in result]), 200

    @error_wrap
    @requires_auth
    def handle_tasks(self, user: domain.User=None):
        """

        :return:

        """
        try:
            data = json.loads(request.data or "{}")
        except Exception as e:
            return self._err(e), 400

        result = self._gate.get_tasks(user, spec.QuerySpec.decode(data or {}))
        return json.dumps([r.encode() for r in result]), 200

    @error_wrap
    @requires_auth
    def handle_workers(self, user: domain.User=None):
        try:
            data = json.loads(request.data or "{}")
        except Exception as e:
            return self._err(e), 400

        result = self._gate.get_workers(user, spec.QuerySpec.decode(data or {}))
        return json.dumps([r.encode() for r in result]), 200

    @error_wrap
    @requires_auth
    def handle_job(self, id_: str, user: domain.User=None):
        """

        :return:

        """
        if request.method == "DELETE":
            self._gate.perform_kill(user, request_tag(), job_id=id_)
            return "{}", 200

        elif request.method == "POST":
            try:
                data = json.loads(request.data or "{}")
            except Exception as e:
                return self._err(e), 400

            action = data.get("action")

            if action == _action_pause:
                etag = self._gate.perform_pause(user, request_tag(), job_id=id_)
                return self._id(id_, etag), 200
            else:
                return self._err(f"unknown action {action}"), 400

        return json.dumps(self._gate.one_job(user, id_).encode()), 200

    @error_wrap
    @requires_auth
    def handle_layer(self, id_: str, user: domain.User=None):
        """

        :return:

        """
        if request.method == "DELETE":
            self._gate.perform_kill(user, request_tag(), layer_id=id_)
            return "{}", 200

        elif request.method == "POST":
            try:
                data = json.loads(request.data or "{}")
            except Exception as e:
                return self._err(e), 400

            action = data.get("action")

            if action == _action_pause:
                etag = self._gate.perform_pause(user, request_tag(), layer_id=id_)
                return self._id(id_, etag), 200
            else:
                return self._err(f"unknown action {action}"), 400

        return json.dumps(self._gate.one_layer(user, id_).encode()), 200

    @error_wrap
    @requires_auth
    def handle_task_result(self, id_: str, user: domain.User=None):
        """

        :return:

        """
        etag = self._gate.set_task_result(user, request_tag(), id_, request.data)
        return self._id(id_, etag), 200

    @error_wrap
    @requires_auth
    def handle_task_env(self, id_: str, user: domain.User=None):
        """

        :return:

        """
        try:
            data = json.loads(request.data or "{}")
        except Exception as e:
            return self._err(e), 400

        etag = self._gate.set_task_env(user, request_tag(), id_, data)
        return self._id(id_, etag), 200

    @error_wrap
    @requires_auth
    def handle_task(self, id_: str, user: domain.User=None):
        """

        :return:

        """
        if request.method == "DELETE":
            self._gate.perform_kill(user, request_tag(), task_id=id_)
            return "{}", 200

        elif request.method == "POST":
            try:
                data = request.json or {}
            except Exception as e:
                return self._err(e), 400

            action = data.get("action")

            if action == _action_pause:
                etag = self._gate.perform_pause(user, request_tag(), task_id=id_)
                return self._id(id_, etag), 200
            elif action == _action_retry:
                etag = self._gate.retry_task(user, request_tag(), id_)
                return self._id(id_, etag), 200
            else:
                return self._err(f"unknown action {action}"), 400

        return json.dumps(self._gate.one_task(user, id_).encode()), 200

    @error_wrap
    @requires_auth
    def handle_worker(self, id_: str, user: domain.User=None):
        return json.dumps(self._gate.one_worker(user, id_).encode()), 200

    @error_wrap
    @requires_auth
    def handle_create_task(self, id_: str, user: domain.User=None):
        """

        :param id_: parent layer id
        :param user:

        """
        try:
            data = json.loads(request.data or "{}")
        except Exception as e:
            return self._err(e), 400

        if isinstance(data, list):  # we can accept a task or list of tasks
            results = []

            for i in data:
                result, tag = self._gate.create_task(user, id_, spec.TaskSpec.decode(i))
                results.append({"id": result, "etag": tag})

            return json.dumps(results), 203
        else:
            result, tag = self._gate.create_task(user, id_, spec.TaskSpec.decode(data))
            return self._id(result, tag), 203

    @staticmethod
    def redirect_to_ssl():
        """Sugar to redirect incoming requests to HTTPS.

        """
        is_forwarded_from_ssl = request.headers.get('X-Forwarded-Proto', 'http') == 'https'
        if not (request.is_secure or is_forwarded_from_ssl):
            if request.url.startswith('http://'):
                return redirect(request.url.replace('http://', 'https://', 1), code=302)

    def serve(self, gate: IgorGateway):
        self._gate = gate

        # get / create jobs
        self._app.add_url_rule(
            f"{self._V}/jobs/", "handle_jobs", self.handle_jobs, methods=["GET", "POST"]
        )

        # get layers, tasks & workers
        self._app.add_url_rule(
            f"{self._V}/layers/", "handle_layers", self.handle_layers, methods=["GET"]
        )
        self._app.add_url_rule(
            f"{self._V}/tasks/", "handle_tasks", self.handle_tasks, methods=["GET"]
        )
        self._app.add_url_rule(
            f"{self._V}/workers/", "handle_workers", self.handle_workers, methods=["GET"]
        )

        # single get / kill functions
        self._app.add_url_rule(
            f"{self._V}/jobs/<id_>",
            "handle_job",
            self.handle_job,
            methods=["GET", "DELETE", "POST"]
        )
        self._app.add_url_rule(
            f"{self._V}/layers/<id_>",
            "handle_layer",
            self.handle_layer,
            methods=["GET", "DELETE", "POST"]
        )
        self._app.add_url_rule(
            f"{self._V}/tasks/<id_>",
            "handle_task",
            self.handle_task,
            methods=["GET", "DELETE", "POST"]
        )
        self._app.add_url_rule(  # set result of task
            f"{self._V}/tasks/<id_>/result",
            "handle_task_result",
            self.handle_task_result,
            methods=["POST"]
        )
        self._app.add_url_rule(  # set environment of task
            f"{self._V}/tasks/<id_>/environment",
            "handle_task_env",
            self.handle_task_env,
            methods=["POST"]
        )
        self._app.add_url_rule(
            f"{self._V}/workers/<id_>", "handle_worker", self.handle_worker, methods=["GET"]
        )

        # create task on existing layer
        self._app.add_url_rule(
            f"{self._V}/layers/<id_>/tasks",
            "handle_create_task",
            self.handle_create_task,
            methods=["POST"]
        )

        # require everything to be https
        self._app.before_request(self.redirect_to_ssl)

        logger.info(f"ssl context: {self._ssl_context}")

        self._app.run(
            host='0.0.0.0',
            port=self._port,
            threaded=True,
            ssl_context=self._ssl_context,
        )
