import pytest
import uuid

from unittest.mock import MagicMock

from igor import domain
from igor import enums
from igor.api.gateway import IgorGateway
from igor.api.domain import spec
from igor import exceptions as exc
from igor.api import exceptions as apiexc


def _user(admin=False, name="foobar", password="hello") -> domain.User:
    """Return fake user.

    :return: domain.User

    """
    u = domain.User()
    u.is_admin = admin
    u.name = name
    u.password = password
    return u


def _gate(service=None, runner=None):
    """Return gateway with faked service / runner objects.

    :param service:
    :param runner:
    :return: IgorGateway

    """
    service = service or MagicMock()
    runner = runner or MagicMock()
    return IgorGateway(service, runner)


class TestOneWorker:

    def test_forbidden_if_not_admin(self):
        # arrange
        user = _user()
        gate = _gate()

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.one_worker(user, "nah")

    def test_ok(self):
        # arrange
        user = _user(admin=True)

        obj = domain.Worker()

        svc = MagicMock()
        svc.one_worker.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_worker(user, obj.id)

        # assert
        assert isinstance(result, domain.Worker)
        assert result.id == obj.id


class TestOneTask:

    def test_forbidden_if_not_owner_or_admin(self):
        # arrange
        user = _user()

        obj = domain.Task()
        obj.user_id = "foobar"

        svc = MagicMock()
        svc.one_task.return_value = obj
        gate = _gate(service=svc)

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.one_task(user, obj.id)

    def test_allowed_if_not_owner_for_admin(self):
        # arrange
        user = _user(admin=True)

        obj = domain.Task()
        obj.user_id = "foobar"

        svc = MagicMock()
        svc.one_task.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_task(user, obj.id)

        # assert
        assert isinstance(result, domain.Task)
        assert result.id == obj.id

    def test_allowed_if_owner(self):
        # arrange
        user = _user()

        obj = domain.Task()
        obj.user_id = user.id

        svc = MagicMock()
        svc.one_task.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_task(user, obj.id)

        # assert
        assert isinstance(result, domain.Task)
        assert result.id == obj.id


class TestOneLayer:

    def test_forbidden_if_not_owner_or_admin(self):
        # arrange
        user = _user()

        obj = domain.Layer()
        obj.user_id = "foobar"

        svc = MagicMock()
        svc.one_layer.return_value = obj
        gate = _gate(service=svc)

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.one_layer(user, obj.id)

    def test_allowed_if_not_owner_for_admin(self):
        # arrange
        user = _user(admin=True)

        obj = domain.Layer()
        obj.user_id = "foobar"

        svc = MagicMock()
        svc.one_layer.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_layer(user, obj.id)

        # assert
        assert isinstance(result, domain.Layer)
        assert result.id == obj.id

    def test_allowed_if_owner(self):
        # arrange
        user = _user()

        obj = domain.Layer()
        obj.user_id = user.id

        svc = MagicMock()
        svc.one_layer.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_layer(user, obj.id)

        # assert
        assert isinstance(result, domain.Layer)
        assert result.id == obj.id
    

class TestOneJob:

    def test_forbidden_if_not_owner_or_admin(self):
        # arrange
        user = _user()

        obj = domain.Job()
        obj.user_id = "foobar"

        svc = MagicMock()
        svc.one_job.return_value = obj
        gate = _gate(service=svc)

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.one_job(user, obj.id)

    def test_allowed_if_not_owner_for_admin(self):
        # arrange
        user = _user(admin=True)

        obj = domain.Job()
        obj.user_id = "foobar"

        svc = MagicMock()
        svc.one_job.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_job(user, obj.id)

        # assert
        assert isinstance(result, domain.Job)
        assert result.id == obj.id

    def test_allowed_if_owner(self):
        # arrange
        user = _user()

        obj = domain.Job()
        obj.user_id = user.id

        svc = MagicMock()
        svc.one_job.return_value = obj
        gate = _gate(service=svc)

        # act
        result = gate.one_job(user, obj.id)

        # assert
        assert isinstance(result, domain.Job)
        assert result.id == obj.id


class TestCreateUser:

    def test_forbidden_if_not_admin(self):
        # arrange
        user = _user()
        gate = _gate()

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.create_user(user, None)


class TestCreateJob:

    def test_forbidden_launch_jobs_as_another_if_not_admin(self):
        # arrange
        user = _user()
        gate = _gate()

        jspec = spec.JobSpec()
        jspec.user_id = "wrong"

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.create_job(user, jspec)

    def test_allowed_launch_jobs_as_another_if_admin(self):
        # arrange
        user = _user(admin=True)
        gate = _gate()

        jspec = spec.JobSpec()
        jspec.user_id = "wrong"

        # act
        gate.create_job(user, jspec)

        # assert
        assert True  # did not raise

    def test_calls_create_funcs(self):
        # arrange
        user = _user()

        svc = MagicMock()
        gate = _gate(service=svc)

        mock = MagicMock()
        mock.return_value = ["layera"], {"layera": ["taska"]}
        gate._build_layers = mock

        jspec = spec.JobSpec()
        jspec.user_id = user.id

        # act
        gate.create_job(user, jspec)

        # assert
        assert svc.create_job.called


class TestBuildLayer:

    def test_ok(self):
        # arrange
        gate = _gate()
        job_id = "somejob"
        user_id = "someuser"

        layer_spec = spec.LayerSpec()
        layer_spec.name = "foobar"
        layer_spec.order = 123
        layer_spec.priority = 45

        # act
        layer, tasks = gate._build_layer(job_id, user_id, layer_spec)

        # assert
        assert isinstance(layer, domain.Layer)
        assert isinstance(tasks, list)
        assert layer.id
        assert layer.key == layer_spec.name
        assert layer.order == layer_spec.order
        assert layer.priority == layer_spec.priority


class TestBuildLayers:

    @staticmethod
    def _layer_spec(order, name):
        """Return fake layer spec

        :param order:
        :param name:
        :return: spec.LayerSpec

        """
        s = spec.LayerSpec()
        s.name = name
        s.order = order
        return s

    def test_correctly_determines_relationships(self):
        # arrange
        expect_parents = {
            "A": [],
            "B": [],
            "C": [],
            "D": ["A", "B", "C"],
            "E": ["A", "B", "C"],
            "F": ["D", "E"],
        }
        expect_siblings = {
            "A": ["B", "C"],
            "B": ["A", "C"],
            "C": ["A", "B"],
            "D": ["E"],
            "E": ["D"],
            "F": [],
        }
        expect_children = {
            "A": ["D", "E"],
            "B": ["D", "E"],
            "C": ["D", "E"],
            "D": ["F"],
            "E": ["F"],
            "F": [],
        }

        job_id = "some_id"
        job_spec = spec.JobSpec()
        job_spec.layers = [
            self._layer_spec(0, "A"),
            self._layer_spec(0, "B"),
            self._layer_spec(0, "C"),

            self._layer_spec(10, "D"),
            self._layer_spec(10, "E"),

            self._layer_spec(15, "F"),
        ]

        gate = _gate()

        # act
        layers, tasks = gate._build_layers(job_id, job_spec)

        id_to_name_map = {l.id: l.key for l in layers}

        # assert
        assert isinstance(layers, list)
        assert isinstance(tasks, list)

        for layer in layers:
            assert isinstance(layer, domain.Layer)

            parents = [id_to_name_map[i] for i in layer.parents]
            siblings = [id_to_name_map[i] for i in layer.siblings]
            children = [id_to_name_map[i] for i in layer.children]

            assert expect_parents[layer.key] == parents
            assert expect_siblings[layer.key] == siblings
            assert expect_children[layer.key] == children


class TestCreateTask:

    @pytest.mark.parametrize('state', [
        enums.State.QUEUED.value,
        enums.State.RUNNING.value,
        enums.State.COMPLETED.value,
        enums.State.ERRORED.value,
        enums.State.SKIPPED.value,
    ])
    def test_raises_if_not_pending(self, state):
        # arrange
        user = _user()

        obj = domain.Layer()
        obj.user_id = user.id
        obj.state = state

        svc = MagicMock()
        svc.one_layer.return_value = obj

        rnr = MagicMock()

        gate = _gate(service=svc, runner=rnr)

        # act & assert
        with pytest.raises(apiexc.InvalidState):
            gate.create_task(user, obj.id, None)

    def test_raises_if_not_owner_or_admin(self):
        # arrange
        user = _user()

        obj = domain.Layer()
        obj.user_id = "someotherid"

        svc = MagicMock()
        svc.one_layer.return_value = obj

        rnr = MagicMock()

        gate = _gate(service=svc, runner=rnr)

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.create_task(user, obj.id, None)

    def test_calls_service_create_tasks(self):
        # arrange
        user = _user()

        obj = domain.Layer()
        obj.user_id = user.id

        tspec = spec.TaskSpec()

        svc = MagicMock()
        svc.one_layer.return_value = obj

        rnr = MagicMock()

        task = domain.Task()
        gate = _gate(service=svc, runner=rnr)
        gate._build_task = MagicMock()
        gate._build_task.return_value = task

        # act
        result_id, result_tag = gate.create_task(user, obj.id, tspec)

        # assert
        assert task.id == result_id
        assert task.etag == result_tag
        assert svc.create_tasks.called_with(obj.id, [task])


class TestBuildTask:

    def test_ok(self):
        """Test relevant data is copied over"""

        # arrange
        layer = domain.Layer()
        layer.job_id = str(uuid.uuid4())
        layer.user_id = str(uuid.uuid4())

        tspec = spec.TaskSpec()
        tspec.name = "my task"
        tspec.cmd = ["python", "foobar.py"]
        tspec.env = {"foo": "bar"}

        gate = _gate()

        # act
        result = gate._build_task(layer, tspec)

        # assert
        assert isinstance(result, domain.Task)
        assert result.id
        assert result.etag
        assert result.job_id == layer.job_id
        assert result.layer_id == layer.id
        assert result.user_id == layer.user_id
        assert result.cmd == tspec.cmd
        assert result.env == tspec.env
        assert result.key == tspec.name


class TestGetWorkers:

    def test_raises_if_non_admin(self):
        # arrange
        user = _user()
        query = spec.QuerySpec()
        query.user_id = user.id

        gate = _gate()

        # act & assert
        with pytest.raises(apiexc.Forbidden):
            gate.get_workers(user, query)

    def test_allows_query_if_admin(self):
        # arrange
        user = _user(admin=True)
        query = spec.QuerySpec()
        query.user_id = user.id

        gate = _gate()

        # act
        gate.get_workers(user, query)

        # assert
        assert True  # ie get_workers should not raise


class TestIgorGatewayAuthenticateUser:

    def test_valid(self):
        # arrange
        expect = _user(name="some_name", password="hello")
        mock = MagicMock()
        mock.one_user_by_name.return_value = expect
        gate = _gate(service=mock)

        # act
        result = gate.authenticate_user("some_name", "hello")

        # assert
        assert isinstance(result, domain.User)
        assert result.id == expect.id

    def test_password_mismatch(self):
        # arrange
        mock = MagicMock()
        mock.one_user_by_name.return_value = _user(password="no way")
        gate = _gate(service=mock)

        # act & assert
        with pytest.raises(exc.UserNotFound):
            gate.authenticate_user("some_id", "what")

    def test_not_found(self):
        # arrange
        mock = MagicMock()
        mock.one_user_by_name.return_value = None
        mock.one_user_by_name.side_effect = exc.UserNotFound("foo?")
        gate = _gate(service=mock)

        # act & assert
        with pytest.raises(exc.UserNotFound):
            gate.authenticate_user("some_id", "what")


class TestIgorGatewayStartMeta:

    def test_returns_dict(self):
        # arrange
        gate = _gate()
        user = _user()

        # act
        result = gate._start_meta(user)

        # assert
        assert isinstance(result, dict)















