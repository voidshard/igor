import pytest
import uuid

from igor.api.domain import spec
from igor.api import exceptions as exc
from igor import enums


class TestTaskSpec:

    @pytest.mark.parametrize('given, expect', [  # arrange
        (
            None,
            {}
        ),
        (
            {"b": 1},
            {"b": "1"}
        ),
        (
            {"b": True},
            {"b": "True"}
        ),
        (
            {None: "x", "x": None},
            {}
        ),
        (
            {"PYTHONPATH": "/a/b/x/a/w"},
            {"PYTHONPATH": "/a/b/x/a/w"},
        ),
    ])
    def to_env_dict(self, given, expect):
        # act
        result = spec.TaskSpec.to_env_dict(given)

        # assert
        assert result == expect

    def test_decode(self):
        # arrange
        given = {
            "name": "foobar",
            "cmd": ["python", "myfile.py"],
            "max_attempts": 321,
            "env": {"FOOBAR": "BLAH"},
        }

        # act
        result = spec.TaskSpec.decode(given)

        # assert
        assert isinstance(result, spec.TaskSpec)
        assert given == {
            "name": result.name,
            "cmd": result.cmd,
            "max_attempts": result.max_attempts,
            "env": result.env,
        }


class TestLayerSpec:

    def test_decode(self):
        # arrange
        given = {
            "order": 100,
            "priority": 27,
            "name": "foobar",
            "tasks": [],
        }

        # act
        result = spec.LayerSpec.decode(given)

        # assert
        assert isinstance(result, spec.LayerSpec)

        del given["tasks"]
        assert given == {
            "order": result.order,
            "priority": result.priority,
            "name": result.name,
        }


class TestJobSpec:

    def test_decode(self):
        # arrange
        given = {
            "name": "foo",
            "user_id": str(uuid.uuid4()),
            "layers": [{
                "tasks": [],
            }],
        }

        # act
        result = spec.JobSpec.decode(given)

        # assert
        assert isinstance(result, spec.JobSpec)
        assert len(given["layers"]) == len(result.layers)
        assert isinstance(result.layers[0], spec.LayerSpec)
        assert given["name"] == result.name
        assert given["user_id"] == result.user_id

    @pytest.mark.parametrize('given, expect', [  # arrange
        (
            None,
            {
                "type": enums.LoggerType.STDOUT.value,
            }
        ),
        (
            {
                "type": enums.LoggerType.STDOUT.value,
            },
            {
                "type": enums.LoggerType.STDOUT.value,
            }
        ),
        (
            {
                "type": enums.LoggerType.UDP.value,
                "host": "123.12.1.2",
                "port": "7123"
            },
            {
                "type": enums.LoggerType.UDP.value,
                "host": "123.12.1.2",
                "port": 7123
            },
        ),
    ])
    def test_to_logger_config(self, given, expect):
        # act
        result = spec.JobSpec.to_logger_config(given)

        # assert
        assert result == expect

    @pytest.mark.parametrize('given', [  # arrange
        {
            "type": "What",
        },
        {
            "type": enums.LoggerType.UDP.value,
            "host": "123.12.1.2",
            "port": "0",  # too low
        },
        {
            "type": enums.LoggerType.UDP.value,
            "host": "123.12.1.2",
            "port": "65536",  # too high
        },
        {
            "type": enums.LoggerType.UDP.value,
            "host": "123.12.1.2",  # no port
        },
        {
            "type": enums.LoggerType.UDP.value,
            "port": '8032',  # no host
        },
    ])
    def test_to_logger_config_raises_on_invalid(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec.JobSpec.to_logger_config(given)

    @pytest.mark.parametrize('given', [  # arrange
        {"user_id": None},
        {"user_id": "invalid"},
        {"user_id": str(uuid.uuid4()), "layers": []},
    ])
    def test_required_fields(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec.JobSpec.decode(given)


class TestQuerySpec:

    def test_decode(self):
        # arrange
        given = {
            "limit": 301,
            "offset": 13,
            "user_id": str(uuid.uuid4()),
            "filters": [
                {
                    "states": [enums.State.ERRORED.value],
                    "task_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
                    "keys": ["something", "nada"],
                },
                {
                    "states": [enums.State.QUEUED.value],
                    "layer_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
                    "task_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
                    "keys": ["bar", "foo"],
                }
            ],
        }

        # act
        result = spec.QuerySpec.decode(given)

        # assert
        assert isinstance(result, spec.QuerySpec)
        assert len(result.filters) == len(given.get("filters", []))

        for key, got in [
            ("limit", result.limit),
            ("offset", result.offset),
            ("user_id", result.user_id),
        ]:
            assert given.get(key) == got

    def test_min_limit(self):
        # arrange
        given = {"limit": 0}

        # act
        result = spec.QuerySpec.decode(given)

        # assert
        assert result.limit == 1

    def test_min_max_limit_and_offset(self):
        # arrange
        given = {"limit": spec.QuerySpec._MAX_LIMIT * 2, "offset": -100}

        # act
        result = spec.QuerySpec.decode(given)

        # assert
        assert result.limit == spec.QuerySpec._MAX_LIMIT
        assert result.offset == 0

    def test_defaults(self):
        # arrange
        given = {}

        # act
        result = spec.QuerySpec.decode(given)

        # assert
        assert result.limit == spec.QuerySpec._DEFAULT_LIMIT
        assert result.offset == 0

    def test_handles_empty_fields(self):
        """In particular the user_id is not enforced at this level"""
        # arrange
        given = {"user_id": None, "filters": []}

        # act
        result = spec.QuerySpec.decode(given)

        # assert
        assert result.user_id is None
        assert not result.filters


class TestFilterSpec:

    @pytest.mark.parametrize('given', [  # arrange
        # invalid states
        {"states": ["what is this"]},
        {"states": [enums.State.SKIPPED.value, "garbage"]},
        {"states": ["WISHFUL_THINKING"]},
        {"states": [{"clearly": "not a state"}]},

        # invalid ids
        {"job_ids": ["not-an-id"]},
        {"layer_ids": ["not-an-id"]},
        {"task_ids": ["not-an-id"]},
    ])
    def test_raises_on_invalid_data(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec.FilterSpec.decode(given)

    def test_decode(self):
        # arrange
        given = {
            "states": [enums.State.ERRORED.value],
            "job_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
            "layer_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
            "task_ids": [str(uuid.uuid4()), str(uuid.uuid4())],
            "keys": ["something", "else"],
        }

        # act
        result = spec.FilterSpec.decode(given)

        # assert
        assert isinstance(result, spec.FilterSpec)
        assert given == {
            "states": result.states,
            "job_ids": result.job_ids,
            "layer_ids": result.layer_ids,
            "task_ids": result.task_ids,
            "keys": result.keys,
        }


class TestUserSpec:

    @pytest.mark.parametrize('given', [  # arrange
        {"name": "foobar", "is_admin": False, "password": ""},
    ])
    def test_required_fields(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec.UserSpec.decode(given)

    def test_decode(self):
        # arrange
        given = {"name": "foobar", "is_admin": False, "password": "secret"}

        # act
        result = spec.UserSpec.decode(given)

        # assert
        assert isinstance(result, spec.UserSpec)
        assert given == {
            "name": result.name,
            "is_admin": result.is_admin,
            "password": result.password,
        }


class TestDecode:

    @pytest.mark.parametrize('given, expect', [
        (None, None),
        (True, "True"),
        (b"foobar", "foobar"),
        (1, "1"),
        (87.1, "87.1"),
        ([True], "[True]"),
        ({1, 2}, "{1, 2}"),
        ({1: 2}, "{1: 2}"),
        ("blah", "blah"),
    ])
    def test_to_name(self, given, expect):
        # arrange

        # act
        result = spec._Decode.to_name(given)

        # assert
        assert result == expect

    @pytest.mark.parametrize('given', [  # arrange
        ("1234567890"),
        (None),
        (str(uuid.uuid4())[1:]),  # too short
        (str(uuid.uuid4()) + "3"),  # too long
        ('ae840ebc1356744d36aa09400a39c36411fc3'),  # invalid
    ])
    def test_to_id_raises(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec._Decode.to_id(given)

    def test_to_id(self):
        # arrange
        valid = str(uuid.uuid4())

        # act
        result = spec._Decode.to_id(valid)

        # assert
        assert result == valid

    def test_to_list_str_removes_none(self):
        # arrange
        valid = [
            str(uuid.uuid4())[:10],
            str(uuid.uuid4())[:15],
            str(uuid.uuid4())[:20],
        ]

        # act
        result = spec._Decode.to_list_str(valid + [None])

        # assert
        assert result == valid

    def test_to_list_str(self):
        # arrange
        valid = [
            str(uuid.uuid4())[:10],
            str(uuid.uuid4())[:15],
            str(uuid.uuid4())[:20],
        ]

        # act
        result = spec._Decode.to_list_str(valid)

        # assert
        assert result == valid

    @pytest.mark.parametrize('given', [  # arrange
        (["1234567890"]),
        ({}),
        ([None]),
        ([str(uuid.uuid4())[1:]]),  # too short
        ([str(uuid.uuid4()) + "3"]),  # too long
        (['ae840ebc1356744d36aa09400a39c36411fc3']),  # invalid
    ])
    def test_to_list_ids_raises(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec._Decode.to_list_ids(given)

    def test_to_list_ids_handles_empty(self):
        # arrange
        valid = []

        # act
        result = spec._Decode.to_list_ids(valid)

        # assert
        assert result == valid

    def test_to_list_ids(self):
        # arrange
        valid = [
            str(uuid.uuid4()),
            str(uuid.uuid4()),
            str(uuid.uuid4()),
        ]

        # act
        result = spec._Decode.to_list_ids(valid)

        # assert
        assert result == valid

    @pytest.mark.parametrize('given', [  # arrange
        ("A12"),
        ([1]),
        ({1, 2}),
        ({"hi"}),
        ({"hello": "foo"}),
    ])
    def test_to_int_raises(self, given):
        # act & assert
        with pytest.raises(exc.InvalidSpec):
            spec._Decode.to_int(given)

    @pytest.mark.parametrize('given, expect', [  # arrange
        (1, True),
        (0, False),
        ("x", True),
        ("", False),
        (True, True),
        (False, False),
        (None, False),
        ([], False),
        ({"what"}, True),
    ])
    def test_to_bool(self, given, expect):
        # act
        result = spec._Decode.to_bool(given)

        # assert
        assert result == expect

    @pytest.mark.parametrize('given, expect', [  # arrange
        (True, 1),
        (False, 0),
        (1, 1),
        ("17", 17),
        (87.1, 87),
    ])
    def test_to_int(self, given, expect):
        # act
        result = spec._Decode.to_int(given)

        # assert
        assert result == expect
