import copy
import pytest
import time
import json
import uuid

from igor.runner.default import message
from igor.runner.default import exceptions as exc


class TestMessageBuffer:

    msg_data = {
        "id": str(uuid.uuid4()),
        "time": time.time(),
        "type": message.MSG_WORK,
        "data": {
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
        }
    }

    def test_decode_raises_on_duplicate(self):
        # arrange
        size = 10
        buf = message.MessageBuffer(size=size)

        msg = buf.decode_message(self.msg_data)

        # act & assert
        with pytest.raises(exc.DuplicateMessage):
            buf.decode_message(self.msg_data)

        assert isinstance(msg, message._Message)

    def test_incr_count(self):
        """Prove that incr_count is a rotating buffer of fixed size
        """
        # arrange
        size = 10
        buf = message.MessageBuffer(size=size)
        seen = set()

        # act
        for i in range(0, size * 5):
            seen.add(buf._incr_count())

        # assert
        assert seen == set(range(0, size))


class TestMessageData:

    def test_returns_dict(self):
        # arrange
        args = {
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
        }

        # act
        result = message._message_data(message.MSG_WORK, **args)

        # assert
        assert isinstance(result, dict)

    def test_raises_on_invalid(self):
        # arrange
        type_ = "WHAT"
        args = {}

        # act & assert
        with pytest.raises(exc.UnknownMessage):
            message._message_data(type_, **args)

    def test_raises_on_invalid_message(self):
        # arrange
        type_ = message.MSG_WORK
        args = {}

        # act & assert
        with pytest.raises(exc.MalformedMessage):
            message.encode_message(type_, **args)


class TestDecodeMessage:

    msg_data = {
        "id": str(uuid.uuid4()),
        "time": time.time(),
        "type": message.MSG_WORK,
        "data": {
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
        }
    }
    msg_str = message.encode_message(message.MSG_WORK, **msg_data["data"])

    def test_raises_on_missing_args(self):
        # arrange
        msg = copy.copy(self.msg_data)
        msg["data"] = {"random": 'stuff'}

        mstg_str = json.dumps(msg)

        # act & assert
        with pytest.raises(exc.MalformedMessage):
            message.decode_message(mstg_str)

    def test_raises_on_unexpected_type(self):
        # arrange
        msg = ["foo"]

        # act & assert
        with pytest.raises(exc.MalformedMessage):
            message.decode_message(msg)

    def test_decode_message_handles_redis_dict(self):
        # arrange
        msg = {
            # something like what a redis pubsub.listen() returns
            "pattern": "",
            "channel": "SOME_CHANNEL",
            "data": copy.copy(self.msg_data),
        }

        # act
        result = message.decode_message(msg)

        # assert
        assert isinstance(result, message._Message)

    def test_decode_message_handles_dict(self):
        # arrange

        # act
        result = message.decode_message(self.msg_data)

        # assert
        assert isinstance(result, message._Message)

    def test_decode_message_handles_str(self):
        # arrange

        # act
        result = message.decode_message(self.msg_str)

        # assert
        assert isinstance(result, message._Message)


class TestEncodeMessage:

    def test_encodes_valid_message_ok(self):
        # arrange
        type_ = message.MSG_KILL
        args = {
            "job_id": str(uuid.uuid4()),
            "layer_id": str(uuid.uuid4()),
            "task_id": str(uuid.uuid4()),
        }

        # act
        result = message.encode_message(type_, **args)

        # assert
        assert isinstance(result, str)
        for key in message._MSG_REQUIRED_KEYS:
            assert key in result

    def test_raises_on_invalid_message(self):
        # arrange
        type_ = "WHAT"
        args = {}

        # act & assert
        with pytest.raises(exc.UnknownMessage):
            message.encode_message(type_, **args)

    def test_raises_on_malformed_message(self):
        # arrange
        type_ = message.MSG_WORK
        args = {}

        # act & assert
        with pytest.raises(exc.MalformedMessage):
            message.encode_message(type_, **args)
