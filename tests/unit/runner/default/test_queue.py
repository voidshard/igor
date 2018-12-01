import uuid
import random

from igor.runner.default import queue


class TestTaskQueue:

    def test_key_returns_string(self):
        # arrange
        inputs = [
            str(uuid.uuid4()),
            uuid.uuid4(),
            12,
        ]

        # act
        for v in inputs:
            result = queue.TaskQueue._key(v)

            # assert
            assert isinstance(result,  str)


class TestHoursSinceEpoch:

    def test_returns_int(self):
        # arrange

        # act
        result = queue._hours_since_epoch()

        # assert
        assert isinstance(result,  int)


class TestUpperLimit:

    def test_returns_int(self):
        # arrange

        # act
        result = queue._upper_limit()

        # assert
        assert isinstance(result, int)


class TestFinalPriority:

    def test_reverses(self):
        # arrange
        values_in = [
            random.randint(50, 60),  # higher number
            random.randint(10, 20),  # lower number
        ]

        # act
        result = [
            queue._final_priority(v) for v in values_in
        ]

        # assert
        assert result[0] < result[1]  # note the second result is higher

    def test_returns_int(self):
        # arrange

        # act
        result = queue._final_priority(random.randint(10, 1000))

        # assert
        assert isinstance(result, int)
