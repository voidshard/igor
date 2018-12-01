import time
import uuid

from igor.runner.default import queue

from integration import utils


class TestQueue:

    queue = None
    redis_container = None
    CONTAINER_START_WAIT_TIME = 1
    TEST_QUEUE = "test"

    @classmethod
    def setup_class(cls):
        # start ourselves a new container & give it some time to get ready
        client = utils.Client()
        cls.redis_container = client.redis_container()
        cls.redis_container.start()
        time.sleep(cls.CONTAINER_START_WAIT_TIME)

        # connect to the container
        cls.queue = queue.TaskQueue(host="localhost")

    @classmethod
    def teardown_class(cls):
        try:
            cls.redis_container.stop()
        except:
            pass

    @classmethod
    def clear_db(cls):
        """Drop everything on the floor.

        """
        while cls.queue.pop(cls.TEST_QUEUE):
            continue

    def test_initial_count_ok(self):
        # arrange
        self.clear_db()

        # act
        initial = self.queue.count(self.TEST_QUEUE)

        # assert
        assert initial == 0

    def test_count_ok(self):
        # arrange
        self.clear_db()
        values = [
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
            uuid.uuid4(),
        ]
        for v in values:
            self.queue.add(self.TEST_QUEUE, v, 0)

        # act
        result_a = self.queue.count(self.TEST_QUEUE)

        _ = self.queue.pop(self.TEST_QUEUE)
        _ = self.queue.pop(self.TEST_QUEUE)

        result_b = self.queue.count(self.TEST_QUEUE)

        # assert
        assert len(values) == result_a
        assert len(values) -2 == result_b

    def test_pop_respects_order(self):
        # arrange
        self.clear_db()

        values = [("a", 10), ("b", 100), ("c", 50)]
        for v, p in values:
            self.queue.add(self.TEST_QUEUE, v, p)

        # act
        results = []
        while self.queue.count(self.TEST_QUEUE):
            results.append(self.queue.pop(self.TEST_QUEUE))

        # assert
        assert results == ["b", "c", "a"]

    def test_add(self):
        # arrange
        value = "test"
        before = self.queue.count(self.TEST_QUEUE)

        # act
        self.queue.add(self.TEST_QUEUE, value, 0)
        after = self.queue.count(self.TEST_QUEUE)

        # assert
        assert after == before + 1

    def test_pop_when_empty(self):
        # arrange
        self.clear_db()

        # act
        result = self.queue.pop(self.TEST_QUEUE)

        # assert
        assert result is None

    def test_pop_when_not_empty(self):
        # arrange
        self.clear_db()

        value = "test"
        self.queue.add(self.TEST_QUEUE, value, 0)

        # act
        result = self.queue.pop(self.TEST_QUEUE)

        # assert
        assert result == value
