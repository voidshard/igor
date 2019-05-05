import abc
import time

from igor import utils


logger = utils.logger()


class Base(metaclass=abc.ABCMeta):

    _DEFAULT_ITERATION_TIME = 10  # time between main loop iterations in seconds

    @abc.abstractmethod
    def orphan_workers(self):
        """Workers that stop replying to us need to be cast out and their tasks reclaimed for
        re-scheduling.

        """
        pass

    @abc.abstractmethod
    def archive_completed_jobs(self):
        """Jobs that complete should have their data archived or deleted in order to keep our
        working set as small as possible.

        """
        pass

    @abc.abstractmethod
    def queue_work(self):
        """The task runner system need to be feed more work as the tasks in given layers become
        ready for execution.

        """
        pass

    @abc.abstractmethod
    def check_running_layers(self):
        """Layers that are currently running need to be watch to check if/when they complete
        (or error). Their child layers then need to be queued up for task scheduling.

        """
        pass

    @property
    def iteration_time(self) -> int:
        """Approximately how long we should take between our main server loops.

        :return: int

        """
        return self._DEFAULT_ITERATION_TIME

    @property
    def should_run(self) -> bool:
        """Return if the main loop should run (or not).

        :return: bool

        """
        return True

    def run(self):
        """Simple run function that performs all of the things required, if not in the
        most efficient manner.

        """
        # begin by cleaning up missing workers & archiving completed jobs.
        self.orphan_workers()
        self.archive_completed_jobs()

        while self.should_run:
            start_time = time.time()
            logger.info(f"running scheduler {start_time}")

            # these things need to get done every iteration
            self.queue_work()
            self.check_running_layers()

            # things things we'll do if we've got time (if under pressure, queue now, clean later)
            time_left = self.iteration_time - (time.time() - start_time)
            if time_left > 0:
                self.orphan_workers()
                self.archive_completed_jobs()

            # finally, if there's still time left let's just twiddle our thumbs for a bit
            time_left = self.iteration_time - (time.time() - start_time)
            if time_left > 0:
                logger.info(f"sleeping {time_left}")
                time.sleep(time_left)
