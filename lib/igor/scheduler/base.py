import abc


class Base(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def run(self):
        """There are four primary goals that need to happen here:

        1: [task scheduling] the task runner system need to be feed more work as the tasks in
           given layers become ready for execution.

        2: [layer watching] layers that are currently running need to be watch to check if/when
           they complete (or error). Their child layers then need to be queued up for task
           scheduling.

        3: [worker orphaning] workers that stop replying to us need to be cast out and their
           tasks reclaimed for re-scheduling.

        4: [data archiving] jobs that complete should have their data archived or deleted in order
           to keep our working set as small as possible.

        """
        pass
