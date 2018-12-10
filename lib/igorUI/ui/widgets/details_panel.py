import pprint

from igorUI.ui.widgets.widget import DetailsWidget, DetailsPanel
from igorUI.ui.manifest import *
from igorUI import client


class _JobDetailsWidget(DetailsWidget):

    def __init__(self, parent, object_id):
        super(_JobDetailsWidget, self).__init__(parent, object_id)

        self._widget_user = self._add_widget_row("User:", 3, QLabel(), "owner id")
        self._widget_state = self._add_widget_row("State:", 4, QLabel(), "latest state")
        self._widget_paused = self._add_widget_row("Paused:", 5, QLabel(), "paused time")

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)
        self._widget_user.setText(obj.user_id)
        self._widget_state.setText(obj.state)
        self._widget_paused.setText(obj.paused)

    def fetch_object(self):
        return client.Service.one_job(self._object_id)


class JobDetailsPanel(DetailsPanel):

    _title = "Job %s"
    _widget_class = _JobDetailsWidget


class _LayerDetailsWidget(_JobDetailsWidget):

    def __init__(self, parent, object_id):
        super(_LayerDetailsWidget, self).__init__(parent, object_id)

        self._widget_priority = self._add_widget_row("Priority:", 6, QLabel(), "layer priority")

        self._widget_parents = self._add_widget_row(
            "Parent Layers:", 7, QLabel(), "number parents"
        )
        self._widget_siblings = self._add_widget_row(
            "Sibling Layers:", 8, QLabel(), "number siblings"
        )
        self._widget_children = self._add_widget_row(
            "Child Layers:", 9, QLabel(), "number children"
        )

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)

        self._widget_priority.setText(f"{obj.priority}")
        self._widget_parents.setText(f"{len(obj.parents)}")
        self._widget_siblings.setText(f"{len(obj.siblings)}")
        self._widget_children.setText(f"{len(obj.children)}")

    def fetch_object(self):
        return client.Service.one_layer(self._object_id)


class LayerDetailsPanel(DetailsPanel):

    _title = "Layer %s"
    _widget_class = _LayerDetailsWidget


class _TaskDetailsWidget(_JobDetailsWidget):

    def __init__(self, parent, object_id):
        super(_TaskDetailsWidget, self).__init__(parent, object_id)

        self._widget_worker = self._add_widget_row("Worker Id:", 6, QLabel(), "worker id (if any)")
        self._widget_cmd = self._add_widget_row("Command:", 7, QLabel(), "task command")
        self._widget_attempts = self._add_widget_row(
            "Attempts:", 8, QLabel(), "number of attempts made to run task"
        )
        self._widget_mattempts = self._add_widget_row(
            "Max Attempts:", 9, QLabel(), "number of attempts to be made before igor gives up"
        )

        self._widget_env = QTextEdit()
        self._widget_env.setToolTip("task environment data")
        self.mainlayout.addWidget(self._widget_env, 2, 0)

        self._widget_result = QTextEdit()
        self._widget_result.setToolTip("task 'result' object currently in the system")
        self.mainlayout.addWidget(self._widget_result, 3, 0)

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)

        self._widget_worker.setText(obj.worker_id)
        self._widget_attempts.setText(f"{obj.attempts}")
        self._widget_mattempts.setText(f"{obj.max_attempts}")
        self._widget_cmd.setText(obj.cmd_string)
        self._widget_env.setText(pprint.pformat(obj.env, indent=2))
        self._widget_result.setText(f"{obj.result}")

    def fetch_object(self):
        return client.Service.one_task(self._object_id)


class TaskDetailsPanel(DetailsPanel):

    _title = "Task %s"
    _widget_class = _TaskDetailsWidget


class _WorkerDetailsWidget(DetailsWidget):

    def __init__(self, parent, object_id):
        super(_WorkerDetailsWidget, self).__init__(parent, object_id)

        msg = "id of currently running task (if any)"

        self._widget_job = self._add_widget_row("Job Id:", 6, QLabel(), f"job {msg}")
        self._widget_lyr = self._add_widget_row("Layer Id:", 7, QLabel(), f"layer {msg}")
        self._widget_tsk = self._add_widget_row("Task Id:", 8, QLabel(), f"task {msg}")
        self._widget_host = self._add_widget_row("Host:", 9, QLabel(), "host machine")
        self._widget_last_ping = self._add_widget_row("Last Ping:", 10, QLabel(), "last ping time")

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)

        self._widget_job.setText(obj.job_id)
        self._widget_lyr.setText(obj.layer_id)
        self._widget_tsk.setText(obj.task_id)
        self._widget_host.setText(obj.host)
        self._widget_last_ping.setText(obj.last_ping_time)

    def fetch_object(self):
        return client.Service.one_worker(self._object_id)


class WorkerDetailsPanel(DetailsPanel):

    _title = "Worker %s"
    _widget_class = _WorkerDetailsWidget
