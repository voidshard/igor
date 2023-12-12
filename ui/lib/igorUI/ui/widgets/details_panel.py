import pprint

from igorUI.ui.widgets.widget import DetailsWidget, DetailsPanel
from igorUI.ui.manifest import *
from igorUI import service


class _JobDetailsWidget(DetailsWidget):

    def __init__(self, parent, object_id):
        super(_JobDetailsWidget, self).__init__(parent, object_id)

        self._widget_status = self._add_widget_row("Status:", 3, QLabel(), "last status")
        self._widget_created = self._add_widget_row("Created:", 4, QLabel(), "created")
        self._widget_updated = self._add_widget_row("Updated:", 5, QLabel(), "updated")

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)
        self._widget_name.setText(obj.name)
        self._widget_id.setText(obj.id)
        self._widget_status.setText(obj.status)
        self._widget_etag.setText(obj.etag)
        self._widget_created.setText(obj.created_at)
        self._widget_updated.setText(obj.updated_at)

    def fetch_object(self):
        return service.Service.one_job(self._object_id)


class JobDetailsPanel(DetailsPanel):

    _title = "Job %s"
    _widget_class = _JobDetailsWidget


class _LayerDetailsWidget(_JobDetailsWidget):

    def __init__(self, parent, object_id):
        super(_LayerDetailsWidget, self).__init__(parent, object_id)

        self._paused_at = self._add_widget_row("Paused At:", 6, QLabel(), "paused at")
        self._widget_order = self._add_widget_row("Order:", 7, QLabel(), "layer order")
        self._widget_job_id = self._add_widget_row("JobID:", 8, QLabel(), "job id")

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)

        self._paused_at.setText(obj.paused_at)
        self._widget_order.setText(f"{obj.order}")
        self._widget_job_id.setText(f"{obj.job_id}")

    def fetch_object(self):
        return service.Service.one_layer(self._object_id)


class LayerDetailsPanel(DetailsPanel):

    _title = "Layer %s"
    _widget_class = _LayerDetailsWidget


class _TaskDetailsWidget(_JobDetailsWidget):

    def __init__(self, parent, object_id):
        super(_TaskDetailsWidget, self).__init__(parent, object_id)

        self._paused_at = self._add_widget_row("Paused At:", 6, QLabel(), "paused at")
        self._widget_job_id = self._add_widget_row("JobID:", 7, QLabel(), "job id")
        self._widget_layer_id = self._add_widget_row("LayerID:", 8, QLabel(), "layer id")
        self._widget_args = self._add_widget_row("Args:", 9, QLabel(), "args")
        self._widget_type = self._add_widget_row("Type:", 10, QLabel(), "type")
        self._widget_queue_id = self._add_widget_row("QID:", 11, QLabel(), "queued task id")
        self._widget_message = self._add_widget_row("Message:", 12, QLabel(), "message")

    def set_widgets(self, obj):
        """

        :param obj:

        """
        super().set_widgets(obj)

        self._paused_at.setText(obj.paused_at)
        self._widget_job_id.setText(f"{obj.job_id}")
        self._widget_layer_id.setText(f"{obj.layer_id}")
        self._widget_args.setText(f"{obj.args}")
        self._widget_type.setText(f"{obj.type}")
        self._widget_queue_id.setText(f"{obj.queue_task_id}")
        self._widget_message.setText(f"{obj.message}")


    def fetch_object(self):
        return service.Service.one_task(self._object_id)


class TaskDetailsPanel(DetailsPanel):

    _title = "Task %s"
    _widget_class = _TaskDetailsWidget

