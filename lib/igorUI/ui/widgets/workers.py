import re

from igorUI.ui.widgets.panel import ClosablePanel
from igorUI.ui.widgets.widget import (
    PanelWidget,
    TableWidget,
    AbstractEditableTableModel,
    AlnumSortProxyModel
)
from igorUI.ui.manifest import QVBoxLayout, Qt
from igorUI import client
from igorUI.ui.events import Events


class WorkerPanel(ClosablePanel):

    def __init__(self, parent):
        super(WorkerPanel, self).__init__(parent)

        self.setWidget(_WorkerWidget(self))
        self.setWindowTitle("Workers")
        self.set_title("Workers")


class _WorkerWidget(PanelWidget):
    """
    The widget here is the parent widget which holds together both the model
    and the view for our table.
    """
    def __init__(self, parent=None):
        super(_WorkerWidget, self).__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 4)

        self.__refreshEnabled = True

        self._model = model = _WorkerModel(self)
        self.__proxy = proxy = _WorkerFilterProxyModel(self)
        proxy.setSortRole(model.DataRole)
        proxy.setSourceModel(model)

        self.__view = view = TableWidget(self)
        view.setModel(proxy)

        layout.addWidget(view)

        headers = model.HEADERS
        view.setColumnWidth(headers.index('Name'), 250)
        view.setColumnWidth(headers.index('WorkerId'), 250)
        view.setColumnWidth(headers.index('JobId'), 250)
        view.setColumnWidth(headers.index('LayerId'), 250)
        view.setColumnWidth(headers.index('TaskId'), 250)
        view.setColumnWidth(headers.index('LastPing'), 100)
        view.setColumnWidth(headers.index('Host'), 100)
        view.setContextMenuPolicy(Qt.CustomContextMenu)

        view.customContextMenuRequested.connect(self.__showContextMenu)
        view.clicked.connect(self.__itemClicked)
        view.doubleClicked.connect(self.__itemDoubleClicked)

        # bind all the Events we might need to respond to
        Events.Refresh.connect(self.__refresh)

    def __refresh(self):
        """"""
        self._model.refresh()

    def __showContextMenu(self, pos):
        """"""
        pass

    def __itemClicked(self, index):
        """Default left-click handler

        Args:
            index (QIndex):

        """
        pass

    def __itemDoubleClicked(self, index):
        """Default double left click handler

        Args:
            index (QIndex):
        """
        obj = index.data(self._model.ObjectRole)
        Events.OpenDetails.emit("worker", obj.id)


class _WorkerModel(AbstractEditableTableModel):

    HEADERS = ["Name", "WorkerId", "JobId", "LayerId", "TaskId", "LastPing", "Host"]

    DISPLAY_CALLBACKS = {
        0: lambda n: n.key,
        1: lambda n: n.id,
        2: lambda n: n.job_id,
        3: lambda n: n.layer_id,
        4: lambda n: n.task_id,
        5: lambda n: n.last_ping_time,
        6: lambda n: n.host,
    }

    def __init__(self, parent=None):
        super(_WorkerModel, self).__init__(parent)

    def display_for_index(self, index):
        """Return the text that should be displayed on the panel for the given
        index.

        Args:
            index (QIndex):

        Returns:
            str
        """
        obj = index.data(self.ObjectRole)
        cb = self.DISPLAY_CALLBACKS.get(index.column())
        if not cb:
            return ""
        return cb(obj)

    def fetchObjects(self):
        """Called by parent class. This function is responsible to returning
        the list of objects to display on demand.

        Returns:
            []Worker
        """
        try:
            for i in client.Service.get_workers():
                yield i
        except Exception as e:
            Events.Status.emit(f"unable to fetch worker information: {e}")


class _WorkerFilterProxyModel(AlnumSortProxyModel):

    def __init__(self, *args, **kwargs):
        super(_WorkerFilterProxyModel, self).__init__(*args, **kwargs)
        self.__regex = None
        self.__all_filters = (None, )
        self.__customFilterEnabled = False

    def setFilters(self, regex=None):
        """Set the filters to be used in displaying the table data.

        Args:
            regex (str):

        """
        if regex is not None:
            try:
                self.__regex = re.compile(regex)
            except Exception as e:
                return

            self.__all_filters = (self.__regex,)

        self.__customFilterEnabled = any(self.__all_filters)
        self.invalidateFilter()

    def filterAcceptsRow(self, row, parent):
        """Return whether the given row should be displayed, given our current
        filters.

        Args:
            row:
            parent:

        Returns:
            bool
        """
        if not self.__customFilterEnabled:
            return super(
                _WorkerFilterProxyModel, self).filterAcceptsRow(row, parent)

        if not self.__regex:
            return True

        model = self.sourceModel()
        idx = model.index(row, 0, parent)
        if not idx.isValid():
            return False

        obj = model.data(idx, _WorkerModel.ObjectRole)
        if not obj:
            return False

        result = self.__regex.match(obj.name)
        if not result:
            return False
        return True
