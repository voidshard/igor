import re

from igorUI.ui.widgets.panel import ClosablePanel
from igorUI.ui.widgets.widget import (
    PanelWidget,
    TableWidget,
    AbstractEditableTableModel,
    AlnumSortProxyModel
)
from igorUI.ui.manifest import QVBoxLayout, Qt, QMenu, QIcon
from igorUI.ui import resources
from igorUI import service
from igorUI.service import enums
from igorUI.ui.events import Events


class TaskPanel(ClosablePanel):

    def __init__(self, parent):
        super(TaskPanel, self).__init__(parent)

        self.setWidget(_TaskWidget(self))
        self.setWindowTitle("Tasks")
        self.set_title("Tasks")


class _TaskWidget(PanelWidget):
    """
    The widget here is the parent widget which holds together both the model
    and the view for our table.
    """
    def __init__(self, parent=None):
        super(_TaskWidget, self).__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 4)

        self.__refreshEnabled = True

        self._model = model = _TaskModel(self)
        self.__proxy = proxy = _TaskFilterProxyModel(self)
        proxy.setSortRole(model.DataRole)
        proxy.setSourceModel(model)

        self._view = view = TableWidget(self)
        view.setModel(proxy)

        layout.addWidget(view)

        headers = model.HEADERS
        view.setColumnWidth(headers.index('Name'), 250)
        view.setColumnWidth(headers.index('TaskId'), 250)
        view.setColumnWidth(headers.index('State'), 100)
        view.setColumnWidth(headers.index('Paused'), 50)
        view.setColumnWidth(headers.index('UserId'), 200)
        view.setColumnWidth(headers.index('Cmd'), 400)
        view.setColumnWidth(headers.index('Attempts'), 50)
        view.setContextMenuPolicy(Qt.CustomContextMenu)

        view.customContextMenuRequested.connect(self.__showContextMenu)
        view.clicked.connect(self.__itemClicked)
        view.doubleClicked.connect(self.__itemDoubleClicked)

        # bind all the Events we might need to respond to
        Events.LayerSelected.connect(self.__set_layer)
        Events.Refresh.connect(self.__refresh)

    def __refresh(self):
        """"""
        self._model.refresh()

    def __set_layer(self, id_: str):
        self._model.set_current_layer(id_)

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
        self.__itemClicked(index)
        obj = index.data(self._model.ObjectRole)
        Events.OpenDetails.emit("task", obj.id)

    def __showContextMenu(self, pos):
        """"""
        menu = QMenu()
        menu.addAction(
            QIcon(resources.get("pause.png")),
            "Pause / Resume",
            self.__pause_selected
        )
        menu.addAction(
            QIcon(resources.get("retry.png")),
            "Retry",
            self.__retry_selected
        )
        menu.addAction(
            QIcon(resources.get("skip.png")),
            "Skip",
            self.__skip_selected
        )
        menu.addAction(
            QIcon(resources.get("kill.png")),
            "Kill",
            self.__kill_selected
        )
        menu.exec_(self.mapToGlobal(pos))

    def __pause_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.pause_task(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error pausing task {o.id}: {e}")
        self.__refresh()

    def __skip_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.skip_task(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error skipping task {o.id}: {e}")
        self.__refresh()

    def __retry_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.retry_task(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error retrying task {o.id}: {e}")
        self.__refresh()

    def __kill_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.kill_task(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error killing task {o.id}: {e}")
        self.__refresh()


class _TaskModel(AbstractEditableTableModel):

    HEADERS = ["Name", "TaskId", "State", "Paused", "UserId", "Cmd", "Attempts"]

    DISPLAY_CALLBACKS = {
        0: lambda n: n.name,
        1: lambda n: n.id,
        2: lambda n: n.state,
        3: lambda n: n.paused,
        4: lambda n: n.user_id,
        5: lambda n: n.cmd_string,
        6: lambda n: n.attempts,
    }

    def __init__(self, parent=None):
        super(_TaskModel, self).__init__(parent)
        self._current_layer = None

    def set_current_layer(self, id_: str):
        self._current_layer = id_
        self.refresh()

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
            []Task
        """
        if not self._current_layer:
            return []

        try:
            for i in service.Service.get_tasks(
                layer_ids=[self._current_layer],
                states=enums.ALL,
            ):
                yield i
        except Exception as e:
            Events.Status.emit(f"unable to fetch task information: {e}")


class _TaskFilterProxyModel(AlnumSortProxyModel):

    def __init__(self, *args, **kwargs):
        super(_TaskFilterProxyModel, self).__init__(*args, **kwargs)
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
                _TaskFilterProxyModel, self).filterAcceptsRow(row, parent)

        if not self.__regex:
            return True

        model = self.sourceModel()
        idx = model.index(row, 0, parent)
        if not idx.isValid():
            return False

        obj = model.data(idx, _TaskModel.ObjectRole)
        if not obj:
            return False

        result = self.__regex.match(obj.name)
        if not result:
            return False
        return True
