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
from igorUI.ui.events import Events
from igorUI import service
from igorUI.service import enums


class JobPanel(ClosablePanel):

    def __init__(self, parent):
        super(JobPanel, self).__init__(parent)

        self.setWidget(_JobWidget(self))
        self.setWindowTitle("Jobs")
        self.set_title("Jobs")

    @property
    def default_dock_widget_area(self):
        """Return where this widget should be placed by default, relative to
        the dock widget it belongs to.

        Returns:
            int
        """
        return Qt.TopDockWidgetArea


class _JobWidget(PanelWidget):
    """
    The widget here is the parent widget which holds together both the model
    and the view for our table.
    """
    def __init__(self, parent=None):
        super(_JobWidget, self).__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 4)

        self.__refreshEnabled = True

        self._model = model = _JobModel(self)
        self.__proxy = proxy = _JobFilterProxyModel(self)
        proxy.setSortRole(model.DataRole)
        proxy.setSourceModel(model)

        self._view = view = TableWidget(self)
        view.setModel(proxy)

        layout.addWidget(view)

        headers = model.HEADERS
        view.setColumnWidth(headers.index('Name'), 250)
        view.setColumnWidth(headers.index('JobId'), 250)
        view.setColumnWidth(headers.index('State'), 100)
        view.setColumnWidth(headers.index('Paused'), 50)
        view.setColumnWidth(headers.index('UserId'), 200)
        view.setContextMenuPolicy(Qt.CustomContextMenu)

        view.customContextMenuRequested.connect(self.__showContextMenu)
        view.clicked.connect(self.__itemClicked)
        view.doubleClicked.connect(self.__itemDoubleClicked)

        Events.Refresh.connect(self.__refresh)

    def __refresh(self):
        """"""
        self._model.refresh()

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

    def __skip_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.skip_job(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error skipping job {o.id}: {e}")
        self.__refresh()

    def __retry_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.retry_job(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error retrying job {o.id}: {e}")
        self.__refresh()

    def __pause_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.pause_job(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error pausing job {o.id}: {e}")
        self.__refresh()

    def __kill_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.kill_job(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error killing job {o.id}: {e}")
        self.__refresh()

    def __itemClicked(self, index):
        """Default left-click handler

        Args:
            index (QIndex):

        """
        obj = index.data(self._model.ObjectRole)
        Events.JobSelected.emit(obj.id)

    def __itemDoubleClicked(self, index):
        """Default double left click handler

        Args:
            index (QIndex):
        """
        self.__itemClicked(index)
        obj = index.data(self._model.ObjectRole)
        Events.OpenDetails.emit("job", obj.id)


class _JobModel(AbstractEditableTableModel):

    HEADERS = ["Name", "JobId", "State", "Paused", "UserId"]

    DISPLAY_CALLBACKS = {
        0: lambda n: n.name,
        1: lambda n: n.id,
        2: lambda n: n.state,
        3: lambda n: n.paused,
        4: lambda n: n.user_id,
    }

    def __init__(self, parent=None):
        super(_JobModel, self).__init__(parent)

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
            []Job
        """
        try:
            for i in service.Service.get_jobs(states=enums.ALL_NOT_COMPLETE):
                yield i
        except Exception as e:
            Events.Status.emit(f"unable to fetch job information: {e}")


class _JobFilterProxyModel(AlnumSortProxyModel):

    def __init__(self, *args, **kwargs):
        super(_JobFilterProxyModel, self).__init__(*args, **kwargs)
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
                _JobFilterProxyModel, self).filterAcceptsRow(row, parent)

        if not self.__regex:
            return True

        model = self.sourceModel()
        idx = model.index(row, 0, parent)
        if not idx.isValid():
            return False

        obj = model.data(idx, _JobModel.ObjectRole)
        if not obj:
            return False

        result = self.__regex.match(obj.name)
        if not result:
            return False
        return True
