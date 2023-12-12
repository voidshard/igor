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


class LayerPanel(ClosablePanel):

    def __init__(self, parent):
        super(LayerPanel, self).__init__(parent)

        self.setWidget(_LayerWidget(self))
        self.setWindowTitle("Layers")
        self.set_title("Layers")


class _LayerWidget(PanelWidget):
    """
    The widget here is the parent widget which holds together both the model
    and the view for our table.
    """
    def __init__(self, parent=None):
        super(_LayerWidget, self).__init__(parent)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 0, 4, 4)

        self.__refreshEnabled = True

        self._model = model = _LayerModel(self)
        self.__proxy = proxy = _LayerFilterProxyModel(self)
        proxy.setSortRole(model.DataRole)
        proxy.setSourceModel(model)

        self._view = view = TableWidget(self)
        view.setModel(proxy)

        layout.addWidget(view)

        headers = model.HEADERS
        view.setColumnWidth(headers.index('Name'), 100)
        view.setColumnWidth(headers.index('ID'), 250)
        view.setColumnWidth(headers.index('Status'), 80)
        view.setColumnWidth(headers.index('Order'), 30)
        view.setColumnWidth(headers.index('PausedAt'), 100)
        view.setColumnWidth(headers.index('CreatedAt'), 100)
        view.setColumnWidth(headers.index('UpdatedAt'), 100)
        view.setContextMenuPolicy(Qt.CustomContextMenu)

        view.customContextMenuRequested.connect(self.__showContextMenu)
        view.clicked.connect(self.__itemClicked)
        view.doubleClicked.connect(self.__itemDoubleClicked)

        # bind all the Events we might need to respond to
        Events.JobSelected.connect(self.__set_job)
        Events.Refresh.connect(self.__refresh)

    def __refresh(self):
        """"""
        self._model.refresh()

    def __set_job(self, id_: str):
        self._model.set_current_job(id_)

    def __showContextMenu(self, pos):
        """"""
        menu = QMenu()
        menu.addAction(
            QIcon(resources.get("pause.png")),
            "Pause / Resume",
            self.__pause_selected
        )
        menu.addAction(
            QIcon(resources.get("skip.png")),
            "Skip",
            self.__skip_selected
        )
        menu.exec_(self.mapToGlobal(pos))

    def __skip_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                service.Service.skip_layer(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error skipping layer {o.id}: {e}")
        self.__refresh()

    def __pause_selected(self):
        """

        """
        for o in self.get_selected():
            try:
                if o.paused:
                    service.Service.unpause_layer(o.id, o.etag)
                else:
                    service.Service.pause_layer(o.id, o.etag)
            except Exception as e:
                Events.Status.emit(f"error pausing layer {o.id}: {e}")
        self.__refresh()

    def __itemClicked(self, index):
        """Default left-click handler

        Args:
            index (QIndex):

        """
        obj = index.data(self._model.ObjectRole)
        Events.LayerSelected.emit(obj.id)

    def __itemDoubleClicked(self, index):
        """Default double left click handler

        Args:
            index (QIndex):
        """
        self.__itemClicked(index)
        obj = index.data(self._model.ObjectRole)
        Events.OpenDetails.emit("layer", obj.id)


class _LayerModel(AbstractEditableTableModel):

    HEADERS = [
        "Name",
        "ID",
        "Status",
        "Order",
        "PausedAt",
        "CreatedAt",
        "UpdatedAt",
    ]

    DISPLAY_CALLBACKS = {
        0: lambda n: n.name,
        1: lambda n: n.id,
        2: lambda n: n.status,
        3: lambda n: n.order,
        4: lambda n: n.paused_at,
        5: lambda n: n.created_at,
        6: lambda n: n.updated_at,
    }

    def __init__(self, parent=None):
        super(_LayerModel, self).__init__(parent)
        self._current_job = None

    def set_current_job(self, id_: str):
        self._current_job = id_
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
            []Layer
        """
        if not self._current_job:
            return []

        try:
            for i in service.Service.get_layers(
                job_ids=[self._current_job],
            ):
                yield i
        except Exception as e:
            Events.Status.emit(f"unable to fetch layer information: {e}")


class _LayerFilterProxyModel(AlnumSortProxyModel):

    def __init__(self, *args, **kwargs):
        super(_LayerFilterProxyModel, self).__init__(*args, **kwargs)
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
                _LayerFilterProxyModel, self).filterAcceptsRow(row, parent)

        if not self.__regex:
            return True

        model = self.sourceModel()
        idx = model.index(row, 0, parent)
        if not idx.isValid():
            return False

        obj = model.data(idx, _LayerModel.ObjectRole)
        if not obj:
            return False

        result = self.__regex.match(obj.name)
        if not result:
            return False
        return True
