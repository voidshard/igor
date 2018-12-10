import re

import pprint

from igorUI.ui.manifest import *
from igorUI.ui.widgets.panel import ClosablePanel
from igorUI.ui import events


DATA_ROLE = Qt.UserRole
DEFAULT_ROW_HEIGHT = 20
_ALNUM_RX = re.compile('([0-9]+)')


class TableWidget(QTableView):
    """
    Simple class that sets up some default table settings
    """

    def __init__(self, *args, **kwargs):
        super(TableWidget, self).__init__(*args, **kwargs)

        self.setSelectionBehavior(self.SelectRows)
        self.setSelectionMode(self.ExtendedSelection)
        self.setAlternatingRowColors(True)
        self.setAutoFillBackground(False)
        self.setShowGrid(True)
        self.setSortingEnabled(True)
        self.sortByColumn(-1, Qt.AscendingOrder)
        self.horizontalHeader().setStretchLastSection(True)

        vheader = self.verticalHeader()
        vheader.hide()
        vheader.setDefaultSectionSize(DEFAULT_ROW_HEIGHT)


class PanelWidget(QWidget):
    """
    Simple QWidget that panel widgets inherit from to override some internal
    funcs. The shortcut_add function is called on a panel by the main app if
    ctrl+a is used whilst the widget is in focus
    """

    def shortcut_add(self):
        pass

    def get_selected(self):
        """Return the currently selected object(s) (if any) in our panel

        Returns:
            []Object
        """
        rows = self._view.selectionModel().selectedRows()
        return [index.data(self._model.ObjectRole) for index in rows]


def alphaNumericKey(aString):
    """Break the given string, splitting our all numbers (and cast them to ints)

    Return split result as list that includes both numbers and strings.
    That is, given "abc123def" return ["abc", 123, "def"]

    Args:
        aString (str): string to operate on

    Returns:
        [](str and/or int)
    """
    return [int(c) if c.isdigit() else c for c in _ALNUM_RX.split(aString)]


class AlnumSortProxyModel(QSortFilterProxyModel):
    RX_ALNUMS = QRegExp('(\d+|\D+)')

    def __init__(self, *args, **kwargs):
        super(AlnumSortProxyModel, self).__init__(*args, **kwargs)
        self.setSortRole(DATA_ROLE)
        self.__validAlnum = str

    def lessThan(self, left, right):
        """

        Args:
            left:
            right:

        Returns:
            bool
        """
        sortRole = self.sortRole()
        leftData = left.data(sortRole)

        if isinstance(leftData, self.__validAlnum):
            rightData = right.data(sortRole)
            if leftData == rightData:
                return False
            return alphaNumericKey(leftData) < alphaNumericKey(rightData)

        return super(AlnumSortProxyModel, self).lessThan(left, right)


class AbstractTableModel(QAbstractTableModel):
    # A list of string headers for the model
    HEADERS = []

    # Map column number => callback that provides a string display val
    # for a given plow object
    DISPLAY_CALLBACKS = {}

    IdRole = Qt.UserRole
    ObjectRole = Qt.UserRole + 1
    DataRole = Qt.UserRole + 2

    def __init__(self, parent=None):
        QAbstractTableModel.__init__(self, parent)

        # list of objects
        self._items = []

        # map[str]int, that is, map[object.id] index in _items list
        self._index = {}

        self.__columnCount = len(self.HEADERS)

        # Should the refresh operation remove existing
        # items that are not found in each new update?
        self.refreshShouldRemove = True

    def fetchObjects(self):
        """
        Method that should be defined in subclasses,
        to fetch new data that will be applied to the model.

        Should return a list of objects
        """
        return []

    def hasChildren(self, parent):
        return False

    def refresh(self):
        """Handles calling 'fetchObjects' implemented by child class (hopefully)
        and merging the returned data into the current data set (or dropping
        data that no longer appears).

        This assumes that Objects returned by fetchObjects() have a 'id'
        property which is unique.

        Each object will either be added (if it doesn't exist in the table),
        updated (if it does) or deleted (if it exists in the table but not in
        the list returned by fetchObjects()).

        """
        updated = set()
        to_add = set()
        object_ids = set()

        rows = self._index
        columnCount = self.columnCount()
        parent = QModelIndex()
        objects = self.fetchObjects()

        for obj in objects:  # Update existing
            object_ids.add(obj.id)

            try:
                idx = self._index[obj.id]
                self._items[idx] = obj
                updated.add(obj.id)
                self.dataChanged.emit(
                    self.index(idx, 0), self.index(idx, columnCount - 1))
            except (IndexError, KeyError):
                to_add.add(obj)

        if to_add:  # Add new
            size = len(to_add)
            start = len(self._items)
            end = start + size - 1
            self.beginInsertRows(parent, start, end)
            self._items.extend(to_add)
            self.endInsertRows()

        if self.refreshShouldRemove:  # Remove missing
            to_remove = set(list(self._index.keys())).difference(object_ids)
            if to_remove:
                row_ids = ((rows[old_id], old_id) for old_id in to_remove)

                for row, old_id in sorted(row_ids, reverse=True):
                    self.beginRemoveRows(parent, row, row)
                    obj = self._items.pop(row)
                    self.endRemoveRows()

        # reindex the items
        self._index = dict(((item.id, i) for i, item in enumerate(self._items)))

    def rowCount(self, parent):
        """

        Args:
            parent:

        Returns:
            int
        """
        if parent and parent.isValid():
            return 0
        return len(self._items)

    def columnCount(self, parent=None):
        """

        Args:
            parent:

        Returns:
            int
        """
        if parent and parent.isValid():
            return 0
        return self.__columnCount

    def data(self, index, role):
        """Return the data for the given table index.

        Args:
            index (QIndex):
            role (int):

        Returns:
            ?
        """
        row = index.row()
        col = index.column()
        obj = self._items[row]

        if role == Qt.DisplayRole:
            cbk = self.DISPLAY_CALLBACKS.get(col)
            if cbk is not None:
                return cbk(obj)
            return self.displayFallback(row, col, obj)

        elif role == Qt.TextAlignmentRole:
            if col != 0:
                return Qt.AlignCenter

        elif role == self.IdRole:
            return obj.id

        elif role == self.ObjectRole:
            return obj

        return None

    def displayFallback(self, row, col, obj):
        """This function is called when a callback cant be found in
        DISPLAY_CALLBACKS for the given row / column

        Args:
            row (int):
            col (int):
            obj (object):

        Returns:
            str
        """
        pass

    def setItemList(self, itemList):
        """Forcibly set internal item list -- akin to manually using
        fetchObjects then passing this into refresh.

        Args:
            itemList ([]Object):

        """
        self.beginResetModel()
        self._items = itemList
        self._index = dict((n.id, row) for row, n in enumerate(itemList))
        self.endResetModel()

    def itemFromIndex(self, idx):
        """Look up an object by it's Index

        Args:
            idx (QIndex):

        Returns:
            Object
        """
        if not idx.isValid():
            return None

        item = self._items[idx.row()]
        return item

    def headerData(self, section, orientation, role):
        """

        Args:
            section:
            orientation:
            role:

        """
        if role == Qt.TextAlignmentRole:
            if section == 0:
                return Qt.AlignLeft | Qt.AlignVCenter
            else:
                return Qt.AlignCenter

        if role != Qt.DisplayRole:
            return None

        if orientation == Qt.Vertical:
            return section

        return self.HEADERS[section]

    def get_item(self, row):
        """Return the object at the given row.

        Args:
            row (int):

        Returns:
            Object

        Raises:
            IndexError
        """
        return self._items[row]


class AbstractEditableTableModel(AbstractTableModel):

    # A map of editable columns to
    EDIT_CALLBACKS = {}

    # a map of editable columns to auto complete suggestions
    EDIT_SUGGESTIONS = {}

    def __init__(self, parent=None):
        AbstractTableModel.__init__(self, parent)

    def update_item(self, obj):
        """Update a single obj that already exists in our set. This uses the
        objects ID and maps it to the correct row internally.

        Args:
            obj (object):

        """
        idx = self._index.get(obj.id)
        if idx is not None:  # update item
            self._items[idx] = obj

            # emit change message so that the row is redrawn
            self.dataChanged.emit(
                self.index(idx, 0), self.index(idx, self.columnCount() - 1))
        else:  # add item
            start = len(self._items)
            self.beginInsertRows(QModelIndex(), start, start)
            self._items.append(obj)
            self.endInsertRows()
        self._index = dict(((item.id, i) for i, item in enumerate(self._items)))

    def flags(self, index):
        """Returns the item flags for the given index.

        Qt uses this to determine what can be done with the given table index.

        Args:
            index (QIndex):

        """
        if index.column() in self.EDIT_CALLBACKS:
            return Qt.ItemIsEnabled | Qt.ItemIsSelectable | Qt.ItemIsEditable | Qt.ItemIsUserCheckable
        return super(AbstractTableModel, self).flags(index)

    def setData(self, index, new_value, role):
        """Call set data on the given data in the given index, updating it
        with the new value.

        Assuming it allows editing..

        Args:
            index (QIndex):
            new_value (str):
            role (int):

        Returns:
            bool
        """
        cb = self.EDIT_CALLBACKS.get(index.column())
        if cb is None:
            return False

        obj = self.get_item(index.row())
        return cb(obj, new_value)


class DetailsPanel(ClosablePanel):

    _widget_class = QWidget
    _title = "%s"

    def __init__(self, parent, object_id):
        super(ClosablePanel, self).__init__(parent)

        self.setContentsMargins(4, 0, 4, 4)
        self._widget = self._widget_class(parent, object_id)
        self.setWidget(self._widget)

        self._obj_id = object_id
        self.set_title(self._title % self._obj_id)

    def refresh(self):
        self._widget.refresh()


class DetailsWidget(QWidget):

    def __init__(self, parent, object_id):
        super(DetailsWidget, self).__init__(parent)
        self._object_id = object_id

        self.setContentsMargins(4, 0, 4, 4)
        self.mainlayout = QGridLayout()
        self.layout = QGridLayout()

        upper_widget = QWidget(self)
        upper_widget.setLayout(self.layout)
        self.mainlayout.addWidget(upper_widget, 0, 0)
        self.setLayout(self.mainlayout)

        events.Events.Refresh.connect(self.refresh)

        self._widget_id = self._add_widget_row("Id:", 0, QLabel(), "object id")
        self._widget_etag = self._add_widget_row("Etag:", 1, QLabel(), "object etag")
        self._widget_name = self._add_widget_row("Name:", 2, QLabel(), "object name / key")

        self._widget_meta = QTextEdit()  # placed in special lower widget area
        self._widget_meta.setToolTip("misc metadata associated with object")
        self.mainlayout.addWidget(self._widget_meta, 1, 0)

    def _add_widget_row(self, text, row, widget, tip=""):
        """Adds a widget with a label beside it.

        :param text:
        :param row:
        :return: QCheckBox
        """
        label = QLabel(text)
        if tip:
            label.setToolTip(tip)
            widget.setToolTip(tip)

        self.layout.addWidget(label, row, 1)
        self.layout.addWidget(widget, row, 2)
        return widget

    def refresh(self):
        """
        """
        obj = self.fetch_object()
        if obj:
            self.set_widgets(obj)

    def set_widgets(self, obj):
        """

        :param obj:

        """
        self._widget_id.setText(obj.id)
        self._widget_name.setText(obj.key)
        self._widget_etag.setText(obj.etag)
        self._widget_meta.setText(pprint.pformat(obj.metadata or {}, indent=2))

    def fetch_object(self):
        pass
