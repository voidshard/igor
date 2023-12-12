import uuid

from igorUI.ui.manifest import *
from igorUI.ui import resources


class Panel(QDockWidget):
    _FLOAT = "float.png"

    panel_closed = Signal(object)

    def __init__(self, parent):
        super(Panel, self).__init__(parent)
        self._id = uuid.uuid4()

        # label for the widget title
        self.__label = QLabel(self)
        self.__label.setIndent(10)

        # toolbar to hold buttons
        self._toolbar = QToolBar(self)
        self._toolbar.setIconSize(QSize(18, 18))
        self._add_toolbar_actions()
        float_action = QAction(QIcon(resources.get(self._FLOAT)), "Float", self)
        float_action.toggled.connect(self.__float_changed)
        float_action.setCheckable(True)
        self._toolbar.addAction(float_action)

        # title bar to hold toolbar & label widget
        self._title_bar = QWidget(self)
        barLayout = QHBoxLayout(self._title_bar)
        barLayout.setSpacing(0)
        barLayout.setContentsMargins(0, 0, 0, 0)
        barLayout.addStretch()
        barLayout.addWidget(self.__label)
        barLayout.addWidget(self._toolbar)
        self.setTitleBarWidget(self._title_bar)

    def _add_toolbar_actions(self):
        """For adding child actions.

        This exists so that actions (buttons) are added in the order of
         child -> parent -> parent of parent

        """
        pass

    def _add_shortcut(self, name, sequence, func):
        """Build & attach a shortcut with the given settings

        Args:
            name (str): name of the action
            sequence (str): keypress sequence (eg 'Ctrl+b')
            func (func): callback to attach to action

        """
        act = QAction(name, self)
        act.triggered.connect(func)
        act.setShortcut(QKeySequence(sequence))
        self.addAction(act)

    @property
    def default_dock_widget_area(self):
        """Return where this widget should be placed by default, relative to
        the dock widget it belongs to.

        Returns:
            int
        """
        return Qt.TopDockWidgetArea

    @property
    def id(self):
        return self._id

    def set_title(self, text):
        self.__label.setText(text)

    def __float_changed(self, value):
        self.setFloating(value)


class ClosablePanel(Panel):

    _CLOSE = "close.png"

    def __init__(self, parent):
        super(ClosablePanel, self).__init__(parent)

    def _add_toolbar_actions(self):
        self._toolbar.addAction(QIcon(resources.get(self._CLOSE)), "Close", self.__close)
        super(ClosablePanel, self)._add_toolbar_actions()

    def __close(self):
        self.on_close()
        self.panel_closed.emit(self)

    def on_close(self):
        pass
