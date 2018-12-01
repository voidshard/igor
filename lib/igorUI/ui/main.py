import functools
import os
import sys

from conversation.ui.manifest import *
from conversation.ui.widgets import Canvas, Properties, Editor, Actions, Conditions
from conversation.ui.events import Events
from conversation.ui import resources
from conversation.ui.undo import Undo
from conversation.ui import constants as consts
from conversation.backend import FilesystemStorage


class MainWindow(QMainWindow):
    """MainWindow class holds all panels & main menubar
    """

    _NAME = "Igor"
    _ICON = "main_icon.jpg"

    def __init__(self, app):
        super(MainWindow, self).__init__()
        self.__parent_app = app

        # ui fluff & defaults
        self.setWindowIcon(QIcon(resources.get(self._ICON)))
        self.setWindowTitle(self._NAME)
        self.setDockOptions(
            self.AnimatedDocks | self.AllowNestedDocks | self.AllowTabbedDocks | self.VerticalTabs
        )
        self.setContextMenuPolicy(Qt.NoContextMenu)
        self.resize(1024, 800)

        # internal vars
        self._panels = {}

        # throw up our menubar
        # self.__init_menubar()

        # add main widget
        self.__view = Canvas(self)
        self.add_panel(self.__view)

        # allow events to set the status bar text
        Events.Status.connect(self.__set_status)
        self.__set_status("ready")

    def __set_status(self, text):
        """

        :param text:

        """
        self.statusBar().showMessage(text)

    def add_panel(self, wgt):
        """

        :param wgt:

        """
        type_ = wgt.__class__.__name__

        if self._panels.get(type_):
            print(f"Panel of type {type_} already open")
            return

        self.addDockWidget(wgt.default_dock_widget_area, wgt)
        self._panels[type_] = wgt

        # make sure we are alerted when this panel is closed
        wgt.panel_closed.connect(self.__close_panel)

    def __close_panel(self, wgt):
        """

        :param wgt:
        """
        try:
            del self._panels[wgt.__class__.__name__]
        except KeyError:
            pass

        self.removeDockWidget(wgt)
        wgt.destroy()

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

    def __init_menubar(self):
        """Create & add default menu w/ options & actions

        """
        pass
        # self.__menubar = QMenuBar()  # Setup menu bar
        # self.__menubar.setNativeMenuBar(False)
        # self.setMenuBar(self.__menubar)
        #
        # menu_file = self.__menubar.addMenu("File")
        #
        # action = QAction("Save", self)
        # action.triggered.connect(self.__action_save_as)
        # menu_file.addAction(action)
        #
        # action = QAction("Save As ...", self)
        # action.triggered.connect(self.__action_save)
        # menu_file.addAction(action)
        #
        # menu_file.addSeparator()
        #
        # action = QAction("Open ...", self)
        # action.triggered.connect(self.__action_load)
        # menu_file.addAction(action)
        #
        # menu_edit = self.__menubar.addMenu("Edit")
        #
        # action = QAction("Undo", self)
        # action.triggered.connect(functools.partial(Undo.undo))
        # menu_edit.addAction(action)
        #
        # action = QAction("Refresh", self)
        # action.triggered.connect(functools.partial(self.__trigger_flush_refresh))
        # menu_edit.addAction(action)
        #
        # menu_view = self.__menubar.addMenu("View")
        #
        # action = QAction("Properties", self)
        # action.triggered.connect(self.__action_open_properies)
        # menu_view.addAction(action)
        #
        # action = QAction("Copy Editor", self)
        # action.triggered.connect(self.__action_open_editor)
        # menu_view.addAction(action)
        #
        # action = QAction("Conditions", self)
        # action.triggered.connect(self.__action_open_conditions)
        # menu_view.addAction(action)
        #
        # action = QAction("Actions", self)
        # action.triggered.connect(self.__action_open_actions)
        # menu_view.addAction(action)
        #
        # menu_view.addSeparator()
        #
        # action = QAction("Dark Theme", self)
        # action.triggered.connect(functools.partial(consts.enable_dark_theme))
        # menu_view.addAction(action)
        #
        # action = QAction("Light Theme", self)
        # action.triggered.connect(functools.partial(consts.enable_light_theme))
        # menu_view.addAction(action)
        #
        # menu_help = self.__menubar.addMenu("Help")
        # action = QAction("About", self)
        # action.triggered.connect(self.__action_about)
        # menu_help.addAction(action)


def start():
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    app.setStyle(QStyleFactory.create("gtk"))

    screen = MainWindow(app)
    screen.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    start()
