import functools
import sys

from igorUI import utils
from igorUI.ui import resources
from igorUI.ui.manifest import *
from igorUI.ui.widgets.jobs import JobPanel
from igorUI.ui.widgets.layers import LayerPanel
from igorUI.ui.widgets.tasks import TaskPanel
from igorUI.ui.widgets.details_panel import (
    JobDetailsPanel,
    LayerDetailsPanel,
    TaskDetailsPanel,
)
from igorUI.ui.events import Events


logger = utils.logger()


class MainWindow(QMainWindow):
    """MainWindow class holds all panels & main menubar
    """

    _NAME = "Igor"
    _ICON = "main_icon.png"

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

        # add main widgets
        self.add_panel(JobPanel(self))
        self.add_panel(LayerPanel(self))
        self.add_panel(TaskPanel(self))

        self._add_shortcut("quit", "Ctrl+q", functools.partial(self.__exit))
        self._add_shortcut("jobs", "Ctrl+j", functools.partial(self.__open_panel, JobPanel))
        self._add_shortcut("layers", "Ctrl+l", functools.partial(self.__open_panel, LayerPanel))
        self._add_shortcut("tasks", "Ctrl+t", functools.partial(self.__open_panel, TaskPanel))

        Events.OpenDetails.connect(self.__open_details)

        # allow events to set the status bar text
        Events.Status.connect(self.__set_status)
        self.__set_status("ready")

    def __exit(self):
        sys.exit(0)

    def __open_details(self, classname, obj_id):
        """

        :param classname:
        :param obj_id:

        """
        panel = None
        if classname == "job":
            panel = JobDetailsPanel(self, obj_id)
        elif classname == "layer":
            panel = LayerDetailsPanel(self, obj_id)
        elif classname == 'task':
            panel = TaskDetailsPanel(self, obj_id)

        if not panel:
            self.__set_status(f"details panel unknown for class {classname} with id {obj_id}")
            return

        self.add_panel(panel, name=f"{classname}:{obj_id}")
        panel.refresh()

    def __open_panel(self, cls):
        self.add_panel(cls(self))

    def __set_status(self, text):
        """

        :param text:

        """
        logger.info(f"status: {text}")
        self.statusBar().showMessage(text)

    def add_panel(self, wgt, name=None):
        """

        :param wgt:
        :param name:

        """
        type_ = name or wgt.__class__.__name__
        if self._panels.get(type_):
            Events.Status.emit(f"Panel {type_} already open")
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


def start():
    app = QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    app.setStyle(QStyleFactory.create("gtk"))

    screen = MainWindow(app)
    screen.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    start()
