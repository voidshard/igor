"""The manifest module imports all of the PyQt bits we need.
"""

from PyQt5.Qt import (
    Qt,
    QRegExp,
    QSortFilterProxyModel,
    QAbstractTableModel,
    QMenu,
    QModelIndex,
)
from PyQt5.QtCore import (
    QObject,
    QSize,
    QPoint,
    QRect,
)
from PyQt5.QtCore import pyqtSignal as Signal
from PyQt5.QtWidgets import (
    QWidget,
    QComboBox,
    QFileDialog,
    QTextEdit,
    QLineEdit,
    QApplication,
    QStyleFactory,
    QMainWindow,
    QMenuBar,
    QAction,
    QMessageBox,
    QToolBar,
    QLabel,
    QDockWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QTableView,
    QCheckBox,
    QDialog,
    QPushButton,
)
from PyQt5.QtGui import (
    QKeySequence,
    QPen,
    QColor,
    QPainter,
    QIcon,
)


__all__ = [
    "QComboBox",
    "QDialog",
    "QPushButton",
    "QMenu",
    "QModelIndex",
    "QAbstractTableModel",
    "QSortFilterProxyModel",
    "QRegExp",
    "Qt",
    "QRect",
    "QTableView",
    "QFileDialog",
    "QCheckBox",
    "QTextEdit",
    "QLineEdit",
    "QPoint",
    "QDockWidget",
    "QLabel",
    "QHBoxLayout",
    "QVBoxLayout",
    "QGridLayout",
    "QObject",
    "QSize",
    "QToolBar",
    "QIcon",
    "Signal",
    "QWidget",
    "QApplication",
    "QStyleFactory",
    "QMainWindow",
    "QMenuBar",
    "QAction",
    "QKeySequence",
    "QMessageBox",
    "QPen",
    "QColor",
    "QPainter",
]
