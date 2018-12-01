"""The manifest module imports all of the PyQt bits we need.
"""

from PyQt5.Qt import Qt
from PyQt5.QtCore import QObject, QSize, QPoint, QRect
from PyQt5.QtCore import pyqtSignal as Signal
from PyQt5.QtWidgets import (
    QWidget,
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
    QCheckBox,
)
from PyQt5.QtGui import (
    QKeySequence,
    QPen,
    QColor,
    QPainter,
    QIcon,
)


__all__ = [
    "Qt",
    "QRect",
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
