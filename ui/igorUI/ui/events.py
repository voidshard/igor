
import time
import inspect
import threading

from igorUI.ui.manifest import Signal, QObject


_REFRESH_INTERVAL = 5


class _EventManager(QObject):
    """A place to hold global / public events that are used to co-ordinate
    panels.
    """

    # set main window status
    Status = Signal(str)

    # tells listening things to refresh their data
    Refresh = Signal()

    # selected job
    JobSelected = Signal(str)

    # selected layer
    LayerSelected = Signal(str)

    # selected class & id
    OpenDetails = Signal(str, str)


Events = _EventManager()


def __log(name):
    """This log wrapper allows us to print the event name (signal name) in our generic logger
    function.

    :param name: name of event
    :return: function
    """

    def __fn(*args, **kwargs):
        """Log all signals sent through the event manager.

        Args:
            *args:
            **kwargs:
        """
        try:
            if name != "Refresh":
                # printing Refresh every few seconds is annoying rather than helpful usually
                print("[event] %s %s %s" % (name, args, kwargs))
        except Exception:  # never raise on failure to log an event
            pass
    return __fn


def __init():
    """Just for the sake of verbosity we can bind to signals so we can log
    them all.
    """
    sigs = [i for i in inspect.getmembers(Events) if "signal" in i[1].__class__.__name__.lower()]

    for name, signal in sigs:  # bind to every signal and log it's use.
        signal.connect(__log(name))


__init()


def _fire_refresh():
    while True:
        Events.Refresh.emit()
        time.sleep(_REFRESH_INTERVAL)


_thread = threading.Thread(target=_fire_refresh)
_thread.setDaemon(True)
_thread.setName("tick-tock")
_thread.start()
