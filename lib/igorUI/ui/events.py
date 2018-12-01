
import inspect


from conversation.ui.manifest import Signal, QObject


class _EventManager(QObject):
    """A place to hold global / public events that are used to co-ordinate
    panels.
    """

    # force refresh of widgets
    Refresh = Signal()

    # flush is a signal emitted when the user wishes to save or exit
    Flush = Signal()

    # a node on the graph is selected
    NodeSelected = Signal(object)

    # save shortcut
    Save = Signal()

    # set main window status
    Status = Signal(str)


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
