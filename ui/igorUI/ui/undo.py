from conversation.ui.events import Events


class _UndoManager(object):
    """Trivial undo service manager.

    Essentially you add items to the undo stack by calling 'add undo' and
    supplying a name (for display purposes) and a list of partial functions
    that would effectively undo the given action if called.

    At some point, if the user chooses to call "undo" we pop off the top action
    and iterate through the partial funcs, calling them one at a time.
    """
    def __init__(self):
        self._stack = []

    @property
    def size(self):
        """Return the current size of the stack.

        Returns:
            int
        """
        return len(self._stack)

    @property
    def last_action_name(self):
        """Returns the given 'name' string for the last action recorded here
        (ie, the first to be undone)

        Returns:
            str or None
        """
        if not self._stack:
            return None

        return self._stack[-1][0]

    def undo(self):
        """Pop off the last action & call the given partial funcs.
        """
        if not self._stack:
            return

        name, funcs = self._stack.pop()
        for func in funcs:
            try:
                func()
            except Exception as e:
                print("Undo action '%s' failed, aborting undo sequence. %s" % (name, e))
                return

        nxt = self.last_action_name
        if nxt:
            Events.Status.emit(f"Control+Z: Undo '{nxt}'")
        else:
            Events.Status.emit("Undo stack empty")

    def add_undo(self, name, partials):
        """Add a new undo to the stack.

        Args:
            name (str): arbitrary name that explains to user what this undo does
            partials ([]functools.partial): list of funcs that would effect undo

        """
        self._stack.append((name, partials))
        Events.Status.emit(f"Control+Z: Undo '{name}'")


Undo = _UndoManager()
