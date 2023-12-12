from conversation.ui.events import Events


_black = (0, 0, 0)
_white = (255, 255, 255)
_red = (255, 0, 0)
_green = (0, 255, 0)
_blue = (0, 0, 255)
_purple = (255, 0, 255)
_yellow = (0, 255, 255)


COLOUR_DEFAULT = _black
COLOUR_ANTI_DEFAULT = _white  # the opposite of whatever default is
COLOUR_SELECTED = _red  # selected node
COLOUR_BEGIN = _green  # entry node


def enable_dark_theme():
    global COLOUR_DEFAULT
    global COLOUR_ANTI_DEFAULT

    COLOUR_DEFAULT = _white
    COLOUR_ANTI_DEFAULT = _black

    Events.Refresh.emit()


def enable_light_theme():
    global COLOUR_DEFAULT
    global COLOUR_ANTI_DEFAULT

    COLOUR_DEFAULT = _black
    COLOUR_ANTI_DEFAULT = _white

    Events.Refresh.emit()
