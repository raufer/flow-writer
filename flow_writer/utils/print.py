import sys
import os
from colorama import init
from termcolor import colored

init()


def supports_color():
    """
    Returns True if the running system's terminal supports color, and False otherwise.
    """
    plat = sys.platform
    supported_platform = plat != 'Pocket PC' and (plat != 'win32' or 'ANSICON' in os.environ)

    is_a_tty = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()

    if not supported_platform or not is_a_tty:
        return False

    return True


def color_text(text, color=None, attrs=None):
    """
    Use ANSI escapes to color text, special sequences of characters which terminals process to switch font styles.
    https://en.wikipedia.org/wiki/ANSI_escape_code

    We should first check if the calling system supports ANSI escapes
    """
    if not attrs:
        attrs = ['bold']

    colored_text = colored(text, color, attrs=attrs)
    return colored_text

