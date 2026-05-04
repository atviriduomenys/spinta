import sys

import tqdm
from click import echo
from click.exceptions import Exit


def cli_error(message: str):
    echo(message, err=True)
    raise Exit(code=1)


def cli_warning(message: str):
    echo(f"Warning: {message}", err=True)


def cli_message(message: str, progress_bar: tqdm.tqdm = None):
    # https://pubs.opengroup.org/onlinepubs/9799919799/
    # This documentation states that `stderr` should be used for diagnostic messages (in our case status)
    if progress_bar is not None:
        progress_bar.write(message, file=sys.stderr)
    else:
        echo(message, err=True)
