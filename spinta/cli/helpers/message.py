import tqdm
from click import echo
from click.exceptions import Exit


def cli_error(message: str):
    echo(message, err=True)
    raise Exit(code=1)


def cli_message(message: str, progress_bar: tqdm.tqdm = None):
    if progress_bar is not None:
        progress_bar.write(message)
    else:
        echo(message)
