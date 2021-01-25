from typing import Callable
from typing import Union

from typer import Typer


def add(app: Typer, name: str, item: Union[Callable, Typer], **kwargs):
    if isinstance(item, Typer):
        app.add_typer(item, name=name, **kwargs)
    else:
        app.command(name, **kwargs)(item)
