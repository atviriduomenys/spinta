import json

from typing import Union, List

from spinta.dispatcher import command
from spinta.components import Context
from spinta.types.dataset import Model, Property
from spinta.fetcher import fetch


@command()
def read_json():
    pass


@read_json.register()
def read_json(context: Context, model: Model, *, source=Union[str, List[str]], dependency: dict, items: str):
    urls = source if isinstance(source, list) else [source]
    for url in urls:
        url = url.format(**dependency)
        with fetch(context, url, text=True).open() as f:
            data = json.load(f)
        data = data[items]
        if isinstance(data, list):
            yield from data
        else:
            yield data


@read_json.register()
def read_json(context: Context, prop: Property, *, source: str, value: dict):
    return value.get(source)
