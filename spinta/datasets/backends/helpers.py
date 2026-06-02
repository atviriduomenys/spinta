import pathlib
from typing import Any

from spinta import commands, spyna
from spinta.components import Property, Model, Context
from spinta.core.ufuncs import asttoexpr
from spinta.exceptions import MultiplePrimaryKeyCandidatesFound, NoPrimaryKeyCandidatesFound
from spinta.types.datatype import Ref


def generate_ref_id_using_select(context: Context, dtype: Ref, data: dict) -> str:
    ref_model = dtype.model
    expr_parts = ["select()"]
    for prop in dtype.refprops:
        expr_parts.append(f'{prop.place}="{data[prop.name]}"')
    expr = asttoexpr(spyna.parse("&".join(expr_parts)))
    rows = commands.getall(context, ref_model, ref_model.backend, query=expr)

    found_value = False
    val = None
    for row in rows:
        if val is not None:
            raise MultiplePrimaryKeyCandidatesFound(dtype, values=data)
        val = row["_id"]
        found_value = True

    if not found_value:
        raise NoPrimaryKeyCandidatesFound(dtype, values=data)

    return val


def extract_values_from_row(row: Any, model: Model, keys: list):
    return_list = []
    for key in keys:
        if isinstance(key, str):
            key = model.properties[key]
        if isinstance(key, Property):
            if key.external and key.external.name:
                key = key.external.name
            else:
                key = key.name

        return_list.append(row[key])
    if len(return_list) == 1:
        return_list = return_list[0]
    return return_list


def flatten_keymap_encoding_values(data: object):
    # Backwards compatibility function, that converts nested dict values to flat list values
    # Used for generating and looking up keymap values
    if isinstance(data, dict):
        if len(data) == 1:
            return flatten_keymap_encoding_values(list(data.values())[0])
        return flatten_keymap_encoding_values(list(data.values()))
    elif isinstance(data, (list, tuple)):
        return list(flatten_keymap_encoding_values(item) for item in data)
    return data


def is_file_path(path: str) -> bool:
    try:
        return pathlib.Path(path).is_file()
    except OSError:
        return False
