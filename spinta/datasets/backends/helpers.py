import pathlib
from typing import Any, Iterator

from spinta import commands, spyna
from spinta.auth import authorized
from spinta.components import Context, Model, Property
from spinta.core.enums import Action
from spinta.core.ufuncs import asttoexpr
from spinta.exceptions import MultiplePrimaryKeyCandidatesFound, NoPrimaryKeyCandidatesFound
from spinta.types.datatype import Ref


def generate_ref_id_using_select(context: Context, dtype: Ref, data: dict) -> str | None:
    def _find_required_value(rows_: Iterator) -> object | None:
        found_value = False
        result_value = None
        for row in rows_:
            if result_value is not None:
                raise MultiplePrimaryKeyCandidatesFound(dtype, values=data)
            result_value = row.get("_id")
            found_value = True

        if not found_value:
            raise NoPrimaryKeyCandidatesFound(dtype, values=data)

        return result_value

    ref_model = dtype.model

    has_permission = False
    # Check permissions if able to use select with specific scenarios
    # Check if user can select only _id
    select_query = "select(_id)"
    if authorized(context, ref_model.id_prop, Action.SEARCH):
        has_permission = True
    # Check if user has getall permission
    elif authorized(context, ref_model.id_prop, Action.GETALL):
        has_permission = True
        select_query = "select()"

    expr_parts = [select_query]
    for prop in dtype.refprops:
        expr_parts.append(f'{prop.place}="{data[prop.place]}"')
    expr = asttoexpr(spyna.parse("&".join(expr_parts)))

    # User does not have permission to select _id, so we need to use admin context
    if not has_permission:
        with context.fork("_id_select") as admin_context:
            admin_context.set("request.system", True)
            rows = commands.getall(admin_context, ref_model, ref_model.backend, query=expr)
            # Have to call _find_required_value, since we need to be inside the admin context
            val = _find_required_value(rows)
    else:
        rows = commands.getall(context, ref_model, ref_model.backend, query=expr)
        val = _find_required_value(rows)

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
