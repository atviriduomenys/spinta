from typing import Iterator

import dask
from zeep.helpers import serialize_object

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.soap.components import Soap
from spinta.datasets.backends.dataframe.commands.read import parametrize_bases, get_dask_dataframe_meta, dask_get_all
from spinta.datasets.backends.dataframe.ufuncs.query.components import DaskDataFrameQueryBuilder
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import SoapRequestBodyParseError
from spinta.typing import ObjectData
from spinta.ufuncs.querybuilder.components import QueryParams


def _get_data_soap(url: str, backend: Soap, soap_request: dict) -> list[dict]:
    response_data = serialize_object(backend.soap_operation(**soap_request), target_cls=dict)

    return response_data or []


def _check_empty_key(key: str) -> None:
    if not key:
        raise ValueError(f'Invalid dictionary key "{key}"')


def _add_dict_from_key_part(dictionary: dict, key: str) -> dict:
    """
    If key not in dictionary: add "key": {} to dictionary and return key value
    If key is in dictionary: return key value

    Raise error if key is empty or key value is not dict
    """
    _check_empty_key(key)

    if not isinstance(dictionary.get(key, {}), dict):
        raise ValueError(f'Cannot set key {{"{key}": {{}}}} because it already exists in: {dictionary}')

    return dictionary.setdefault(key, {})


def _set_dict_value(dictionary: dict, key: str, value: str) -> None:
    _check_empty_key(key)

    if key in dictionary.keys():
        raise ValueError(f'Cannot set value {{"{key}": "{value}"}} because it already exists in : {dictionary}')
    dictionary[key] = value


def _expand_dict_keys(target: dict, separator: str = "/") -> dict:
    """
    Function splits target dictionary keys into separate dictionaries, if key has separator.
    Example: {"a/b/c": "value"} -> {"a": {"b": {"c": "value"}}}
             {"a/b": "value1", "a/c": "value2"} -> {"a": {"b": "value1", "c": "value2"}}

    Raises ValueError if target dictionary is incorrect.
    Example: {"a//b/": "value"} <- dictionary key cannot be empty string
             {"a": "value1", "a/b": "value2"} <- key "a" cannot be "value1" and {"b": "value2"} at the same time
    """
    result = {}

    for key, value in target.items():
        key_parts = key.split(separator)

        # Gets/Creates dictionaries from every key part, except the last one
        current = result
        for part in key_parts[:-1]:
            current = _add_dict_from_key_part(current, part)

        # Sets value on the last key part
        _set_dict_value(current, key_parts[-1], value)

    return result


@commands.getall.register(Context, Model, Soap)
def getall(
    context: Context,
    model: Model,
    backend: Soap,
    *,
    query: Expr = None,
    resolved_params: ResolvedParams = None,
    extra_properties: dict[str, Property] = None,
    params: QueryParams,
    **kwargs,
) -> Iterator[ObjectData]:
    bases = parametrize_bases(context, model, model.external.resource, resolved_params)
    bases = list(bases)

    builder = backend.query_builder_class(context)
    builder = builder.init(backend=backend, model=model, query_params=params)
    query = builder.resolve(query)
    builder.build()

    try:
        soap_request = _expand_dict_keys(builder.soap_request_body)
    except ValueError as err:
        raise SoapRequestBodyParseError(err)

    meta = get_dask_dataframe_meta(model)
    df = (
        dask.bag.from_sequence(bases)
        .map(
            _get_data_soap,
            backend=backend,
            soap_request=soap_request,
        )
        .flatten()
        .to_dataframe(meta=meta)
    )

    dask_dataframe_query_builder = DaskDataFrameQueryBuilder(context)
    dask_dataframe_query_builder.update(model=model)

    yield from dask_get_all(
        context, query, df, backend, model, dask_dataframe_query_builder, extra_properties, builder.property_values
    )
