from typing import Iterator

import dask
from zeep.helpers import serialize_object

from spinta import commands
from spinta.components import Context, Property, Model
from spinta.core.ufuncs import Expr
from spinta.datasets.backends.dataframe.backends.soap.components import Soap
from spinta.datasets.backends.dataframe.commands.read import (
    parametrize_bases,
    get_dask_dataframe_meta,
    dask_get_all
)
from spinta.datasets.backends.dataframe.ufuncs.query.components import DaskDataFrameQueryBuilder
from spinta.datasets.utils import iterparams
from spinta.dimensions.param.components import ResolvedParams
from spinta.exceptions import SoapRequestBodyParseError
from spinta.typing import ObjectData
from spinta.ufuncs.querybuilder.components import QueryParams


def _get_data_soap(url: str, backend: Soap, soap_request: dict) -> list[dict]:
    return serialize_object(backend.soap_operation(**soap_request), target_cls=dict)


def _check_key(key: str) -> None:
    if not key:  # In case if split is empty string
        raise ValueError(f'Invalid dictionary key "{key}"')


def _set_intermediate_dict(dictionary: dict, key) -> dict:
    """
    Intermediate dict is any non-last part of key.
    Example: In {a/b/c/d: "foo"} -> a, b, c will become intermediate dicts

    If intermediate dict already has non-dict value set it means that given data is incorrect
    """
    _check_key(key)

    if not isinstance(dictionary.get(key, {}), dict):
        raise ValueError(f'Cannot set key {{"{key}": {{}}}} because it already exists in: {dictionary}')

    return dictionary.setdefault(key, {})


def _set_dict_value(dictionary: dict, key: str, value: str) -> None:
    _check_key(key)

    if dictionary.get(key):
        raise ValueError(f'Cannot set value {{"{key}": "{value}"}} because it already exists in : {dictionary}')
    dictionary[key] = value


def _expand_dict_keys(target: dict, separator: str = "/") -> dict:
    """
    Function splits target dictionary keys into separate dictionaries, if key has separator.
    Example: {"a/b/c": "value"} -> {"a": {"b": "c": "value"}}
             {"a/b": "value1", "a/c": "value2"} -> {"a": {"b": "value1", "c": "value2"}}

    Raises ValueError if target dictionary is incorrect.
    Example: {"a//b/": "value"} <- dictionary key cannot be empty string
             {"a": "value1", "a/b": "value2"} <- key "a" cannot be "value1" and {"b": "value2"} at the same time
    """
    result = {}

    for key, value in target.items():
        parts = key.split(separator)

        current = result
        for part in parts[:-1]:
            current = _set_intermediate_dict(current, part)

        _set_dict_value(current, parts[-1], value)

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
    **kwargs
) -> Iterator[ObjectData]:
    bases = parametrize_bases(
        context,
        model,
        model.external.resource,
        resolved_params
    )
    bases = list(bases)

    resource_params = next(iterparams(context, model, model.manifest, model.external.resource.params))

    builder = backend.query_builder_class(context)
    builder = builder.init(backend=backend, model=model, resource_params=resource_params, query_params=params)
    query = builder.resolve(query)
    builder.build()

    try:
        soap_request = _expand_dict_keys(builder.soap_request_body)
    except ValueError as err:
        raise SoapRequestBodyParseError(err)

    meta = get_dask_dataframe_meta(model)
    df = dask.bag.from_sequence(bases).map(
        _get_data_soap, backend=backend, soap_request=soap_request,
    ).flatten().to_dataframe(meta=meta)

    df_builder = DaskDataFrameQueryBuilder(context)
    df_builder.update(model=model)

    yield from dask_get_all(context, query, df, backend, model, df_builder, extra_properties, builder.property_values)
