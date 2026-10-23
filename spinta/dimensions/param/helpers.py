from typing import Any

from spinta import commands, spyna
from spinta.components import Context, Model
from spinta.core.ufuncs import Expr, asttoexpr
from spinta.datasets.components import Dataset, Param, Resource
from spinta.dimensions.param.components import ParamLoader, ParamValues
from spinta.exceptions import QueryParamNotFound, ResourceSourceQueryConflict, UnlinkedQueryParam
from spinta.manifests.components import Manifest
from spinta.nodes import load_node
from spinta.ufuncs.linkbuilder.components import LinkBuilder
from spinta.ufuncs.loadbuilder.components import LoadBuilder
from spinta.utils.schema import NA


def load_param_formulas_and_sources(
    context: Context,
    param: Param,
    prepare_asts: list[dict],
    sources: list[str],
    resource: Resource | None = None,
) -> None:
    param_formulas = []
    param_sources = []
    builder = LoadBuilder(context)
    for source, ast in zip(sources, prepare_asts):
        builder.update(this=source, param=param, resource=resource)

        # If possible - resolve formulas with LoadBuilder
        expr = asttoexpr(ast)
        if isinstance(expr, Expr):
            if formula := builder.resolve(expr):
                param_formulas.append(formula)
                param_sources.append(source)
        else:
            param_formulas.append(expr)
            param_sources.append(source)

    param.formulas = param_formulas
    param.sources = param_sources


def load_params(context: Context, manifest: Manifest, param_data: Any, resource: Resource | None = None) -> list[Param]:
    params = []
    if isinstance(param_data, dict):
        for key, data in param_data.items():
            param = Param()
            load_node(context, param, data)
            param.name = data["name"]
            load_param_formulas_and_sources(context, param, data["prepare"], data["source"].copy(), resource)
            params.append(param)
    elif isinstance(param_data, list):
        for item in param_data:
            if isinstance(item, dict) and len(item.keys()) == 1:
                param = Param()
                name = list(item.keys())[0]
                prepare = item[name]
                if isinstance(prepare, str):
                    prepare = spyna.unparse(prepare)
                data = {"name": name, "source": [NA], "prepare": [prepare], "title": "", "description": ""}
                load_node(context, param, data)
                param.name = data["name"]
                load_param_formulas_and_sources(context, param, data["prepare"], data["source"].copy(), resource)
                params.append(param)

    return params


def link_params(context: Context, manifest: Manifest, params: list[Param], dataset: Dataset = None):
    param_loader = ParamLoader(context)
    param_loader.update(manifest=manifest, dataset=dataset)
    param_loader.resolve(Expr("resolve_param", params))


def finalize_param_link(context, manifest: Manifest):
    datasets = commands.get_datasets(context, manifest)
    for dataset in datasets.values():
        for resource in dataset.resources.values():
            if resource:
                for param in resource.params:
                    for source in param.sources:
                        if isinstance(source, Model):
                            if source.external and source.external.resource:
                                param.dependencies.update(source.external.resource.source_params)
                _link_property_params(context, resource)


def _link_property_params(context: Context, resource: Resource) -> None:
    """Link source-less properties with `param()` prepare formula to the resource params."""
    builder = LinkBuilder(context, resource, resource.dataset, this=resource)
    for model in resource.models.values():
        for prop in model.properties.values():
            if not prop.external or prop.external.name:
                continue
            prepare = prop.external.prepare
            if isinstance(prepare, Expr) and prepare.name == "param":
                builder(this=prop).resolve(prepare)


def get_param_props(params: Any) -> list:
    """Request-time `ParamProperty` values from `QueryParams`, empty if there are none."""
    return list(getattr(params, "param_props", None) or [])


def _url_overrides(context: Context, model: Model, param_props: list | None) -> dict[str, Any]:
    """Values given in the request URL for source-less properties linked to a param, keyed by `query_name`."""
    values: dict[str, Any] = {}
    env = ParamValues(context)
    env.update(values=values)
    env.call("and", list(param_props or []))
    overrides: dict[str, Any] = {}
    for prop in model.properties.values():
        if prop.param is None or prop.param.query_name is None:
            continue
        if prop.name in values:
            overrides[prop.param.query_name] = values[prop.name]
    return overrides


def check_resource_source_query_conflicts(resource: Resource) -> None:
    """Raise if `resource.source` URL and a `query()` param define the same query key (#2066)."""
    conflicts = sorted(resource.url.source_query & resource.url.query.keys())
    if conflicts:
        raise ResourceSourceQueryConflict(resource, param=conflicts[0])


def check_query_params_have_values(resource: Resource) -> None:
    """Raise if a `query()` param without default is not linked to any source-less property (#2066).

    Such a param can only get its value from the request URL through a property linked with `param()`. Without
    a link the value can never be provided, so the manifest is invalid.
    """
    linked = {
        prop.param.query_name
        for model in resource.models.values()
        for prop in model.properties.values()
        if prop.param is not None and prop.param.query_name is not None
    }
    for name, default in resource.url.query.items():
        if default is NA and name not in linked:
            raise UnlinkedQueryParam(resource, param=name)


def resolve_url_query_params(
    context: Context, model: Model, resource: Resource, param_props: list | None = None
) -> dict[str, Any]:
    """Outgoing query parameters (`source` -> value) of `resource` for a request on `model`.

    Defaults come from `resource.url.query`, stored on load by `query()` formulas. A value given in the request URL
    for a source-less property linked to a param (`Property.param`, set on link) overrides the default. A `query()`
    without default and without a URL value raises `QueryParamNotFound`.
    """
    overrides = _url_overrides(context, model, param_props)

    values: dict[str, Any] = {}
    for name, default in resource.url.query.items():
        value = overrides.get(name, default)
        if value is NA:
            raise QueryParamNotFound(param=name, reason="value is missing in the request URL")
        values[name] = value
    return values


def resolve_linked_param_values(
    context: Context, model: Model, resource: Resource, param_props: list | None = None
) -> dict[str, Any]:
    """Values of resource params linked to source-less properties (`param()`), keyed by param name.

    Uses the same values as `resolve_url_query_params()`, so rows returned to the client show the value that was
    sent to the data source (URL value if given, otherwise `query()` default).
    """
    query_values = resolve_url_query_params(context, model, resource, param_props)
    values: dict[str, Any] = {}
    for prop in model.properties.values():
        if prop.param is None or prop.param.query_name is None:
            continue
        if prop.param.query_name in query_values:
            values[prop.param.name] = query_values[prop.param.query_name]
    return values
