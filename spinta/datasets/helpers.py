from typing import Any
from typing import List
from typing import Optional
from typing import Tuple
from typing import Set

from spinta import exceptions
from spinta.auth import authorized
from spinta.backends import Backend
from spinta.backends.components import BackendOrigin
from spinta.backends.helpers import load_backend
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ShortExpr
from spinta.datasets.components import Resource
from spinta.dimensions.enum.helpers import get_prop_enum
from spinta.types.datatype import Ref
from spinta.ufuncs.changebase.helpers import change_base_model
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.helpers import merge_formulas
from spinta.utils.data import take
from spinta.utils.naming import to_code_name


def load_resource_backend(
    context: Context,
    resource: Resource,
    name: Optional[str],  # Backend name given in `ref` column.
) -> Backend:
    if name:
        # If backend name is given, just use an exiting backend.
        store = resource.dataset.manifest.store
        possible_backends = [
            # Backend defined in manifest.
            resource.dataset.manifest.backends,
            # Backend defined via configuration.
            store.backends,
        ]
        for backends in possible_backends:
            if name in backends:
                return backends[name]
        raise exceptions.BackendNotFound(resource, name=name)

    elif resource.type or resource.external:
        # If backend is defined inline on resource itself, try to load it.
        name = f'{resource.dataset.name}/{resource.name}'
        return load_backend(context, resource, name, BackendOrigin.resource, {
            'type': resource.type,
            'name': name,
            'dsn': resource.external,
        })

    else:
        # If backend was not given, try to check configuration with resource and
        # dataset names as backend names or fallback to manifest backend.
        store = resource.dataset.manifest.store
        possible_backends = [
            # Dataset and resource name combination.
            f'{resource.dataset.name}/{resource.name}',
            # Just dataset name.
            resource.dataset.name,
            # Just resource type.
            # XXX: I don't think that using resource type as a backend name is a
            #      good idea. Probably should be removed.
            resource.type,
        ]
        for name in filter(None, possible_backends):
            name = to_code_name(name)
            if name in store.backends:
                return store.backends[name]
        return resource.dataset.manifest.backend


def get_enum_filters(
    context: Context,
    model: Model,
    fpr: ForeignProperty = None,
) -> Optional[Expr]:
    args: List[Expr] = []
    for prop in take(['_id', all], model.properties).values():
        if (
            (enum := get_prop_enum(prop)) and
            authorized(context, prop, Action.GETALL)
        ):
            has_lower_access = any(
                item.access < prop.access
                for item in enum.values()
                if item.access is not None
            )
            if has_lower_access:
                values = []
                for item in enum.values():
                    if item.access is not None and item.access >= prop.access:
                        values.append(item.prepare)

                if fpr:
                    dtype = fpr.push(prop)
                else:
                    dtype = prop.dtype

                if len(values) == 1:
                    arg = values[0]
                elif len(values) > 1:
                    arg = values
                else:
                    arg = None

                if arg is not None:
                    expr = ShortExpr('eq', dtype.get_bind_expr(), arg)
                    args.append(expr)
    if len(args) == 1:
        return args[0]
    elif len(args) > 1:
        return ShortExpr('and', *args)
    else:
        return None


def isexpr(
    obj: Any,
    names: Set[str],
    *,
    nargs: Optional[int] = None,
):
    return (
        isinstance(obj, Expr) and
        obj.name in names and
        (nargs is None or len(obj.args) == nargs)
    )


def add_is_null_checks(expr: Expr) -> Expr:
    if isexpr(expr, {'eq'}, nargs=2):
        lhs, rhs = expr.args
        if isexpr(lhs, {'getattr'}, nargs=2) and rhs is not None:
            if isexpr(lhs.args[0], {'bind'}):
                return Expr('group', ShortExpr('or', ShortExpr('eq', lhs, None), expr))
    if isexpr(expr, {'getattr'}, nargs=2):
        if isexpr(expr.args[0], {'bind'}):
            return Expr('group', ShortExpr('or', ShortExpr('eq', expr, None), expr))
    if isexpr(expr, {'and'}):
        return ShortExpr('and', *(
            add_is_null_checks(arg)
            for arg in expr.args
        ))
    return expr


def get_ref_filters(
    context: Context,
    model: Model,
    fpr: ForeignProperty = None,
    *,
    seen: List[Tuple[
        str,  # absolute model name
        str,  # model property
    ]] = None,
) -> Optional[Expr]:
    query: Optional[Expr] = None
    if fpr:
        query = merge_formulas(
            query,
            add_is_null_checks(change_base_model(context, model, fpr)),
        )
        query = merge_formulas(
            query,
            get_enum_filters(context, model, fpr),
        )

    seen = seen or []

    for prop in take(['_id', all], model.properties).values():
        if prop.external is None or not prop.external.name:
            # Do not include properties, that has no external source, we can't
            # query data if there is no source.
            continue

        ref_key = (model.name, prop.name)
        if (
            isinstance(prop.dtype, Ref) and
            ref_key not in seen and
            authorized(context, prop, Action.GETALL)
        ):
            if fpr:
                _fpr = fpr.push(prop)
            else:
                _fpr = ForeignProperty(fpr, prop.dtype)

            query = merge_formulas(query, get_ref_filters(
                context,
                prop.dtype.model,
                _fpr,
                seen=seen + [ref_key],
            ))

    return query
