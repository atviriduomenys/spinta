from __future__ import annotations

import datetime
import uuid
from typing import Union, Any

import geoalchemy2.functions
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

from spinta import exceptions, commands
from spinta.auth import authorized
from spinta.backends.constants import TableType, BackendFeatures
from spinta.backends.postgresql.ufuncs.query.components import PgQueryBuilder, InheritForeignProperty, Lower, Recurse, \
    Negative, Positive
from spinta.components import Property, Action, Page
from spinta.core.ufuncs import Bind, Negative as Negative_
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import ufunc, GetAttr
from spinta.datasets.backends.sql.ufuncs.components import Selected
from spinta.exceptions import EmptyStringSearch, NoneValueComparison, NotImplementedFeature
from spinta.exceptions import FieldNotInResource
from spinta.types.datatype import Array
from spinta.types.datatype import DataType, ExternalRef, Inherit, BackRef, Time, ArrayBackRef, Denorm
from spinta.types.datatype import Date
from spinta.types.datatype import DateTime
from spinta.types.datatype import File
from spinta.types.datatype import Integer
from spinta.types.datatype import Number
from spinta.types.datatype import Object
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.types.datatype import UUID as UUID_dtype
from spinta.types.geometry.components import Geometry
from spinta.types.text.components import Text
from spinta.types.text.helpers import determine_language_property_for_text
from spinta.ufuncs.basequerybuilder.components import ReservedProperty, \
    NestedProperty, ResultProperty, Flip
from spinta.ufuncs.basequerybuilder.helpers import get_column_with_extra, get_language_column, \
    expandable_not_expanded
from spinta.ufuncs.basequerybuilder.ufuncs import Star
from spinta.ufuncs.components import ForeignProperty
from spinta.utils.data import take


def _gather_selected_properties(env: PgQueryBuilder):
    result = []
    if env.selected:
        for selected in env.selected.values():
            if selected and selected.prop:
                result.append(selected.prop)
    return result


def _get_property_for_select(env: PgQueryBuilder, name: str):
    prop = env.model.properties.get(name)
    if prop and authorized(env.context, prop, Action.SEARCH):
        return prop
    else:
        raise FieldNotInResource(env.model, property=name)


@ufunc.resolver(PgQueryBuilder, Inherit, Bind)
def _resolve_getattr(env, dtype, attr):
    return InheritForeignProperty(dtype.prop.model, attr.name, dtype.prop)


@ufunc.resolver(PgQueryBuilder, Expr)
def select(env, expr):
    keys = [str(k) for k in expr.args]
    args, kwargs = expr.resolve(env)
    args = list(zip(keys, args)) + list(kwargs.items())

    env.selected = {}
    if args:
        for key, arg in args:
            selected = env.call('select', arg)
            if selected is not None:
                env.selected[key] = selected
    else:
        env.call('select', Star())

    if not (len(args) == 1 and args[0][0] == '_page'):
        assert env.selected, args


@ufunc.resolver(PgQueryBuilder, Star)
def select(env, arg: Star) -> None:
    for prop in take(env.model.properties).values():
        # if authorized(env.context, prop, (Action.GETALL, Action.SEARCH)):
        # TODO: This line above should come from a getall(request),
        #       because getall can be used internally for example for
        #       writes.

        # Check if prop is expanded or not
        if expandable_not_expanded(env, prop):
            continue
        env.selected[prop.place] = env.call('select', prop)


@ufunc.resolver(PgQueryBuilder, Bind)
def select(env, arg):
    if arg.name == '_type':
        return Selected(None, env.model.properties['_type'])
    if arg.name == '_page':
        return None
    prop = _get_property_for_select(env, arg.name)
    if expandable_not_expanded(env, prop):
        return Selected(None, prop, prep=[])
    return env.call('select', prop)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType)
def select(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
) -> Selected:
    table = env.get_joined_table_from_ref(fpr)
    column = env.backend.get_column(table, dtype.prop, select=True).label(
        fpr.left.prop.place + '.' + dtype.prop.name
    )
    return Selected(
        item=env.add_column(column),
        prop=dtype.prop,
    )


@ufunc.resolver(PgQueryBuilder, Property)
def select(env, prop):
    if prop.place not in env.resolved:
        if prop.external and prop.external.prepare:
            result = env(this=prop).resolve(prop.external.prepare)
            result = env.call("select", prop.dtype, result)
        else:
            result = env.call("select", prop.dtype)
        env.resolved[prop.place] = result
    return env.resolved[prop.place]


@ufunc.resolver(PgQueryBuilder, DataType)
def select(env: PgQueryBuilder, dtype: DataType):
    table = env.backend.get_table(env.model)

    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        # XXX: Probably if dtype.prop is in a nested list, we need to get the
        #      first list. Because if I remember correctly, only first list is
        #      stored on the main table.
        column = env.backend.get_column(table, dtype.prop.list, select=True)
    return Selected(env.add_column(column), dtype.prop)


@ufunc.resolver(PgQueryBuilder, String)
def select(env, dtype):
    env.call('validate_dtype_for_select', dtype, _gather_selected_properties(env))
    if dtype.prop.list is None:
        column = env.backend.get_column(env.table, dtype.prop)
    else:
        column = env.backend.get_column(env.table, dtype.prop.list)
    return Selected(env.add_column(column), dtype.prop)


@ufunc.resolver(PgQueryBuilder, Text)
def select(env, dtype):
    env.call('validate_dtype_for_select', dtype, _gather_selected_properties(env))
    if dtype.prop.list:
        column = env.backend.get_column(env.table, dtype.prop.list)
        return Selected(env.add_column(column), dtype.prop)

    if env.query_params.push:
        result = {
            key: env.call('select', prop)
            for key, prop in dtype.langs.items()
        }
        return Selected(prop=dtype.prop, prep=result)

    if env.query_params.lang:
        for lang in env.query_params.lang:
            if isinstance(lang, Star):
                result = {
                    key: env.call('select', prop)
                    for key, prop in dtype.langs.items()
                }
                return Selected(prop=dtype.prop, prep=result)
            break

        result = {
            key: env.call('select', prop)
            for key, prop in dtype.langs.items() if key in env.query_params.lang
        }
        return Selected(prop=dtype.prop, prep=result)

    default_langs = env.context.get('config').languages
    lang_prop = determine_language_property_for_text(dtype, env.query_params.lang_priority, default_langs)
    return Selected(prop=dtype.prop, prep={
        lang_prop.name: env.call('select', lang_prop),
    })


@ufunc.resolver(PgQueryBuilder, Object)
def select(env, dtype):
    prep = {}
    for prop in take(dtype.properties).values():
        sel = env.call('select', prop)
        if sel is not None:
            prep[prop.name] = sel
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(PgQueryBuilder, File)
def select(env, dtype):
    table = env.backend.get_table(env.model)
    name_id = dtype.prop.place + '._id'
    name_content_type = dtype.prop.place + '._content_type'
    name_size = dtype.prop.place + '._size'
    prep = {
        '_id': Selected(env.add_column(table.c[name_id])),
        '_content_type': Selected(env.add_column(table.c[name_content_type])),
        '_size': Selected(env.add_column(table.c[name_size]))
    }
    same_backend = dtype.backend.name == dtype.prop.model.backend.name
    if same_backend or BackendFeatures.FILE_BLOCKS in dtype.backend.features:
        name_bsize = dtype.prop.place + '._bsize'
        name_blocks = dtype.prop.place + '._blocks'
        prep['_bsize'] = Selected(env.add_column(table.c[name_bsize]))
        prep['_blocks'] = Selected(env.add_column(table.c[name_blocks]))
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(PgQueryBuilder, File, str)
def select(env, dtype, leaf):
    table = env.backend.get_table(env.model)
    if leaf == '_content':
        # Currently _content is handled in prepare_dtype_for_response
        # we need to make sure that whole File is loaded when _content is selected
        env.selected[dtype.prop.place] = env.call('select', dtype.prop)
    else:
        column = table.c[dtype.prop.place + '.' + leaf]
        return Selected(env.add_column(column), dtype.prop)


@ufunc.resolver(PgQueryBuilder, Ref)
def select(env, dtype: Ref):
    uri = dtype.model.uri_prop
    prep = {}
    if not dtype.inherited:
        name = '_id'
        if env.query_params.prioritize_uri and uri is not None:
            fpr = ForeignProperty(None, dtype, dtype.model.properties['_id'].dtype)
            table = env.get_joined_table(fpr)
            column = table.c[uri.place]
            name = '_uri'
            column = column.label(dtype.prop.place + '._uri')
        else:
            table = env.backend.get_table(env.model)
            column = table.c[dtype.prop.place + '._id']
        column = env.add_column(column)
        prep[name] = Selected(column, dtype.prop)
    for prop in dtype.properties.values():
        sel = env.call('select', prop)
        prep[prop.name] = sel
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(PgQueryBuilder, ExternalRef)
def select(env, dtype: ExternalRef):
    prep = {}
    if not dtype.inherited:
        table = env.backend.get_table(env.model)
        if dtype.model.given.pkeys or dtype.explicit:
            props = dtype.refprops
        else:
            props = [dtype.model.properties['_id']]
        for prop in props:
            name = f"{dtype.prop.place}.{prop.place}"
            column = table.c[name]
            prep[prop.name] = Selected(env.add_column(column), prop)
    for prop in dtype.properties.values():
        sel = env.call('select', prop)
        prep[prop.name] = sel
    return Selected(prop=dtype.prop, prep=prep)


@ufunc.resolver(PgQueryBuilder, Ref, str)
def select(env, dtype: Ref, prop: str):
    table = env.backend.get_table(env.model)
    return Selected(
        env.add_column(
            table.c[dtype.prop.place + '.' + prop]
        )
    )


@ufunc.resolver(PgQueryBuilder, BackRef)
def select(env, dtype):
    return _select_backref(env, dtype)


@ufunc.resolver(PgQueryBuilder, ArrayBackRef)
def select(env: PgQueryBuilder, dtype: ArrayBackRef):
    selected = _select_backref(env, dtype.items.dtype, is_array=True)
    return Selected(prop=dtype.prop, prep=selected.prep)


def _select_backref(env, dtype, is_array=False):
    fpr = ForeignProperty(
        None,
        left=dtype,
        right=dtype.refprop.dtype,
    )
    refprop = dtype.refprop
    required_columns = []
    return_columns = {}

    if commands.identifiable(refprop):
        id_ = fpr.right.prop.model.properties['_id']
        column_name = id_.name
        label = f'{dtype.prop.name}.{column_name}'
        required_columns.append((column_name, label))
        return_columns[label] = id_
    else:
        for prop in dtype.refprop.dtype.refprops:
            column_name = prop.name
            label = f'{dtype.prop.name}.{column_name}'
            required_columns.append((column_name, label))
            return_columns[label] = prop

    selector = env.generate_backref_select(dtype.prop, dtype.refprop, required_columns, is_array)
    table = env.get_backref_joined_table(fpr, selector)

    prep = {}

    if is_array:
        key = dtype.prop.name
        prop = dtype.prop
        selected_column = env.add_column(table.c[key])
        return Selected(prop=prop, prep=Selected(selected_column, prop))

    for key, prop in return_columns.items():
        prep[prop.place] = Selected(env.add_column(table.c[key]), prop)
    return Selected(prop=fpr.left.prop, prep=prep)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def select(env: PgQueryBuilder, fpr: ForeignProperty):
    table = env.get_joined_table(fpr)
    fixed_name = fpr.right.prop.place
    if fixed_name.startswith(f'{fpr.left.prop.place}.'):
        fixed_name = fixed_name.replace(f'{fpr.left.prop.place}.', '', 1)
    column = table.c[fixed_name]
    column = column.label(fpr.place)
    return Selected(env.add_column(column), fpr.right.prop)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, Inherit)
def select(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: Inherit,
) -> Selected:
    table = env.get_joined_table(fpr)
    fixed_name = dtype.prop.place
    if fixed_name.startswith(f'{fpr.left.prop.place}.'):
        fixed_name = fixed_name.replace(f'{fpr.left.prop.place}.', '', 1)
    column = table.c[fixed_name]
    column = column.label(fpr.place)
    return Selected(env.add_column(column), dtype.prop)


@ufunc.resolver(PgQueryBuilder, Inherit)
def select(env, dtype):
    table = env.get_joined_base_table(dtype.prop.model, dtype.prop.name)
    column = table.c[dtype.prop.name]
    column = column.label(dtype.prop.name)
    return Selected(env.add_column(column), dtype.prop)


@ufunc.resolver(PgQueryBuilder, InheritForeignProperty)
def select(env, dtype):
    table = env.get_joined_base_table(dtype.model, dtype.prop_name)
    column = table.c[dtype.prop_name]
    column = column.label(f"{dtype.base_prop.name}.{dtype.prop_name}")
    return Selected(env.add_column(column), dtype.base_prop)


@ufunc.resolver(PgQueryBuilder, Denorm)
def select(env, dtype):
    ref = dtype.prop.parent
    if isinstance(ref, Property) and isinstance(ref.dtype, Ref):
        fpr = None
        if ref.dtype.inherited:
            root_ref_parent = ref.parent

            while root_ref_parent and isinstance(root_ref_parent, Property) and isinstance(root_ref_parent.dtype, Ref):
                fpr = ForeignProperty(fpr, root_ref_parent.dtype, root_ref_parent.dtype.model.properties['_id'].dtype)

                if not root_ref_parent.dtype.inherited:
                    break
                root_ref_parent = root_ref_parent.parent

        fpr = ForeignProperty(fpr, ref.dtype, ref.dtype.model.properties['_id'].dtype)
        table = env.get_joined_table_from_ref(fpr)
        column = table.c[dtype.rel_prop.place]
        column = column.label(dtype.prop.place)
        return Selected(env.add_column(column), prop=dtype.prop)


@ufunc.resolver(PgQueryBuilder, Page)
def select(env, page):
    return_selected = []
    for item in page.by.values():
        selected = env.call('select', item.prop)
        if selected.item is not None:
            return_selected.append(env.columns[selected.item])
    return return_selected


@ufunc.resolver(PgQueryBuilder, sa.sql.expression.ColumnElement)
def select(env, column):
    return Selected(env.add_column(column))


@ufunc.resolver(PgQueryBuilder, ForeignProperty, PrimaryKey)
def select(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: PrimaryKey,
) -> Selected:
    super_ = ufunc.resolver[env, fpr, dtype]
    return super_(env, fpr, dtype)


@ufunc.resolver(PgQueryBuilder, Geometry, Flip)
def select(env: PgQueryBuilder, dtype: Geometry, func_: Flip):
    table = env.backend.get_table(env.model)

    if dtype.prop.list is None:
        column = env.backend.get_column(table, dtype.prop, select=True)
    else:
        column = env.backend.get_column(table, dtype.prop.list, select=True)

    column = geoalchemy2.functions.ST_FlipCoordinates(column)
    return Selected(env.add_column(column), prop=dtype.prop)


@ufunc.resolver(PgQueryBuilder, int)
def limit(env, n):
    env.limit = n


@ufunc.resolver(PgQueryBuilder, int)
def offset(env, n):
    env.offset = n


@ufunc.resolver(PgQueryBuilder, Expr, name='and')
def and_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    if len(args) > 1:
        return sa.and_(*args)
    elif args:
        return args[0]


@ufunc.resolver(PgQueryBuilder, Expr, name='or')
def or_(env, expr):
    args, kwargs = expr.resolve(env)
    args = [a for a in args if a is not None]
    return env.call('or', args)


@ufunc.resolver(PgQueryBuilder, list, name='or')
def or_(env, args):
    if len(args) > 1:
        return sa.or_(*args)
    elif args:
        return args[0]


@ufunc.resolver(PgQueryBuilder)
def count(env):
    env.aggregate = True
    return sa.func.count().label('count()')


COMPARE = [
    'eq',
    'ne',
    'lt',
    'le',
    'gt',
    'ge',
    'startswith',
    'contains',
]

COMPARE_EQUATIONS = [
    'eq',
    'ne',
    'lt',
    'le',
    'gt',
    'ge',
]

COMPARE_STRING = [
    'eq',
    'startswith',
    'contains',
]


@ufunc.resolver(PgQueryBuilder, GetAttr, object, names=COMPARE)
def compare(env: PgQueryBuilder, op: str, attr: GetAttr, value: Any):
    resolved = env.call('_resolve_getattr', attr)
    return env.call(op, resolved, value)


@ufunc.resolver(PgQueryBuilder, ReservedProperty, object, names=COMPARE)
def compare(env, op: str, reserved: ReservedProperty, value: Any):
    table = env.backend.get_table(reserved.dtype.prop.model)
    column = table.c[reserved.dtype.prop.place + '.' + reserved.param]

    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, reserved.dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, object, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, object, names=COMPARE)
def compare(env, op: str, fpr: ForeignProperty, value: Any):
    return env.call(op, fpr, fpr.right, value)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, object, names=COMPARE)
def compare(env, op, dtype, value):
    str_value = str(value)
    try:
        uuid.UUID(str_value)
    except ValueError:
        raise exceptions.InvalidValue(dtype, op=op, arg=type(value).__name__)

    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, str_value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, type(None), names=COMPARE)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, UUID_dtype, str, names=COMPARE)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, String, str, names=COMPARE)
def compare(env, op, dtype, value):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, (Integer, Number), (int, float), names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, DateTime, str, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.datetime.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, DateTime, datetime.datetime, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, Date, str, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.date.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, Date, datetime.date, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, Time, str, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.time.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, Time, datetime.time, names=COMPARE_EQUATIONS)
def compare(env, op, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, DataType, object, names=COMPARE)
def compare(env, op, dtype, value):
    raise exceptions.InvalidValue(dtype, op=op, arg=type(value).__name__)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType, object, names=COMPARE)
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: DataType,
    value: Any,
):
    raise exceptions.InvalidValue(dtype, op=op, arg=type(value).__name__)


@ufunc.resolver(PgQueryBuilder, UUID_dtype, str)
def eq(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, DataType, type(None))
def eq(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, Text, (str, Bind, type(None)))
def eq(env, dtype, value):
    column = get_language_column(env, dtype.prop)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType, type(None))
def eq(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: type(None),
):
    table = env.get_joined_table(fpr)
    column = env.backend.get_column(table, fpr.right.prop)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, PrimaryKey, str)
def eq(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: type(None),
):
    table = env.backend.get_table(fpr.left.model)
    column = env.backend.get_column(table, fpr.left.prop)
    cond = _sa_compare('eq', column, value)
    return _prepare_condition(env, dtype.prop, cond)


def _ensure_non_empty(op, s):
    if s == '':
        raise EmptyStringSearch(op=op)


@ufunc.resolver(PgQueryBuilder, UUID_dtype, str, names=COMPARE_STRING)
def compare(env: PgQueryBuilder, op: str, dtype: UUID, value: str):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = env.backend.get_column(env.table, dtype.prop).cast(sa.String)
    return _sa_compare(op, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, String, str, names=COMPARE_STRING)
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: String,
    value: str,
):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, str, names=COMPARE_STRING)
def compare(env, op, dtype, value):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = env.backend.get_column(env.table, dtype.prop)
    return _sa_compare(op, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, (Integer, Number), (int, float), names=COMPARE_EQUATIONS)
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: Union[Integer, Number],
    value: Union[int, float],
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DateTime, str, names=COMPARE_EQUATIONS)
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: DateTime,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    value = datetime.datetime.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, Date, str, names=COMPARE_EQUATIONS)
def compare(
    env: PgQueryBuilder,
    op: str,
    fpr: ForeignProperty,
    dtype: Date,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    value = datetime.date.fromisoformat(value)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, dtype.prop, cond)


@ufunc.resolver(PgQueryBuilder, String)
def lower(env, dtype):
    return Lower(dtype)


@ufunc.resolver(PgQueryBuilder, Recurse)
def lower(env, recurse):
    return Recurse([env.call('lower', arg) for arg in recurse.args])


@ufunc.resolver(PgQueryBuilder, Lower, str, names=COMPARE_STRING)
def compare(env, op, fn, value):
    if op in ('startswith', 'contains'):
        _ensure_non_empty(op, value)
    column = get_column_with_extra(env, fn.dtype.prop)
    column = sa.func.lower(column)
    cond = _sa_compare(op, column, value)
    return _prepare_condition(env, fn.dtype.prop, cond)


def _sa_compare(op, column, value):
    if value is None and op not in ['eq', 'ne']:
        raise NoneValueComparison(op=op)

    # Convert JSONB value from -> to ->> with astext
    if isinstance(column.type, sa.JSON):
        if not isinstance(column, sa.Column):
            column = column.element
        column = column.astext

    if op == 'eq':
        return column == value

    if op == 'lt':
        return column < value

    if op == 'le':
        return column <= value

    if op == 'gt':
        return column > value

    if op == 'ge':
        return column >= value

    if op == 'contains':
        if isinstance(column.type, UUID):
            column = column.cast(sa.String)
        return column.contains(value)

    if op == 'startswith':
        if isinstance(column.type, UUID):
            column = column.cast(sa.String)
        return column.startswith(value)
    raise NotImplementedError


def _prepare_condition(env: PgQueryBuilder, prop: Property, cond):
    if prop.list is None:
        return cond

    main_table = env.table
    list_table = env.backend.get_table(prop.list, TableType.LIST)
    subqry = (
        sa.select(
            [list_table.c._rid],
            distinct=list_table.c._rid,
        ).
        where(cond).
        alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry,
        main_table.c._id == subqry.c._rid,
    )
    return subqry.c._rid.isnot(None)


@ufunc.resolver(PgQueryBuilder, DataType, type(None))
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, UUID_dtype, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType, type(None))
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: type(None),
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, String, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, Text, str)
def ne(env, dtype, value):
    column = get_language_column(env, dtype)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, String, str)
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DataType,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, PrimaryKey, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, (Integer, Number), (int, float))
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, (Integer, Number), (int, float))
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: Union[Integer, Number],
    value: Union[int, float],
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, DateTime, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.datetime.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DateTime, str)
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: DateTime,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    value = datetime.datetime.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, Date, str)
def ne(env, dtype, value):
    column = env.backend.get_column(env.table, dtype.prop)
    value = datetime.date.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, Date, str)
def ne(
    env: PgQueryBuilder,
    fpr: ForeignProperty,
    dtype: Date,
    value: str,
):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    value = datetime.date.fromisoformat(value)
    return _ne_compare(env, dtype.prop, column, value)


@ufunc.resolver(PgQueryBuilder, Array, (object, type(None)), names=COMPARE)
def compare(env, op, dtype, value):
    return env.call(op, dtype.items.dtype, value)


@ufunc.resolver(PgQueryBuilder, Lower, str)
def ne(env, fn, value):
    column = get_column_with_extra(env, fn.dtype.prop)
    column = sa.func.lower(column)
    return _ne_compare(env, fn.dtype.prop, column, value)


def _ne_compare(env: PgQueryBuilder, prop: Property, column, value):
    """Not equal operator is quite complicated thing and need explaining.

    If property is not defined within a list, just do `!=` comparison and be
    done with it.

    If property is in a list:

    - First check if there is at least one list item where field is not None
        (existance check).

    - Then check if there is no list items where field equals to given
        value.
    """

    if prop.list is None:
        return column != value

    main_table = env.backend.get_table(prop.model)
    list_table = env.backend.get_table(prop.list, TableType.LIST)

    # Check if at liest one value for field is defined
    subqry1 = (
        sa.select(
            [list_table.c._rid],
            distinct=list_table.c._rid,
        ).
        where(column != None).  # noqa
        alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry1,
        main_table.c._id == subqry1.c._rid,
    )

    # Check if given value exists
    subqry2 = (
        sa.select(
            [list_table.c._rid],
            distinct=list_table.c._rid,
        ).
        where(column == value).
        alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry2,
        main_table.c._id == subqry2.c._rid,
    )

    # If field exists and given value does not, then field is not equal to
    # value.
    return sa.and_(
        subqry1.c._rid != None,  # noqa
        subqry2.c._rid == None,
    )


FUNCS = [
    'lower',
    'upper',
    'flip'
]


@ufunc.resolver(PgQueryBuilder, Bind, names=FUNCS)
def func(env, name, field):
    prop = env.model.flatprops[field.name]
    return env.call(name, prop.dtype)


@ufunc.resolver(PgQueryBuilder, GetAttr, names=FUNCS)
def func(env, name, field):
    resolved = env.call('_resolve_getattr', field)
    return env.call(name, resolved)


@ufunc.resolver(PgQueryBuilder, NestedProperty, names=FUNCS)
def func(env, name, nested):
    return env.call(name, nested.right)


@ufunc.resolver(PgQueryBuilder, Bind)
def recurse(env, field):
    if field.name in env.model.leafprops:
        return Recurse([prop.dtype for prop in env.model.leafprops[field.name]])
    else:
        raise exceptions.FieldNotInResource(env.model, property=field.name)


@ufunc.resolver(PgQueryBuilder, Recurse, object, names=COMPARE)
def recurse(env, op, recurse, value):
    return env.call('or', [
        env.call(op, arg, value)
        for arg in recurse.args
    ])


@ufunc.resolver(PgQueryBuilder, Expr, name='any')
def any_(env, expr):
    args, kwargs = expr.resolve(env)
    op, field, *args = args
    if isinstance(op, Bind):
        op = op.name
    return env.call('or', [
        env.call(op, field, arg)
        for arg in args
    ])


@ufunc.resolver(PgQueryBuilder, Expr)
def sort(env, expr):
    args, kwargs = expr.resolve(env)
    env.sort = [
        env.call('sort', arg) for arg in args
    ]


@ufunc.resolver(PgQueryBuilder, Bind)
def sort(env, field):
    prop = env.model.get_from_flatprops(field.name)
    return env.call('asc', prop.dtype)


@ufunc.resolver(PgQueryBuilder, DataType)
def sort(env, dtype):
    return env.call('asc', dtype)


@ufunc.resolver(PgQueryBuilder, Negative_)
def sort(env, field):
    prop = env.model.get_from_flatprops(field.name)
    return env.call('desc', prop.dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def sort(env: PgQueryBuilder, fpr: ForeignProperty):
    return env.call('asc', fpr, fpr.right)


@ufunc.resolver(PgQueryBuilder, Positive)
def sort(env, sign):
    return env.call('asc', sign.arg)


@ufunc.resolver(PgQueryBuilder, Negative)
def sort(env, sign):
    return env.call('desc', sign.arg)


@ufunc.resolver(PgQueryBuilder, DataType)
def asc(env, dtype):
    column = _get_sort_column(env, dtype.prop)
    return column.asc()


@ufunc.resolver(PgQueryBuilder, NestedProperty)
def asc(env: PgQueryBuilder, nested: NestedProperty):
    return env.call('asc', nested.right)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def asc(env: PgQueryBuilder, fpr: ForeignProperty):
    return env.call('asc', fpr, fpr.right)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType)
def asc(env: PgQueryBuilder, fpr: ForeignProperty, dtype: DataType):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    return column.asc()


@ufunc.resolver(PgQueryBuilder, DataType)
def desc(env, dtype):
    column = _get_sort_column(env, dtype.prop)
    return column.desc()


@ufunc.resolver(PgQueryBuilder, NestedProperty)
def desc(env: PgQueryBuilder, nested: NestedProperty):
    return env.call('desc', nested.right)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def desc(env: PgQueryBuilder, fpr: ForeignProperty):
    return env.call('desc', fpr, fpr.right)


@ufunc.resolver(PgQueryBuilder, ForeignProperty, DataType)
def desc(env: PgQueryBuilder, fpr: ForeignProperty, dtype: DataType):
    table = env.get_joined_table(fpr)
    column = table.c[fpr.right.prop.place]
    return column.desc()


@ufunc.resolver(PgQueryBuilder, Array, names=['asc', 'desc'])
def sort(env, name, dtype: Array):
    return env.call(name, dtype.items.dtype)


def _get_sort_column(env: PgQueryBuilder, prop: Property):
    column = get_column_with_extra(env, prop)

    if prop.list is None:
        return column

    main_table = env.table
    list_table = env.backend.get_table(prop.list, TableType.LIST)
    subqry = (
        sa.select(
            [list_table.c._rid, column.label('value')],
            distinct=list_table.c._rid,
        ).alias()
    )
    env.from_ = env.from_.outerjoin(
        subqry,
        main_table.c._id == subqry.c._rid,
    )
    return subqry.c.value


@ufunc.resolver(PgQueryBuilder, GetAttr)
def negative(env, field) -> Negative:
    resolved = env.call('_resolve_getattr', field)
    return Negative(resolved)


@ufunc.resolver(PgQueryBuilder, GetAttr)
def positive(env, field) -> Positive:
    resolved = env.call('_resolve_getattr', field)
    return Positive(resolved)


@ufunc.resolver(PgQueryBuilder, Bind)
def negative(env, field) -> Negative:
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return Negative(prop.dtype)


@ufunc.resolver(PgQueryBuilder, Bind)
def positive(env, field) -> Positive:
    if field.name in env.model.properties:
        prop = env.model.properties[field.name]
    else:
        raise FieldNotInResource(env.model, property=field.name)
    return Positive(prop.dtype)


@ufunc.resolver(PgQueryBuilder, DataType)
def negative(env, dtype) -> Negative:
    return Negative(dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def negative(env: PgQueryBuilder, fpr: ForeignProperty) -> Negative:
    return Negative(fpr)


@ufunc.resolver(PgQueryBuilder, DataType)
def positive(env, dtype) -> Positive:
    return Positive(dtype)


@ufunc.resolver(PgQueryBuilder, ForeignProperty)
def positive(env: PgQueryBuilder, fpr: ForeignProperty) -> Positive:
    return Positive(fpr)


@ufunc.resolver(PgQueryBuilder, Expr)
def checksum(env: PgQueryBuilder, expr: Expr):
    args = []
    for prop in take(env.model.properties).values():
        if env.selected is not None and prop.place in env.selected:
            selected = env.selected[prop.place]
        else:
            selected = env.call('select', prop)
        if isinstance(selected, Selected):
            args.append(selected)
    return ResultProperty(
        Expr('checksum', *args)
    )


@ufunc.resolver(PgQueryBuilder, Geometry)
def flip(env: PgQueryBuilder, dtype: Geometry):
    return Flip(dtype)


@ufunc.resolver(PgQueryBuilder, Expr)
def file(env: PgQueryBuilder, expr: Expr) -> Expr:
    raise NotImplementedFeature(env.backend, feature="Ability to use file() function with `PostgreSql` backend")


@ufunc.resolver(PgQueryBuilder, Bind, Bind)
def point(env: PgQueryBuilder, x: Bind, y: Bind) -> Expr:
    return Expr(
        'point',
        env.call('select', x),
        env.call('select', y),
    )
