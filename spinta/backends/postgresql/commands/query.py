from __future__ import annotations

from typing import Optional, List, Union, Tuple

import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import UUID

from spinta import commands
from spinta import exceptions
from spinta.core.ufuncs import Env, ufunc
from spinta.core.ufuncs import Bind
from spinta.exceptions import UnknownExpr
from spinta.components import Property
from spinta.hacks.recurse import _replace_recurse
from spinta.utils.schema import is_valid_sort_key
from spinta.types.datatype import Array
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.helpers import get_column_name


class PgQueryBuilder(Env):

    def init(self, backend: PostgreSQL, table: sa.Table):
        return self(
            backend=backend,
            table=table,
            # TODO: Select list must be taken from params.select.
            select=[table],
            joins={},
            from_=table,
            sort=[],
            limit=None,
            offset=None,
        )

    def build(self, where):
        qry = sa.select(self.select)
        qry = qry.select_from(self.from_)

        if where is not None:
            qry = qry.where(where)

        if self.sort:
            qry = qry.order_by(*self.sort)

        if self.limit is not None:
            qry = qry.limit(self.limit)

        if self.offset is not None:
            qry = qry.offset(self.offset)

        return qry

    def default_resolver(self, expr, *args, **kwargs):
        raise UnknownExpr(expr=str(expr(*args, **kwargs)), name=expr.name)

    def get_joined_table(self, prop: ForeignProperty) -> sa.Table:
        for fpr in prop.chain:
            if fpr.name in self.joins:
                continue

            ltable = self.backend.get_table(fpr.left.model)
            lrkeys = [self.backend.get_column(ltable, fpr.left)]

            rmodel = fpr.right.model
            rtable = self.backend.get_table(rmodel)
            rpkeys = fpr.left.dtype.refprops or rmodel.external.pkeys
            rpkeys = [self.backend.get_column(rtable, k) for k in rpkeys]

            assert len(lrkeys) == len(rpkeys), (lrkeys, rpkeys)
            condition = []
            for lrk, rpk in zip(lrkeys, rpkeys):
                condition += [lrk == rpk]

            assert len(condition) > 0
            if len(condition) == 1:
                condition = condition[0]
            else:
                condition = sa.and_(condition)

            self.from_ = self.joins[fpr.name] = self.from_.join(rtable, condition)

        model = fpr.right.model
        table = self.backend.get_table(model)
        return table


class ForeignProperty:

    def __init__(self, fpr: ForeignProperty, left: Property, right: Property):
        if fpr is None:
            self.name = left.place
            self.chain = [self]
        else:
            self.name += '->' + left.place
            self.chain = fpr.chain + [self]

        self.left = left
        self.right = right

    def __repr__(self):
        return f'<{self.name}->{self.right.name}:{self.right.dtype.name}>'


@ufunc.resolver(PgQueryBuilder, Bind, str)
def eq(env, field, value):
    prop = env.model.flatprops[field.name]
    column = env.backend.get_column(env.table, prop)
    return column == value


@ufunc.resolver(PgQueryBuilder, str, str)
def eq(env, field, value):
    # XXX: Backwards compatible resolver, first `str` argument is deprecated,
    #      should be Bind.
    prop = env.model.flatprops[field]
    column = env.backend.get_column(env.table, prop)
    return column == value


class Legacy:

    def _get_method(self, name):
        method = getattr(self, f'op_{name}', None)
        if method is None:
            raise exceptions.UnknownOperator(self.model, operator=name)
        return method

    def resolve_recurse(self, arg):
        name = arg['name']
        if name in self.compops:
            return _replace_recurse(self.model, arg, 0)
        if name == 'any':
            return _replace_recurse(self.model, arg, 1)
        return arg

    def resolve(self, args: Optional[List[dict]]) -> None:
        for arg in (args or []):
            arg = self.resolve_recurse(arg)
            name = arg['name']
            opargs = arg.get('args', ())
            method = self._get_method(name)
            if name in self.compops:
                yield self.comparison(name, method, *opargs)
            else:
                yield method(*opargs)

    def resolve_property(self, key: Union[str, tuple], sort: bool = False) -> Property:
        if isinstance(key, tuple):
            key = '.'.join(key)

        if sort:
            if not is_valid_sort_key(key, self.model):
                raise exceptions.FieldNotInResource(self.model, property=key)
        elif key not in self.model.flatprops:
            raise exceptions.FieldNotInResource(self.model, property=key)

        prop = self.model.flatprops[key]
        if isinstance(prop.dtype, Array):
            return prop.dtype.items
        else:
            return prop

    def resolve_value(self, op: str, prop: Property, value: Union[str, dict]) -> object:
        return commands.load_search_params(self.context, prop.dtype, self.backend, {
            'name': op,
            'args': [prop.place, value]
        })

    def resolve_lower_call(self, key):
        if isinstance(key, dict) and key['name'] == 'lower':
            return key['args'][0], True
        else:
            return key, False

    def comparison(self, op, method, key, value):
        key, lower = self.resolve_lower_call(key)
        prop = self.resolve_property(key)
        value = self.resolve_value(op, prop, value)
        field = self.get_sql_field(prop, lower)
        value = self.get_sql_value(prop, value)
        cond = method(prop, field, value)
        return self.compare(op, prop, cond)

    def compare(self, op, prop, cond):
        if prop.list is not None and op != 'ne':
            main_table = self.backend.get_table(self.model)
            list_table = self.backend.get_table(prop.list, TableType.LIST)
            subqry = (
                sa.select(
                    [list_table.c._rid],
                    distinct=list_table.c._rid,
                ).
                where(cond).
                alias()
            )
            self.joins = self.joins.outerjoin(
                subqry,
                main_table.c._id == subqry.c._rid,
            )
            return subqry.c._rid.isnot(None)
        else:
            return cond

    def get_sql_field(self, prop: Property, lower: bool = False):
        if prop.list is not None:
            list_table = self.backend.get_table(prop.list, TableType.LIST)
            field = list_table.c[get_column_name(prop)]
        else:
            main_table = self.backend.get_table(self.model)
            field = main_table.c[prop.place]
        if lower:
            field = sa.func.lower(field)
        return field

    def get_sql_value(self, prop: Property, value: object):
        return value

    def op_group(self, *args: List[dict]):
        args = list(self.resolve(args))
        assert len(args) == 1, "Group with multiple args are not supported here."
        return args[0]

    def op_and(self, *args: List[dict]):
        return sa.and_(*self.resolve(args))

    def op_or(self, *args: List[dict]):
        return sa.or_(*self.resolve(args))

    def op_eq(self, prop, field, value):
        return field == value

    def op_ge(self, prop, field, value):
        return field >= value

    def op_gt(self, prop, field, value):
        return field > value

    def op_le(self, prop, field, value):
        return field <= value

    def op_lt(self, prop, field, value):
        return field < value

    def op_ne(self, prop, field, value):
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
            return field != value

        main_table = self.backend.get_table(self.model)
        list_table = self.backend.get_table(prop.list, TableType.LIST)

        # Check if at liest one value for field is defined
        subqry1 = (
            sa.select(
                [list_table.c._rid],
                distinct=list_table.c._rid,
            ).
            where(field != None).  # noqa
            alias()
        )
        self.joins = self.joins.outerjoin(
            subqry1,
            main_table.c._id == subqry1.c._rid,
        )

        # Check if given value exists
        subqry2 = (
            sa.select(
                [list_table.c._rid],
                distinct=list_table.c._rid,
            ).
            where(field == value).
            alias()
        )
        self.joins = self.joins.outerjoin(
            subqry2,
            main_table.c._id == subqry2.c._rid,
        )

        # If field exists and given value does not, then field is not equal to
        # value.
        return sa.and_(
            subqry1.c._rid != None,  # noqa
            subqry2.c._rid == None,
        )

    def op_contains(self, prop, field, value):
        if isinstance(field.type, UUID):
            return field.cast(sa.String).contains(value)
        return field.contains(value)

    def op_startswith(self, prop, field, value):
        if isinstance(field.type, UUID):
            return field.cast(sa.String).startswith(value)
        return field.startswith(value)

    def op_any(self, op: str, key: str, *value: Tuple[Union[str, int, float]]):
        if op in ('contains', 'startswith'):
            return self.op_or(*(
                {
                    'name': op,
                    'args': [key, v],
                }
                for v in value
            ))

        method = self._get_method(op)
        key, lower = self.resolve_lower_call(key)
        prop = self.resolve_property(key)
        field = self.get_sql_field(prop, lower)
        value = [
            self.get_sql_value(prop, self.resolve_value(op, prop, v))
            for v in value
        ]
        value = sa.any_(value)
        cond = method(prop, field, value)
        return self.compare(op, prop, cond)

    def sort(
        self,
        qry: sa.sql.Select,
        sort: List[Tuple[str, str]],
    ) -> sa.sql.Select:
        direction = {
            'positive': lambda c: c.asc(),
            'negative': lambda c: c.desc(),
        }
        fields = []
        for key in sort:
            # Optional sort direction: sort(+key) or sort(key)
            # XXX: Probably move this to spinta/urlparams.py.
            if isinstance(key, dict) and key['name'] in direction:
                d = direction[key['name']]
                key = key['args'][0]
            else:
                d = direction['positive']

            key, lower = self.resolve_lower_call(key)
            prop = self.resolve_property(key, sort=True)
            field = self.get_sql_field(prop, lower)

            main_table = self.backend.get_table(self.model)
            if prop.list is not None:
                list_table = self.backend.get_table(prop.list, TableType.LIST)
                subqry = (
                    sa.select(
                        [list_table.c._rid, field.label('value')],
                        distinct=list_table.c._rid,
                    ).alias()
                )
                self.joins = self.joins.outerjoin(
                    subqry,
                    main_table.c._id == subqry.c._rid,
                )
                field = subqry.c.value
            else:
                field = main_table.c[prop.place]

            if lower:
                field = sa.func.lower(field)

            field = d(field)
            fields.append(field)

        return qry.order_by(*fields)


def _getall_offset(qry: sa.sql.Select, offset: Optional[int]) -> sa.sql.Select:
    if offset:
        return qry.offset(offset)
    else:
        return qry


def _getall_limit(qry: sa.sql.Select, limit: Optional[int]) -> sa.sql.Select:
    if limit:
        return qry.limit(limit)
    else:
        return qry
