from typing import Any

from multipledispatch import dispatch
from sqlalchemy.cimmutabledict import immutabledict

from spinta.backends.helpers import TableIdentifier
from spinta.utils.sqlalchemy import Convention
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.utils.itertools import ensure_list


class _PgNamingConvention(str):
    def __mod__(self, other) -> str:
        name = super().__mod__(other)
        return get_pg_name(name)


# https://docs.sqlalchemy.org/en/14/core/constraints.html#configuring-a-naming-convention-for-a-metadata-collection
PG_NAMING_CONVENTION = immutabledict(
    {
        Convention.IX: _PgNamingConvention("ix_%(table_name)s_%(column_0_N_name)s"),
        Convention.UQ: _PgNamingConvention("uq_%(table_name)s_%(column_0_N_name)s"),
        Convention.FK: _PgNamingConvention("fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s"),
        Convention.CK: _PgNamingConvention("ck_%(table_name)s_%(constraint_name)s"),
        Convention.PK: _PgNamingConvention("pk_%(table_name)s"),
    }
)


@dispatch((str, type(None)), (str, type(None)))
def name_changed(old_name: str | None, new_name: str | None) -> bool:
    if old_name is None and new_name is None:
        return False

    return old_name != new_name


@dispatch(str, str, str, str)
def name_changed(old_table_name: str, new_table_name: str, old_property_name: str, new_property_name: str) -> bool:
    return old_table_name != new_table_name or old_property_name != new_property_name


def get_pg_column_name(column_name: str) -> str:
    return get_pg_name(column_name)


def get_old_pg_table_name(table_name: str) -> str:
    return get_pg_name(table_name)


def get_pg_constraint_name(table_name: str, columns: Any) -> str:
    column_names = ensure_list(columns)
    return PG_NAMING_CONVENTION[Convention.UQ] % {"table_name": table_name, "column_0_N_name": "_".join(column_names)}


def get_removed_name(name: str, remove_model_only: bool = False) -> str:
    # Check if it is a special table
    special_table = "/:" in name

    split = name.split("/")
    place = -1
    node = split[place]
    if special_table:
        if node.startswith(":"):
            # datasets/data/Model/:redirect <- -2 is Model
            place = -2
            node = split[place]
        elif remove_model_only:
            # dataset/data/Model/:file/new <- -3 is Model
            place = -3
            node = split[place]

    if not node.startswith("__"):
        node = f"__{node}"
    split[place] = node
    new_name = "/".join(split)
    return new_name


def get_pg_removed_name(name: str, remove_model_only: bool = False) -> str:
    return get_pg_name(get_removed_name(name, remove_model_only))


def get_pg_foreign_key_name(
    table_identifier: TableIdentifier,
    referred_table_identifier: TableIdentifier,
    column_name: str,
) -> str:
    referred_table_name = referred_table_identifier.pg_table_name
    if referred_table_identifier.pg_schema_name != table_identifier.pg_schema_name:
        referred_table_name = referred_table_identifier.pg_qualified_name

    return PG_NAMING_CONVENTION[Convention.FK] % {
        "table_name": table_identifier.pg_table_name,
        "column_0_N_name": column_name,
        "referred_table_name": referred_table_name,
    }


def get_pg_index_name(table_name: str, columns: Any) -> str:
    column_names = ensure_list(columns)
    return PG_NAMING_CONVENTION[Convention.IX] % {"table_name": table_name, "column_0_N_name": "_".join(column_names)}


def is_removed(name: str) -> bool:
    if "/" in name:
        last = name.split("/")[-1]
        return last.startswith("__")
    return name.startswith("__")
