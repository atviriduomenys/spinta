from typing import Any

from multipledispatch import dispatch
from sqlalchemy.cimmutabledict import immutabledict

from spinta.backends.constants import TableType
from spinta.utils.sqlalchemy import Convention
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.cli.helpers.migrate import MigrateRename
from spinta.utils.itertools import ensure_list


class _PgNamingConvention(str):

    def __mod__(self, other) -> str:
        name = super().__mod__(other)
        return get_pg_name(name)


# https://docs.sqlalchemy.org/en/14/core/constraints.html#configuring-a-naming-convention-for-a-metadata-collection
PG_NAMING_CONVENTION = immutabledict({
    Convention.IX: _PgNamingConvention("ix_%(table_name)s_%(column_0_N_name)s"),
    Convention.UQ: _PgNamingConvention("uq_%(table_name)s_%(column_0_N_name)s"),
    Convention.FK: _PgNamingConvention("fk_%(table_name)s_%(column_0_N_name)s"),
    Convention.CK: _PgNamingConvention("ck_%(table_name)s_%(constraint_name)s"),
    Convention.PK: _PgNamingConvention("pk_%(table_name)s"),
})


@dispatch(str, str)
def name_changed(
    old_name: str,
    new_name: str
) -> bool:
    return old_name != new_name


@dispatch(str, str, str, str)
def name_changed(
    old_table_name: str,
    new_table_name: str,
    old_property_name: str,
    new_property_name: str
) -> bool:
    return old_table_name != new_table_name or old_property_name != new_property_name


def get_pg_table_name(table_name: str) -> str:
    return get_pg_name(table_name)


def get_pg_changelog_name(table_name: str) -> str:
    return get_pg_name(f"{table_name}{TableType.CHANGELOG.value}")


def get_pg_file_name(table_name: str, arg: str = "") -> str:
    if arg and not arg.startswith('/'):
        arg = f'/{arg}'

    return get_pg_name(f"{table_name}{TableType.FILE.value}{arg}")


def get_pg_column_name(column_name: str) -> str:
    return get_pg_name(column_name)


def get_pg_constraint_name(table_name: str, columns: Any) -> str:
    column_names = ensure_list(columns)
    return PG_NAMING_CONVENTION[Convention.UQ] % {
        "table_name": table_name,
        "column_0_N_name": '_'.join(column_names)
    }


def get_pg_removed_name(name: str) -> str:
    new_name = name.split("/")
    if not new_name[-1].startswith("__"):
        new_name[-1] = f'__{new_name[-1]}'
    new_name = '/'.join(new_name)
    new_name = get_pg_name(new_name)
    return new_name


def get_pg_foreign_key_name(table_name: str, column_name: str) -> str:
    return PG_NAMING_CONVENTION[Convention.FK] % {
        "table_name": table_name,
        "column_0_N_name": column_name.removeprefix('_')
    }


def get_pg_index_name(table_name: str, columns: Any) -> str:
    column_names = ensure_list(columns)
    return PG_NAMING_CONVENTION[Convention.IX] % {
        "table_name": table_name,
        "column_0_N_name": '_'.join(column_names)
    }


def is_removed(name: str) -> bool:
    if '/' in name:
        last = name.split('/')[-1]
        return last.startswith('__')
    return name.startswith('__')


def nested_column_rename(column_name: str, table_name: str, rename: MigrateRename) -> str:
    renamed = rename.get_column_name(table_name, column_name)
    if name_changed(column_name, renamed):
        return renamed

    if '.' in column_name:
        split_name = column_name.split('.')
        renamed = nested_column_rename('.'.join(split_name[:-1]), table_name, rename)
        return f'{renamed}.{split_name[-1]}'

    return column_name
