import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.types.datatype import ExternalRef, Denorm
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_column_name


@commands.prepare.register(Context, PostgreSQL, ExternalRef)
def prepare(context: Context, backend: PostgreSQL, dtype: ExternalRef, propagate=True, **kwargs):
    columns = []
    if not dtype.inherited:
        if dtype.model.given.pkeys or dtype.explicit:
            for prop in dtype.refprops:
                # Skip if refprop is already defined in properties, but is not Denorm type
                if prop.name in dtype.properties.keys():
                    if not isinstance(dtype.properties[prop.name].dtype, Denorm):
                        continue

                original_place = prop.place
                prop.place = f"{dtype.prop.place}.{prop.place}"
                if dtype.prop.list and dtype.prop.place == dtype.prop.list.place:
                    prop.place = original_place
                column = commands.prepare(
                    context,
                    backend,
                    prop.dtype,
                )
                if isinstance(column, list):
                    columns.extend(column)
                elif column is not None:
                    columns.append(column)
                prop.place = original_place
            if dtype.unique:
                extracted_columns = [column for column in columns if isinstance(column, sa.Column)]
                unique_constraint = sa.UniqueConstraint(
                    *extracted_columns
                )
                columns.append(unique_constraint)

        else:
            column_name = get_column_name(dtype.prop) + '._id'
            if dtype.prop.list and dtype.prop.place == dtype.prop.list.place:
                column_name = '_id'
            column_type = commands.get_primary_key_type(context, backend)
            columns.append(sa.Column(
                column_name,
                column_type,
                nullable=not dtype.required,
                unique=dtype.unique,
            ))

    if propagate:
        for key, value in dtype.properties.items():
            res = commands.prepare(context, backend, value, **kwargs)
            if res is not None:
                if isinstance(res, list):
                    columns.extend(res)
                else:
                    columns.append(res)
    return columns
