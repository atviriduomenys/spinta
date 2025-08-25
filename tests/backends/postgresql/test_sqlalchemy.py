from unittest.mock import Mock


def test_mssql_geometry_type():
    from geoalchemy2.types import Geometry
    from sqlalchemy.dialects.mssql.base import MSDialect
    from sqlalchemy.dialects.mssql import information_schema as ischema

    # This import registers 'geometry' type, so it mus be here even if not
    # used.
    import spinta.backends.postgresql.sqlalchemy  # noqa

    dialect = MSDialect()
    dialect._supports_nvarchar_max = True
    connection = Mock()
    (connection.execution_options.return_value.execute.return_value.mappings.return_value) = [
        {
            ischema.columns.c.column_name: "test_column",
            ischema.columns.c.data_type: "geometry",
            ischema.columns.c.is_nullable: "YES",
            ischema.columns.c.character_maximum_length: "",
            ischema.columns.c.numeric_precision: "",
            ischema.columns.c.numeric_scale: "",
            ischema.columns.c.column_default: "",
            ischema.columns.c.collation_name: "",
            ischema.computed_columns.c.definition: "",
            ischema.computed_columns.c.is_persisted: "",
            ischema.identity_columns.c.is_identity: "",
            ischema.identity_columns.c.seed_value: "",
            ischema.identity_columns.c.increment_value: "",
        }
    ]
    tablename = "test_table"
    (column,) = dialect.get_columns(connection, tablename)
    assert isinstance(column["type"], Geometry)
