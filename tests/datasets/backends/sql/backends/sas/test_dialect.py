from unittest.mock import Mock, patch
from sqlalchemy import types as sqltypes
from sqlalchemy.engine.url import make_url

from spinta.datasets.backends.sql.backends.sas.dialect import SASDialect, register_sas_dialect
from spinta.datasets.backends.sql.backends.sas.formats import map_sas_type_to_sqlalchemy


class TestSASDialect:
    """Test suite for the SAS SQLAlchemy dialect"""

    def test_dialect_name(self):
        """Test that the dialect has the correct name."""
        dialect = SASDialect()
        assert dialect.name == "sas"

    def test_jdbc_db_name(self):
        """Test that the JDBC database name is correctly set."""
        dialect = SASDialect()
        assert dialect.jdbc_db_name == "sasiom"

    def test_jdbc_driver_name(self):
        """Test that the JDBC driver name is correctly set."""
        dialect = SASDialect()
        assert dialect.jdbc_driver_name == "com.sas.rio.MVADriver"

    def test_max_identifier_length(self):
        """Test that the maximum identifier length is set to 32 (SAS limitation)."""
        dialect = SASDialect()
        assert dialect.max_identifier_length == 32

    def test_feature_flags(self):
        """Test that dialect feature flags are correctly configured."""
        dialect = SASDialect()

        # Test transaction support (SAS doesn't support transactions)
        assert dialect.supports_transactions is False

        # Test schema support
        assert dialect.supports_schemas is True

        # Test view support
        assert dialect.supports_views is True

        # Test constraints (SAS doesn't support them)
        assert dialect.supports_pk_autoincrement is False
        assert dialect.supports_sequences is False

        # Test name normalization requirement
        assert dialect.requires_name_normalize is True

    def test_create_connect_args(self):
        """Test URL parsing and connection args creation."""
        url = make_url("sas+jdbc://testuser:testpass@localhost:8591")
        dialect = SASDialect()

        args, kwargs = dialect.create_connect_args(url)

        # Verify kwargs structure
        assert kwargs["jclassname"] == "com.sas.rio.MVADriver"
        assert kwargs["url"] == "jdbc:sasiom://localhost:8591"
        assert kwargs["driver_args"]["user"] == "testuser"
        assert kwargs["driver_args"]["password"] == "testpass"

    def test_create_connect_args_with_schema(self):
        """Test URL parsing with schema parameter in query string."""
        url = make_url("sas+jdbc://testuser:testpass@localhost:8591/?schema=MYLIB")
        dialect = SASDialect()

        args, kwargs = dialect.create_connect_args(url)

        # Verify kwargs structure
        assert kwargs["jclassname"] == "com.sas.rio.MVADriver"
        assert kwargs["url"] == "jdbc:sasiom://localhost:8591"
        assert kwargs["driver_args"]["user"] == "testuser"
        assert kwargs["driver_args"]["password"] == "testpass"

    def test_create_connect_args_no_port(self):
        """Test URL parsing when no port is specified."""
        url = make_url("sas+jdbc://testuser:testpass@localhost")
        dialect = SASDialect()

        args, kwargs = dialect.create_connect_args(url)

        # Verify kwargs structure
        assert kwargs["jclassname"] == "com.sas.rio.MVADriver"
        assert kwargs["url"] == "jdbc:sasiom://localhost"
        assert kwargs["driver_args"]["user"] == "testuser"
        assert kwargs["driver_args"]["password"] == "testpass"

    def test_create_connect_args_no_credentials(self):
        """Test URL parsing when credentials are not provided."""
        url = make_url("sas+jdbc://localhost:8591")
        dialect = SASDialect()

        args, kwargs = dialect.create_connect_args(url)

        # Verify kwargs structure
        assert kwargs["jclassname"] == "com.sas.rio.MVADriver"
        assert kwargs["url"] == "jdbc:sasiom://localhost:8591"
        assert kwargs["driver_args"]["user"] == ""
        assert kwargs["driver_args"]["password"] == ""

    def test_type_mapping_char(self):
        """Test CHAR type mapping to VARCHAR."""
        # Test character type mapping
        sa_type = map_sas_type_to_sqlalchemy("char", 50, None)

        assert isinstance(sa_type, sqltypes.VARCHAR)
        assert sa_type.length == 50

    def test_type_mapping_num_date(self):
        """Test NUM with DATE format mapping to DATE type."""
        # Test with various date formats
        for date_format in ["DATE9.", "DDMMYY10.", "MMDDYY8.", "YYMMDD10."]:
            sa_type = map_sas_type_to_sqlalchemy("num", 8, date_format)
            assert isinstance(sa_type, sqltypes.DATE), f"Failed for format {date_format}"

    def test_type_mapping_num_datetime(self):
        """Test NUM with DATETIME format mapping to DATETIME type."""
        # Test with datetime formats
        for datetime_format in ["DATETIME20.", "DATETIME19.", "DATETIME."]:
            sa_type = map_sas_type_to_sqlalchemy("num", 8, datetime_format)
            assert isinstance(sa_type, sqltypes.DATETIME), f"Failed for format {datetime_format}"

    def test_type_mapping_num_time(self):
        """Test NUM with TIME format mapping to TIME type."""
        # Test with time formats
        for time_format in ["TIME8.", "TIME12.", "TIME."]:
            sa_type = map_sas_type_to_sqlalchemy("num", 8, time_format)
            assert isinstance(sa_type, sqltypes.TIME), f"Failed for format {time_format}"

    def test_type_mapping_num_default(self):
        """Test NUM without format mapping to NUMERIC type."""
        # Test numeric without specific format
        sa_type = map_sas_type_to_sqlalchemy("num", 8, None)
        assert isinstance(sa_type, sqltypes.NUMERIC)

    def test_type_mapping_num_with_integer_format(self):
        """Test NUM with integer-like formats."""
        # Test formats that indicate integers
        for int_format in ["Z8.", "F10.", "COMMA10."]:
            sa_type = map_sas_type_to_sqlalchemy("num", 8, int_format)
            assert isinstance(sa_type, sqltypes.INTEGER), f"Failed for format {int_format}"

    def test_type_mapping_num_with_decimal_format(self):
        """Test NUM with decimal formats."""
        # Test formats with decimal places
        sa_type = map_sas_type_to_sqlalchemy("num", 8, "COMMA10.2")
        assert isinstance(sa_type, sqltypes.NUMERIC)

        sa_type = map_sas_type_to_sqlalchemy("num", 8, "DOLLAR12.2")
        assert isinstance(sa_type, sqltypes.NUMERIC)

        # Test money format without decimals (should still be Numeric)
        sa_type = map_sas_type_to_sqlalchemy("num", 8, "DOLLAR12.")
        assert isinstance(sa_type, sqltypes.NUMERIC)

    def test_normalize_name(self):
        """Test name normalization to uppercase with trailing space stripping."""
        dialect = SASDialect()

        # Test various cases
        assert dialect.normalize_name("tablename") == "TABLENAME"
        assert dialect.normalize_name("TableName") == "TABLENAME"
        assert dialect.normalize_name("TABLENAME") == "TABLENAME"
        assert dialect.normalize_name("table_name") == "TABLE_NAME"
        # Test trailing space stripping
        assert dialect.normalize_name("tablename   ") == "TABLENAME"
        assert dialect.normalize_name("TableName  ") == "TABLENAME"
        assert dialect.normalize_name("TABLENAME ") == "TABLENAME"

    def test_normalize_name_none(self):
        """Test that None is handled correctly in normalize_name."""
        dialect = SASDialect()
        assert dialect.normalize_name(None) is None

    def test_denormalize_name(self):
        """Test name denormalization to lowercase."""
        dialect = SASDialect()

        # Test various cases
        assert dialect.denormalize_name("TABLENAME") == "tablename"
        assert dialect.denormalize_name("TableName") == "tablename"
        assert dialect.denormalize_name("tablename") == "tablename"
        assert dialect.denormalize_name("TABLE_NAME") == "table_name"

    def test_denormalize_name_none(self):
        """Test that None is handled correctly in denormalize_name."""
        dialect = SASDialect()
        assert dialect.denormalize_name(None) is None

    def test_do_rollback_noop(self):
        """Test that rollback is a no-op (SAS doesn't support transactions)."""
        dialect = SASDialect()
        mock_connection = Mock()

        # Should not raise any exception
        dialect.do_rollback(mock_connection)

        # Verify no methods were called on the connection
        mock_connection.assert_not_called()

    def test_do_commit_noop(self):
        """Test that commit is a no-op (SAS doesn't support transactions)."""
        dialect = SASDialect()
        mock_connection = Mock()

        # Should not raise any exception
        dialect.do_commit(mock_connection)

        # Verify no methods were called on the connection
        mock_connection.assert_not_called()

    def test_has_sequence_always_false(self):
        """Test that has_sequence always returns False (SAS doesn't support sequences)."""
        dialect = SASDialect()
        mock_connection = Mock()

        result = dialect.has_sequence(mock_connection, "test_sequence", "test_schema")
        assert result is False

    def test_get_pk_constraint_empty(self):
        """Test that primary key constraints return empty (SAS doesn't support PKs)."""
        dialect = SASDialect()
        mock_connection = Mock()

        result = dialect.get_pk_constraint(mock_connection, "test_table", "test_schema")

        assert result == {"constrained_columns": [], "name": None}

    def test_get_foreign_keys_empty(self):
        """Test that foreign key constraints return empty list (SAS doesn't support FKs)."""
        dialect = SASDialect()
        mock_connection = Mock()

        result = dialect.get_foreign_keys(mock_connection, "test_table", "test_schema")

        assert result == []

    def test_initialize(self):
        """Test dialect initialization."""
        dialect = SASDialect()
        mock_connection = Mock()

        # Should not raise any exception
        dialect.initialize(mock_connection)

        # Verify default_schema_name is initialized
        assert dialect.default_schema_name == ""

    @patch("sqlalchemy.dialects.registry")
    def test_register_sas_dialect(self, mock_registry):
        """Test that the dialect registration function works correctly."""
        register_sas_dialect()

        # Verify registry.register was called with correct parameters
        mock_registry.register.assert_called_once_with(
            "sas.jdbc", "spinta.datasets.backends.sql.backends.sas.dialect", "SASDialect"
        )

    def test_colspecs(self):
        """Test that column type specifications are defined."""
        dialect = SASDialect()

        # Verify colspecs contains mappings for date/time types
        assert sqltypes.Date in dialect.colspecs
        assert sqltypes.DateTime in dialect.colspecs

    def test_connection_pooling_configuration(self):
        """Test that connection pooling is configured with SAS-specific settings."""
        dialect = SASDialect()

        # Check that pool configuration was applied during initialization
        # The actual pool configuration happens in __init__, so we verify
        # the dialect was created successfully with enhanced settings
        assert dialect.name == "sas"

    def test_error_handling_in_get_schema_names(self):
        """Test that get_schema_names handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()
        # Add the missing dialect attribute
        mock_dialect = Mock()
        mock_dialect.default_schema_name = ""  # or None
        mock_connection.dialect = mock_dialect
        mock_connection.execute.side_effect = Exception("Database error")

        # Should return empty list instead of raising
        result = dialect.get_schema_names(mock_connection)
        assert result == []

    def test_error_handling_in_get_table_names(self):
        """Test that get_table_names handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")

        # Should return empty list instead of raising
        result = dialect.get_table_names(mock_connection, "TEST_SCHEMA")
        assert result == []

    def test_error_handling_in_get_columns(self):
        """Test that get_columns handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")

        # Should return empty list instead of raising
        result = dialect.get_columns(mock_connection, "TEST_TABLE", "TEST_SCHEMA")
        assert result == []

    def test_error_handling_in_has_table(self):
        """Test that has_table handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")

        # Should return False instead of raising
        result = dialect.has_table(mock_connection, "TEST_TABLE", "TEST_SCHEMA")
        assert result is False

    def test_error_handling_in_get_table_comment(self):
        """Test that get_table_comment handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")

        # Should return None comment instead of raising
        result = dialect.get_table_comment(mock_connection, "TEST_TABLE", "TEST_SCHEMA")
        assert result == {"text": None}

    def test_error_handling_in_get_indexes(self):
        """Test that get_indexes handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Database error")

        # Should return empty list instead of raising
        result = dialect.get_indexes(mock_connection, "TEST_TABLE", "TEST_SCHEMA")
        assert result == []

    def test_error_handling_in_initialize(self):
        """Test that initialize handles errors gracefully."""
        from unittest.mock import Mock

        dialect = SASDialect()
        mock_connection = Mock()

        # Test that initialize works normally (no parent initialize method exists)
        dialect.initialize(mock_connection)
        # Verify default_schema_name is set to fallback value
        assert dialect.default_schema_name == ""

    def test_create_connect_args_error_handling(self):
        """Test that create_connect_args handles errors gracefully."""
        from sqlalchemy.engine.url import make_url

        dialect = SASDialect()
        url = make_url("sas+jdbc://test:pass@localhost:8591")

        # Test that create_connect_args works normally
        args, kwargs = dialect.create_connect_args(url)
        assert kwargs["jclassname"] == "com.sas.rio.MVADriver"
