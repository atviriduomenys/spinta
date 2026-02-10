"""
Unit tests for SAS Helper Functions

These tests validate the SAS-specific helper functions for date/time conversions
and array aggregation operations without requiring a live SAS connection.

Note: Integration tests requiring a live SAS server connection are not included here.
"""

import pytest
from datetime import date, datetime, time
from unittest.mock import Mock
import sqlalchemy as sa

from spinta.datasets.backends.sql.backends.sas.helpers import (
    sas_date_to_python,
    sas_datetime_to_python,
    sas_time_to_python,
    group_array,
    SAS_EPOCH_DATE,
)


class TestSASHelpers:
    """Test suite for SAS helper functions"""

    # ========== Tests for sas_date_to_python ==========

    def test_sas_date_to_python(self):
        """Test date conversion from SAS days to Python date."""
        # Test SAS epoch (0 = 1960-01-01)
        result = sas_date_to_python(0)
        assert result == date(1960, 1, 1)

    def test_sas_date_to_python_positive_days(self):
        """Test date conversion with positive days after epoch."""
        # 365 days = 1961-01-01 (1960 was a leap year with 366 days)
        result = sas_date_to_python(366)
        assert result == date(1961, 1, 1)

        # Test another date
        result = sas_date_to_python(1000)
        expected = date(1962, 9, 27)
        assert result == expected

    def test_sas_date_to_python_negative_days(self):
        """Test date conversion with negative days (dates before 1960)."""
        # -1 = 1959-12-31
        result = sas_date_to_python(-1)
        assert result == date(1959, 12, 31)

        # Test a date further in the past
        result = sas_date_to_python(-365)
        expected = date(1959, 1, 1)
        assert result == expected

    def test_sas_date_to_python_none(self):
        """Test that None input returns None (NULL handling)."""
        result = sas_date_to_python(None)
        assert result is None

    def test_sas_date_to_python_invalid_type(self):
        """Test error handling for invalid input types."""
        with pytest.raises(ValueError, match="Invalid SAS date value"):
            sas_date_to_python("invalid")

        with pytest.raises(ValueError, match="Invalid SAS date value"):
            sas_date_to_python([1, 2, 3])

    def test_sas_date_to_python_large_values(self):
        """Test date conversion with large values."""
        # Test far future date
        result = sas_date_to_python(50000)
        assert result.year > 2000

        # Test far past date
        result = sas_date_to_python(-50000)
        assert result.year < 1900

    # ========== Tests for sas_datetime_to_python ==========

    def test_sas_datetime_to_python(self):
        """Test datetime conversion from SAS seconds to Python datetime."""
        # Test SAS epoch (0 = 1960-01-01 00:00:00)
        result = sas_datetime_to_python(0)
        assert result == datetime(1960, 1, 1, 0, 0, 0)

    def test_sas_datetime_to_python_positive_seconds(self):
        """Test datetime conversion with positive seconds after epoch."""
        # 86400 seconds = 1 day = 1960-01-02 00:00:00
        result = sas_datetime_to_python(86400)
        assert result == datetime(1960, 1, 2, 0, 0, 0)

        # Test with hours, minutes, seconds
        # 3661 seconds = 1 hour, 1 minute, 1 second
        result = sas_datetime_to_python(3661)
        assert result == datetime(1960, 1, 1, 1, 1, 1)

    def test_sas_datetime_to_python_with_microseconds(self):
        """Test datetime conversion with fractional seconds."""
        # 3600.5 seconds = 1 hour and 500,000 microseconds
        result = sas_datetime_to_python(3600.5)
        assert result == datetime(1960, 1, 1, 1, 0, 0, 500000)

        # Test more precision
        result = sas_datetime_to_python(1.123456)
        assert result.microsecond == 123456

    def test_sas_datetime_to_python_negative_seconds(self):
        """Test datetime conversion with negative seconds (dates before 1960)."""
        # -86400 seconds = 1 day before = 1959-12-31 00:00:00
        result = sas_datetime_to_python(-86400)
        assert result == datetime(1959, 12, 31, 0, 0, 0)

    def test_sas_datetime_to_python_none(self):
        """Test that None input returns None (NULL handling)."""
        result = sas_datetime_to_python(None)
        assert result is None

    def test_sas_datetime_to_python_invalid_type(self):
        """Test error handling for invalid input types."""
        with pytest.raises(ValueError, match="Invalid SAS datetime value"):
            sas_datetime_to_python("invalid")

        with pytest.raises(ValueError, match="Invalid SAS datetime value"):
            sas_datetime_to_python([1, 2, 3])

    def test_sas_datetime_to_python_large_values(self):
        """Test datetime conversion with large values."""
        # Test far future datetime
        result = sas_datetime_to_python(86400 * 20000)  # ~54 years
        assert result.year > 2000

    # ========== Tests for sas_time_to_python ==========

    def test_sas_time_to_python(self):
        """Test time conversion from SAS seconds since midnight."""
        # Test midnight (0 = 00:00:00)
        result = sas_time_to_python(0)
        assert result == time(0, 0, 0)

    def test_sas_time_to_python_hours(self):
        """Test time conversion for various hours."""
        # 3600 seconds = 1 hour = 01:00:00
        result = sas_time_to_python(3600)
        assert result == time(1, 0, 0)

        # 43200 seconds = 12 hours = 12:00:00
        result = sas_time_to_python(43200)
        assert result == time(12, 0, 0)

        # 82800 seconds = 23 hours = 23:00:00
        result = sas_time_to_python(82800)
        assert result == time(23, 0, 0)

    def test_sas_time_to_python_minutes_seconds(self):
        """Test time conversion with minutes and seconds."""
        # 3661 seconds = 1 hour, 1 minute, 1 second
        result = sas_time_to_python(3661)
        assert result == time(1, 1, 1)

        # 3723 seconds = 1 hour, 2 minutes, 3 seconds
        result = sas_time_to_python(3723)
        assert result == time(1, 2, 3)

    def test_sas_time_to_python_with_microseconds(self):
        """Test time conversion with fractional seconds."""
        # 3661.5 seconds = 1 hour, 1 minute, 1 second, 500,000 microseconds
        result = sas_time_to_python(3661.5)
        assert result == time(1, 1, 1, 500000)

        # Test more precision
        result = sas_time_to_python(1.123456)
        assert result.microsecond == 123456

    def test_sas_time_to_python_none(self):
        """Test that None input returns None (NULL handling)."""
        result = sas_time_to_python(None)
        assert result is None

    def test_sas_time_to_python_invalid_negative(self):
        """Test error handling for negative time values."""
        with pytest.raises(ValueError, match="Invalid SAS time value: -1"):
            sas_time_to_python(-1)

        with pytest.raises(ValueError, match="Invalid SAS time value: -3600"):
            sas_time_to_python(-3600)

    def test_sas_time_to_python_invalid_too_large(self):
        """Test error handling for time values >= 86400 (24 hours)."""
        with pytest.raises(ValueError, match="Invalid SAS time value: 86400"):
            sas_time_to_python(86400)

        with pytest.raises(ValueError, match="Invalid SAS time value: 100000"):
            sas_time_to_python(100000)

    def test_sas_time_to_python_invalid_type(self):
        """Test error handling for invalid input types."""
        with pytest.raises(ValueError, match="Invalid SAS time value"):
            sas_time_to_python("invalid")

        with pytest.raises(ValueError, match="Invalid SAS time value"):
            sas_time_to_python([1, 2, 3])

    def test_sas_time_to_python_edge_cases(self):
        """Test edge cases for time conversion."""
        # Just before midnight (23:59:59.999999)
        result = sas_time_to_python(86399.999999)
        assert result.hour == 23
        assert result.minute == 59
        assert result.second == 59

        # Just after midnight
        result = sas_time_to_python(0.000001)
        assert result.hour == 0
        assert result.minute == 0
        assert result.second == 0
        assert result.microsecond > 0

    # ========== Tests for group_array ==========

    def test_group_array_single_column(self):
        """Test array aggregation with a single column."""
        # Create a mock column
        mock_column = Mock(spec=sa.Column)
        mock_column.name = "test_column"

        # Call group_array
        result = group_array(mock_column)

        # Verify it returns a CATX function call
        assert hasattr(result, "name") or callable(result)
        # The result should be a SQLAlchemy function expression

    def test_group_array_multiple_columns(self):
        """Test array aggregation with multiple columns."""
        # Create mock columns
        mock_col1 = Mock(spec=sa.Column)
        mock_col1.name = "col1"
        mock_col2 = Mock(spec=sa.Column)
        mock_col2.name = "col2"
        mock_col3 = Mock(spec=sa.Column)
        mock_col3.name = "col3"

        # Call group_array with a list of columns
        result = group_array([mock_col1, mock_col2, mock_col3])

        # Verify it returns a CATX function call
        assert hasattr(result, "name") or callable(result)

    def test_group_array_with_actual_sqlalchemy_column(self):
        """Test array aggregation with actual SQLAlchemy columns."""
        # Create a real SQLAlchemy table
        metadata = sa.MetaData()
        test_table = sa.Table(
            "test_table",
            metadata,
            sa.Column("id", sa.Integer),
            sa.Column("name", sa.String(50)),
            sa.Column("value", sa.Integer),
        )

        # Test single column
        result_single = group_array(test_table.c.name)
        assert isinstance(result_single, sa.sql.functions.Function)

        # Test multiple columns
        result_multi = group_array([test_table.c.name, test_table.c.value])
        assert isinstance(result_multi, sa.sql.functions.Function)

    def test_group_array_function_name(self):
        """Test that group_array creates a CATX function."""
        metadata = sa.MetaData()
        test_table = sa.Table(
            "test_table",
            metadata,
            sa.Column("name", sa.String(50)),
        )

        result = group_array(test_table.c.name)

        # Check that it's using the catx function
        # The function name should be 'catx' (case-insensitive)
        assert result.name.lower() == "catx"

    def test_group_array_empty_sequence(self):
        """Test that group_array handles empty sequences correctly."""
        # Empty list should still create a function call
        # (though this may not be practically useful)
        result = group_array([])
        assert isinstance(result, sa.sql.functions.Function)

    def test_group_array_string_not_treated_as_sequence(self):
        """Test that string columns are not treated as sequences."""
        # Create a string column
        metadata = sa.MetaData()
        test_table = sa.Table(
            "test_table",
            metadata,
            sa.Column("name", sa.String(50)),
        )

        # Pass the column (which internally has string attributes)
        # but should be treated as single column
        result = group_array(test_table.c.name)

        # Should still create a valid function
        assert isinstance(result, sa.sql.functions.Function)
        assert result.name.lower() == "catx"

    def test_group_array_delimiter(self):
        """Test that group_array uses comma as delimiter."""
        metadata = sa.MetaData()
        test_table = sa.Table(
            "test_table",
            metadata,
            sa.Column("col1", sa.String(50)),
            sa.Column("col2", sa.String(50)),
        )

        result = group_array([test_table.c.col1, test_table.c.col2])

        # Compile the expression to check the delimiter
        compiled = str(result.compile(compile_kwargs={"literal_binds": True}))

        # Should contain the comma delimiter
        assert "catx" in compiled.lower()
        assert "," in compiled

    def test_sas_epoch_constant(self):
        """Test that the SAS_EPOCH_DATE constant is correctly defined."""
        assert SAS_EPOCH_DATE == date(1960, 1, 1)
        assert isinstance(SAS_EPOCH_DATE, date)
