"""
SAS Custom Type Decorators for SQLAlchemy.

This module provides custom SQLAlchemy type decorators for handling SAS-specific
data types and their conversions to Python objects.

SAS has unique data storage formats that require special handling:
- Dates are stored as the number of days since January 1, 1960
- Datetimes are stored as the number of seconds since January 1, 1960 00:00:00
- Times are stored as the number of seconds since midnight
- Character fields are often padded with spaces

These type decorators handle the conversion between SAS internal formats
and Python native types.
"""

import logging
from datetime import time, timedelta
from sqlalchemy import types as sqltypes

from spinta.datasets.backends.sql.backends.sas.constants import SAS_EPOCH_DATE, SAS_EPOCH_DATETIME, is_sas_missing_value

logger = logging.getLogger(__name__)


class SASStringType(sqltypes.TypeDecorator):
    """
    Enhanced string type for SAS that handles encoding, space stripping, and missing values.

    SAS often pads character fields with spaces, so this type ensures
    that returned string values have trailing spaces removed. Also handles
    encoding issues and missing value detection.
    """

    impl = sqltypes.VARCHAR
    cache_ok = True

    def __init__(self, length=None, strip_spaces=True, **kwargs):
        """
        Initialize the SAS string type.

        Args:
            length: Maximum string length
            strip_spaces: Whether to strip leading/trailing spaces (default: True)
            **kwargs: Additional arguments passed to VARCHAR
        """
        super().__init__(**kwargs)
        self.length = length
        self.strip_spaces = strip_spaces
        if length:
            self.impl = sqltypes.VARCHAR(length=length)

    def process_result_value(self, value, dialect):
        """
        Process the result value with enhanced string handling.

        Args:
            value: The raw value from the database
            dialect: The dialect instance

        Returns:
            The processed value with proper encoding and space handling
        """
        if value is None:
            return None

        # Handle empty strings as potential missing values
        if isinstance(value, str):
            if self.strip_spaces:
                value = value.strip()

            # Empty strings after stripping could be missing values
            if not value:
                return None

            # Handle encoding issues - ensure proper string encoding
            try:
                # Ensure the string is properly decoded
                if isinstance(value, bytes):
                    value = value.decode("utf-8", errors="replace")
            except (UnicodeDecodeError, AttributeError):
                pass

            # Check for SAS missing value string representations
            if is_sas_missing_value(value):
                logger.debug(f"Detected SAS missing value in string: {value}")
                return None

            return value

        return str(value) if value else None


class SASDateType(sqltypes.TypeDecorator):
    """SAS Date type that converts numeric days since 1960-01-01 to Python date objects."""

    impl = sqltypes.DATE
    cache_ok = True

    def process_result_value(self, value, dialect):
        """Convert SAS numeric date value to Python date object."""
        if value is None or is_sas_missing_value(value):
            return None

        try:
            days = int(float(value))
            return SAS_EPOCH_DATE + timedelta(days=days)
        except (ValueError, OverflowError, TypeError) as e:
            logger.warning(f"Invalid SAS date value: {value} - {e}")
            return None


class SASDateTimeType(sqltypes.TypeDecorator):
    """SAS DateTime type that converts numeric seconds since 1960-01-01 to Python datetime objects."""

    impl = sqltypes.DATETIME
    cache_ok = True

    def process_result_value(self, value, dialect):
        """Convert SAS numeric datetime value to Python datetime object."""
        if value is None or is_sas_missing_value(value):
            return None

        try:
            seconds = float(value)
            return SAS_EPOCH_DATETIME + timedelta(seconds=seconds)
        except (ValueError, OverflowError, TypeError) as e:
            logger.warning(f"Invalid SAS datetime value: {value} - {e}")
            return None


class SASTimeType(sqltypes.TypeDecorator):
    """SAS Time type that converts numeric seconds to Python time objects."""

    impl = sqltypes.TIME
    cache_ok = True

    def process_result_value(self, value, dialect):
        """Convert SAS numeric time value to Python time object."""
        if value is None or is_sas_missing_value(value):
            return None

        try:
            total_seconds = int(float(value))
            hours = (total_seconds // 3600) % 24
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            return time(hours, minutes, seconds)
        except (ValueError, OverflowError, TypeError) as e:
            logger.warning(f"Invalid SAS time value: {value} - {e}")
            return None
