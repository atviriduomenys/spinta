"""
SAS Helper Functions

This module provides utility functions for working with SAS-specific data types
and operations within the Spinta framework.

SAS uses a different epoch date (January 1, 1960) compared to most systems,
requiring special conversion functions for date, datetime, and time values.
"""

from collections.abc import Sequence
from datetime import date, datetime, time, timedelta

import sqlalchemy as sa


# SAS epoch: January 1, 1960
SAS_EPOCH_DATE = date(1960, 1, 1)


def sas_date_to_python(sas_date_value):
    """
    Convert SAS date value to Python date object.

    SAS stores dates as days since January 1, 1960.

    Args:
        sas_date_value: Days since SAS epoch (1960-01-01), or None

    Returns:
        Python date object, or None if input is None

    Raises:
        ValueError: If the date value is invalid
    """
    if sas_date_value is None:
        return None

    try:
        return SAS_EPOCH_DATE + timedelta(days=int(sas_date_value))
    except (ValueError, OverflowError, TypeError) as e:
        raise ValueError(f"Invalid SAS date value: {sas_date_value}") from e


def sas_datetime_to_python(sas_datetime_value):
    """
    Convert SAS datetime value to Python datetime object.

    SAS stores datetimes as seconds since January 1, 1960 00:00:00.

    Args:
        sas_datetime_value: Seconds since SAS epoch (1960-01-01 00:00:00), or None

    Returns:
        Python datetime object, or None if input is None

    Raises:
        ValueError: If the datetime value is invalid
    """
    if sas_datetime_value is None:
        return None

    try:
        sas_epoch_datetime = datetime(1960, 1, 1, 0, 0, 0)
        return sas_epoch_datetime + timedelta(seconds=float(sas_datetime_value))
    except (ValueError, OverflowError, TypeError) as e:
        raise ValueError(f"Invalid SAS datetime value: {sas_datetime_value}") from e


def sas_time_to_python(sas_time_value):
    """
    Convert SAS time value to Python time object.

    SAS stores times as seconds since midnight (00:00:00).

    Args:
        sas_time_value: Seconds since midnight (0-86400), or None

    Returns:
        Python time object, or None if input is None

    Raises:
        ValueError: If the time value is invalid or out of range
    """
    if sas_time_value is None:
        return None

    try:
        seconds = float(sas_time_value)
        if not 0 <= seconds < 86400:
            raise ValueError(f"Time value must be between 0 and 86400 seconds, got {seconds}")

        hours = int(seconds // 3600)
        remaining = seconds % 3600
        minutes = int(remaining // 60)
        remaining = remaining % 60
        secs = int(remaining)
        microsecs = int((remaining - secs) * 1_000_000)

        return time(hours, minutes, secs, microsecs)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid SAS time value: {sas_time_value}") from e


def group_array(column: sa.Column | Sequence[sa.Column]):
    """
    Create an array aggregation expression for SAS using CATX function.

    SAS does not natively support array aggregation like PostgreSQL's array_agg.
    This uses SAS's CATX function for comma-separated string concatenation.

    Args:
        column: SQLAlchemy Column or sequence of Columns to aggregate

    Returns:
        SQLAlchemy expression: CATX(',', column(s))

    Note:
        - NULL values are automatically ignored by CATX
        - Result is a comma-separated string, not a true array type
    """
    columns = column if isinstance(column, Sequence) and not isinstance(column, str) else [column]
    return sa.func.catx(",", *columns)
