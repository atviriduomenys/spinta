"""
SAS Backend Constants and Shared Utilities.

This module provides centralized constants and helper functions for the SAS backend,
including epoch definitions, missing value thresholds, and common validation logic.
"""

from datetime import date, datetime
import logging

logger = logging.getLogger(__name__)

# SAS epoch: January 1, 1960
SAS_EPOCH_DATE = date(1960, 1, 1)
SAS_EPOCH_DATETIME = datetime(1960, 1, 1, 0, 0, 0)

# SAS Missing Values
# Standard SAS missing value "." is approximately -1.797693e+308
# We use a threshold to detect these and special missing values (.A-.Z, ._)
SAS_MISSING_VALUE_THRESHOLD = -1e10


def is_sas_missing_value(value) -> bool:
    """
    Check if a value represents a SAS missing value.

    SAS uses special values for missing data:
    - NaN for numeric missing values (often from Java double conversion)
    - Very large negative numbers (< -1e10) for special missing values
      (Standard SAS missing value "." is approximately -1.797693e+308)
    - Infinity values

    Args:
        value: The value to check (numeric or string)

    Returns:
        True if the value represents a SAS missing value, False otherwise
    """
    if value is None:
        return True

    try:
        # Check for numeric missing values
        float_val = float(value)

        # Check for NaN (Not a Number)
        if float_val != float_val:
            return True

        # Check for Infinity
        if float_val == float("inf") or float_val == float("-inf"):
            return True

        # Check for SAS missing value threshold
        if float_val < SAS_MISSING_VALUE_THRESHOLD:
            return True

        return False

    except (ValueError, TypeError):
        # If it's not a number, check for string representations of missing values
        if isinstance(value, str):
            val_str = value.strip()
            # Standard missing value '.'
            if val_str == ".":
                return True
            # Special missing values .A - .Z, ._
            if len(val_str) == 2 and val_str.startswith("."):
                suffix = val_str[1]
                if suffix == "_" or ("A" <= suffix.upper() <= "Z"):
                    return True

        return False
