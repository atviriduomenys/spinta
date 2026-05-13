"""
SAS Backend Casting Commands

This module provides SAS-specific data type casting from backend to Python types.

SAS databases store CHAR fields with fixed-width padding (spaces on the right).
This module handles stripping that padding when converting SAS strings to Python.
"""

from __future__ import annotations

from typing import Any

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sql.backends.sas.components import SAS
from spinta.utils.types import is_nan
from spinta.types.datatype import String


@commands.cast_backend_to_python.register(Context, String, SAS, str)
def cast_backend_to_python(context: Context, dtype: String, backend: SAS, data: str, **kwargs) -> Any:
    """
    Cast SAS string data to Python string.

    SAS stores CHAR fields with fixed-width padding (spaces on the right).
    This handler strips the trailing whitespace to get the actual string value.

    For VARCHAR fields, no stripping is needed as they don't have padding,
    but stripping trailing spaces is generally safe and expected behavior.

    Args:
        context: Command execution context
        dtype: String data type definition
        backend: SAS backend instance
        data: String data from SAS database
        **kwargs: Additional keyword arguments

    Returns:
        Stripped string or None if data is NaN

    Example:
        SAS CHAR(10) field with value "test" is stored as "test      "
        This handler returns "test"
    """
    if is_nan(data):
        return None

    # Strip trailing whitespace from SAS CHAR fields
    return data.strip()
