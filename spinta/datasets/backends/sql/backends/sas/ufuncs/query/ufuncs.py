"""
SAS-specific Query Functions for SQLAlchemy Query Building.

This module provides SAS-specific query functions that override or extend
the base SQL query functions to handle SAS-specific SQL syntax or behaviors.

Functions provided:
- _group_array: Aggregate values into a comma-separated string (SAS uses CATX)
- asc: Sort ascending with NULLs last
- desc: Sort descending with NULLs first
"""

from typing import Any

import sqlalchemy as sa

from spinta.core.ufuncs import ufunc
from spinta.datasets.backends.sql.backends.sas.helpers import group_array
from spinta.datasets.backends.sql.backends.sas.ufuncs.query.components import SASQueryBuilder
from spinta.datasets.backends.sql.backends.helpers import nulls_last_asc, nulls_first_desc


@ufunc.resolver(SASQueryBuilder, object)
def _group_array(env: SASQueryBuilder, columns: Any):
    """
    Aggregate values into an array using SAS CATX function.

    SAS does not natively support array aggregation like PostgreSQL's array_agg.
    Instead, we use the CATX function which concatenates values with a delimiter.

    Args:
        env: The SAS query builder environment
        columns: Column(s) to aggregate

    Returns:
        SQLAlchemy expression for array aggregation using CATX
    """
    return group_array(columns)


@ufunc.resolver(SASQueryBuilder, sa.sql.expression.ColumnElement)
def asc(env, column):
    """
    Sort ascending with NULLs last.

    SAS default NULL ordering may vary, so we explicitly specify NULLS LAST
    for consistent behavior across all SAS configurations.

    Args:
        env: The query builder environment
        column: The column to sort

    Returns:
        SQLAlchemy expression for ascending order with NULLS LAST
    """
    return nulls_last_asc(column)


@ufunc.resolver(SASQueryBuilder, sa.sql.expression.ColumnElement)
def desc(env, column):
    """
    Sort descending with NULLs first.

    SAS default NULL ordering may vary, so we explicitly specify NULLS FIRST
    for consistent behavior across all SAS configurations.

    Args:
        env: The query builder environment
        column: The column to sort

    Returns:
        SQLAlchemy expression for descending order with NULLS FIRST
    """
    return nulls_first_desc(column)
