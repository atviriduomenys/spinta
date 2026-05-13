"""
SAS Backend Module for Spinta Framework.

This module provides integration with SAS databases through JDBC,
enabling data access from SAS libraries and datasets.

The SAS SQLAlchemy dialect is automatically registered when this module
is imported, making it available for engine creation via:
    create_engine('sas+jdbc://user:pass@host:port/?schema=libname')

Module structure:
    - dialect.py: Core SQLAlchemy dialect implementation
    - types.py: SAS-specific type decorators
    - formats.py: Format constants and type mapping
    - identifier.py: Identifier handling (no quoting)
    - introspection.py: Schema introspection methods
    - base.py: Base JDBC dialect functionality
    - components.py: Spinta backend component
    - helpers.py: Utility functions
"""

from spinta.datasets.backends.sql.backends.sas.dialect import register_sas_dialect

# Register the SAS dialect immediately when this module is imported
# This ensures it's available before any create_engine() calls
register_sas_dialect()


# Public API
__all__ = [
    "register_sas_dialect",
]
