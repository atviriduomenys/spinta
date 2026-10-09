"""Spinta warning categories.

This module must stay lightweight (no heavy imports), so that importing it
can never cause import cycles or side effects from any code that needs to
emit one of these warnings.

Deprecation notices are issued with ``warnings.warn()`` (not ``log.warning``)
so that Python's default warning filters apply:

- they are deduplicated: with the ``default`` filter, each location is
  shown at most once, instead of firing on every request or row; (note
  that, being ``DeprecationWarning`` subclasses, they are hidden by
  default, see the last paragraph below);
- they can be controlled without touching the code, e.g.::

    # Silence all deprecation warnings:
    PYTHONWARNINGS="ignore::DeprecationWarning" spinta run

    # Turn all deprecation warnings into errors (useful in CI):
    PYTHONWARNINGS="error::DeprecationWarning" pytest

    # Or Spinta-specific filtering in pytest (options are applied by pytest
    # itself, in-process, where spinta is importable):
    pytest -W "error::spinta.warnings.SpintaDeprecationWarning"

    # Or in pytest.ini / pyproject.toml:
    #
    # [tool.pytest.ini_options]
    # filterwarnings = ["error::spinta.warnings.SpintaDeprecationWarning"]

NOTE: dotted categories of third-party packages (for example
``spinta.warnings.SpintaDeprecationWarning``) can't be used directly in
``PYTHONWARNINGS`` or the ``-W`` interpreter option (on current CPython
versions): Python applies warning filters during interpreter startup,
before ``site-packages`` is added to ``sys.path``, so the category fails
to import and is ignored with an
"Invalid -W option ignored: invalid module name" warning. Use the stdlib
``DeprecationWarning`` category as shown above, or pytest's ``-W`` option or
``filterwarnings`` setting, which are applied later, when all packages are
importable.

By default ``DeprecationWarning`` (and its subclasses) is shown only in
``__main__`` or when ``PYTHONDEVMODE=1`` is set, use ``-W
default::DeprecationWarning`` to make it visible elsewhere.
"""


class SpintaDeprecationWarning(DeprecationWarning):
    """Base class for all Spinta deprecation warnings."""


class ScopeFormatDeprecationWarning(SpintaDeprecationWarning):
    """The 'spinta_*' scope prefix is deprecated.

    Old OAuth scope format was prefixed with `spinta_*` prefix.

    New, standardized scope format is prefixed with `uapi:*` and is documented here:

    https://ivpk.github.io/uapi/#section/Authorization/Scope
    """
