import os
import sys
import traceback
from typing import Any, List

import pytest
from click.testing import Result
from typer.testing import CliRunner

from spinta.cli import main
from spinta.core.config import RawConfig
from spinta.testing.context import close_context_engines, create_test_context


def _prepare_args(args: List[Any]) -> List[str]:
    """Prepare args

    This does following conversions:

        [['a']] -> ['a']
        [1]     -> ['1']
        [None]  -> ['']

    """
    if not args:
        return args

    result = []
    for arg in args:
        if not isinstance(arg, list):
            arg = [arg]
        result += ["" if a is None else str(a) for a in arg]
    return result


def result_contains(
    result: Result,
    message: str,
    *,
    error_stream: bool = True,
) -> bool:
    if error_stream:
        return message in result.stderr

    return message in result.stdout


class SpintaCliRunner(CliRunner):
    def invoke(
        self,
        rc: RawConfig,
        args: List[Any] = None,
        fail: bool = True,
        **kwargs,
    ):
        assert isinstance(rc, RawConfig)
        own_context = None
        if "obj" not in kwargs:
            own_context = create_test_context(rc, name="pytest/cli")
            kwargs["obj"] = own_context

        args = _prepare_args(args)
        try:
            result = super().invoke(main.app, args, **kwargs)
        finally:
            # Dispose engines created for the context we own here. Otherwise, on a
            # command failure the traceback kept by pytest pins the context (and
            # its open connection pool) until the session ends, which on CPython
            # 3.14 accumulates and exhausts the PostgreSQL connection limit.
            if own_context is not None:
                close_context_engines(own_context)
        if result.exc_info is not None:
            t, e, tb = result.exc_info
            if not isinstance(e, SystemExit):
                exc = "".join(traceback.format_exception(t, e, tb))
                exc = exc.replace(os.getcwd() + "/", "")
                print(exc, file=sys.stderr)
        if result.exit_code != 0:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
            cmd = " ".join(["spinta"] + (args or []))
            if fail:
                pytest.fail(f"Command `{cmd}` failed, exit code {result.exit_code}.")
        else:
            if result.stderr_bytes is not None and result.stderr:
                print(result.stderr, file=sys.stderr)
        return result
