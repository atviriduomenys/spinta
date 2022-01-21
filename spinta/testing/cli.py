from typing import Any
from typing import List

import os
import sys
import traceback

import pytest
from typer.testing import CliRunner

from spinta.cli import main
from spinta.core.config import RawConfig
from spinta.testing.context import create_test_context


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
        result += ['' if a is None else str(a) for a in arg]
    return result


class SpintaCliRunner(CliRunner):

    def invoke(
        self,
        rc: RawConfig,
        args: List[Any] = None,
        **kwargs,
    ):
        assert isinstance(rc, RawConfig)
        if 'obj' not in kwargs:
            context = create_test_context(rc, name='pytest/cli')
            kwargs['obj'] = context

        args = _prepare_args(args)
        result = super().invoke(main.app, args, **kwargs)
        if result.exc_info is not None:
            t, e, tb = result.exc_info
            if not isinstance(e, SystemExit):
                exc = ''.join(traceback.format_exception(t, e, tb))
                exc = exc.replace(os.getcwd() + '/', '')
                print(exc, file=sys.stderr)
        if result.exit_code != 0:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
            cmd = ' '.join(['spinta'] + (args or []))
            pytest.fail(f"Command `{cmd}` failed, exit code {result.exit_code}.")
        else:
            if result.stderr_bytes is not None and result.stderr:
                print(result.stderr, file=sys.stderr)
        return result
