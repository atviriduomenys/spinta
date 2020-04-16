import sys

from pygments import highlight
from pygments.lexers import get_lexer_by_name
from pygments.formatters import Terminal256Formatter


import pytest
import click
from click.testing import CliRunner

from spinta.core.config import RawConfig
from spinta.testing.context import create_test_context


class SpintaCliRunner(CliRunner):

    def invoke(self, rc: RawConfig, cli: click.Command, args=None, **kwargs):
        assert isinstance(rc, RawConfig)
        if 'obj' not in kwargs:
            kwargs['obj'] = create_test_context(rc, name='pytest/cli')
        result = super().invoke(cli, args, **kwargs)
        if result.exc_info is not None:
            t, e, tb = result.exc_info
            if not isinstance(e, SystemExit):
                print(result.output)
                from _pytest._code import ExceptionInfo
                exc = ExceptionInfo.from_exc_info((t, e, tb))
                exc = str(exc.getrepr(style='short'))
                lexer = get_lexer_by_name("pytb")
                formatter = Terminal256Formatter()
                print(highlight(exc, lexer, formatter), file=sys.stderr)
                # traceback.print_exception(t, e, tb)
        if result.exit_code != 0:
            print(result.output)
            cmd = ' '.join([cli.name] + (args or []))
            pytest.fail(f"Command `{cmd}` failed, exit code {result.exit_code}.")
        return result
