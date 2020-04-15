import os
import sys
import traceback

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
                exc = ''.join(traceback.format_exception(t, e, tb))
                exc = exc.replace(os.getcwd() + '/', '')
                print(exc, file=sys.stderr)
        if result.exit_code != 0:
            print(result.output)
            cmd = ' '.join([cli.name] + (args or []))
            pytest.fail(f"Command `{cmd}` failed, exit code {result.exit_code}.")
        return result
