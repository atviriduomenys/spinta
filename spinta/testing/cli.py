import click
from click.testing import CliRunner

from spinta.core.config import RawConfig
from spinta.testing.context import create_test_context


class SpintaCliRunner(CliRunner):

    def invoke(self, rc: RawConfig, cli: click.Command, args=None, **kwargs):
        assert isinstance(rc, RawConfig)
        if 'catch_exceptions' not in kwargs:
            kwargs['catch_exceptions'] = False
        if 'obj' not in kwargs:
            kwargs['obj'] = create_test_context(rc, name='pytest/cli')
        return super().invoke(cli, args, **kwargs)
