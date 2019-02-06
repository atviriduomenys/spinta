from unittest.mock import MagicMock

from spinta.backends.postgresql import Prepare


def test_get_table_name():
    backend = MagicMock()
    backend.get.return_value = 42
    cmd = Prepare(None, None, backend)
    assert cmd.get_table_name('org') == 'ORG_0042'
    assert len(cmd.get_table_name('a' * 100)) == 63
    assert cmd.get_table_name('a' * 100)[-10:] == 'AAAAA_0042'
    assert cmd.get_table_name('some_/name/hėrę!') == 'SOME_NAME_HERE_0042'
