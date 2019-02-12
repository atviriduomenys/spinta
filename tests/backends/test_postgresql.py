from unittest.mock import Mock, MagicMock

from spinta.backends.postgresql import Prepare


def test_get_table_name():
    backend = MagicMock()
    backend.get.return_value = 42
    cmd = Prepare(None, None, None, None, backend=backend)
    model = Mock()

    model.name = 'org'
    assert cmd.get_table_name(model) == 'ORG_0042'

    model.name = 'a' * 100
    assert len(cmd.get_table_name(model)) == 63

    model.name = 'a' * 100
    assert cmd.get_table_name(model)[-10:] == 'AAAAA_0042'

    model.name = 'some_/name/hėrę!'
    assert cmd.get_table_name(model) == 'SOME_NAME_HERE_0042'
