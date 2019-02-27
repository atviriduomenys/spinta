import pytest

from spinta.config import CONFIG
from spinta.config import update_config_from_cli
from spinta.config import update_config_from_env
from spinta.config import update_config_from_env_file


def test_update_config_from_cli():
    assert update_config_from_cli(CONFIG, ['backends.default.type=psycopg2'])['backends']['default']['type'] == 'psycopg2'
    assert update_config_from_cli(CONFIG, ['backends.new.type=psycopg2'])['backends'] == {
        'default': {'type': 'postgresql', 'dsn': 'postgresql:///spinta'},
        'new': {'type': 'psycopg2', 'dsn': 'postgresql:///spinta'},
    }


def test_update_config_from_cli_unknown_option_error():
    with pytest.raises(Exception) as e:
        update_config_from_cli(CONFIG, ['unknown=1'])
    assert str(e.value) == "Unknown configuration option 'unknown'."

    with pytest.raises(Exception) as e:
        update_config_from_cli(CONFIG, ['nested.param.unknown=1'])
    assert str(e.value) == "Unknown configuration option 'nested.param.unknown'."


def test_update_config_from_env():
    assert update_config_from_env(CONFIG, {
        'SPINTA_BACKENDS_DEFAULT_TYPE': 'psycopg2',
    })['backends']['default']['type'] == 'psycopg2'

    assert update_config_from_env(CONFIG, {
        'SPINTA_BACKENDS': 'default,new',
        'SPINTA_BACKENDS_NEW_TYPE': 'psycopg2',
    })['backends'] == {
        'default': {'type': 'postgresql', 'dsn': 'postgresql:///spinta'},
        'new': {'type': 'psycopg2', 'dsn': 'postgresql:///spinta'},
    }


def test_update_config_from_env_file(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        '# comment line\n'
        '\n'
        'SPINTA_BACKENDS_DEFAULT_TYPE=foo\n'
        'SPINTA_BACKENDS=new\n'
        'SPINTA_BACKENDS_NEW_TYPE=bar\n',
    )
    assert update_config_from_env_file(CONFIG, str(envfile))['backends'] == {
        'default': {'dsn': 'postgresql:///spinta', 'type': 'foo'},
        'new': {'dsn': 'postgresql:///spinta', 'type': 'bar'},
    }
