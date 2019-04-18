import pytest

from spinta.config import Config


def test_udpate_config():
    config = Config()
    config.add({
        'components': {
            'nodes': {
                'new': 'component',
            },
        },
        'manifests': {
            'new': {
                'path': 'here',
            },
        },
    })

    assert config.get('components', 'nodes', 'new') == 'component'
    assert config.get('manifests', 'new', 'path') == 'here'
    assert config.get('manifests', cast=list) == ['default', 'new']


def test_update_config_from_cli():
    config = Config()
    config.add_cli_args([
        'backends.default.backend=psycopg2',
        'backends.new.backend=psycopg2',
    ])
    assert config.get('backends', 'default', 'backend') == 'psycopg2'
    assert config.get('backends', 'new', 'backend') == 'psycopg2'
    assert config.get('backends', cast=list) == ['default', 'new']


def test_update_config_from_env():
    config = Config()
    config.set_env({
        'SPINTA_BACKENDS': 'default,new',
        'SPINTA_BACKENDS_DEFAULT_BACKEND': 'psycopg2',
        'SPINTA_BACKENDS_NEW_BACKEND': 'psycopg2',
    })
    assert config.get('backends', 'default', 'backend') == 'psycopg2'
    assert config.get('backends', 'new', 'backend') == 'psycopg2'
    assert config.get('backends', cast=list) == ['default', 'new']


def test_update_config_from_env_file(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        '# comment line\n'
        '\n'
        'SPINTA_BACKENDS=default,new\n'
        'SPINTA_BACKENDS_DEFAULT_BACKEND=foo\n'
        'SPINTA_BACKENDS_NEW_BACKEND=bar\n',
    )

    config = Config()
    config.add_env_file(str(envfile))
    assert config.get('backends', 'default', 'backend') == 'foo'
    assert config.get('backends', 'new', 'backend') == 'bar'
    assert config.get('backends', cast=list) == ['default', 'new']
