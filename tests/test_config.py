from spinta.config import Config


def test_udpate_config():
    config = Config()
    config.read(env_vars=False, env_files=False, config={
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
    assert config.keys('manifests') == ['default', 'new']


def test_update_config_from_cli():
    config = Config()
    config.read(env_vars=False, env_files=False, cli_args=[
        'backends.default.backend=psycopg2',
        'backends.new.backend=psycopg2',
    ])
    assert config.get('backends', 'default', 'backend') == 'psycopg2'
    assert config.get('backends', 'new', 'backend') == 'psycopg2'
    assert config.keys('backends') == ['default', 'new']


def test_update_config_from_env():
    config = Config()
    config.read(env_files=False, env_vars={
        'SPINTA_BACKENDS': 'default,new',
        'SPINTA_BACKENDS_DEFAULT_BACKEND': 'psycopg2',
        'SPINTA_BACKENDS_NEW_BACKEND': 'psycopg2',
    })
    assert config.get('backends', 'default', 'backend') == 'psycopg2'
    assert config.get('backends', 'new', 'backend') == 'psycopg2'
    assert config.keys('backends') == ['default', 'new']


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
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'backend') == 'foo'
    assert config.get('backends', 'new', 'backend') == 'bar'
    assert config.keys('backends') == ['default', 'new']


def test_custom_env():
    config = Config()
    config.read(env_vars=False, env_files=False, config={
        'env': 'testing',
        'environments': {
            'testing': {
                'backends': {
                    'default': {
                        'dsn': 'foo',
                    },
                    'new': {
                        'dsn': 'bar',
                    },
                }
            }
        }
    })
    assert config.get('backends', 'default', 'dsn') == 'foo'
    assert config.get('backends', 'new', 'dsn') == 'bar'
    assert config.keys('backends') == ['default', 'new']


def test_custom_env_from_envvar():
    config = Config()
    config.read(env_files=False, env_vars={'SPINTA_ENV': 'testing'}, config={
        'environments': {
            'testing': {
                'backends': {
                    'default': {
                        'dsn': 'foo',
                    },
                }
            }
        }
    })
    assert config.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_from_envvars_only():
    config = Config()
    config.read(env_files=False, env_vars={
        'SPINTA_ENV': 'testing',
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN': 'foo',
    })
    assert config.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_priority():
    config = Config()
    config.read(env_files=False, env_vars={
        'SPINTA_ENV': 'testing',
        'SPINTA_BACKENDS_DEFAULT_DSN': 'bar',
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN': 'foo',
    })
    assert config.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_different_env_name():
    config = Config()
    config.read(env_files=False, env_vars={
        'SPINTA_ENV': 'testing',
        'SPINTA_BACKENDS_DEFAULT_DSN': 'bar',
        'SPINTA_PROD_BACKENDS_DEFAULT_DSN': 'foo',
    })
    assert config.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_BACKENDS_DEFAULT_DSN=foo\n'
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN=bar\n'
    )
    config = Config()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile_only(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN=bar\n'
    )
    config = Config()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile_fallback(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_BACKENDS_DEFAULT_DSN=bar\n'
    )
    config = Config()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'dsn') == 'bar'


_TEST_CONFIG = {
    'backends': {
        'custom': {'dsn': 'config'},
    },
}


def test_custom_config():
    config = Config()
    config.read(env_vars=False, env_files=False, config={
        'config': [f'{__name__}:_TEST_CONFIG'],
    })
    assert config.get('backends', 'custom', 'dsn') == 'config'
    assert config.keys('backends') == ['default', 'custom', 'mongo']


def test_custom_config_fron_environ():
    config = Config()
    config.read(env_files=False, env_vars={
        'SPINTA_CONFIG': f'{__name__}:_TEST_CONFIG',
    })
    assert config.get('backends', 'custom', 'dsn') == 'config'
    assert config.keys('backends') == ['default', 'custom', 'mongo']


def test_remove_keys():
    config = Config()
    config.read(
        env_files=False,
        env_vars={
            'SPINTA_BACKENDS': 'one,two',
        },
        config={
            'backends': {
                'one': {'dsn': '1'},
                'two': {'dsn': '2'},
                'six': {'dsn': '6'},
                'ten': {'dsn': '0'},
            }
        },
    )
    assert config.keys('backends') == ['one', 'two']
