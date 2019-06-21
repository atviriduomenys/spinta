from spinta.config import RawConfig


def test_hardset():
    config = RawConfig()
    config.read(
        env_files=False,
        env_vars={
            'SPINTA_MANIFESTS_NEW_PATH': 'envvars',
        },
        hardset={
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
        },
    )

    assert config.get('components', 'nodes', 'new') == 'component'
    assert config.get('manifests', 'new', 'path') == 'here'
    assert config.keys('manifests') == ['new']


def test_update_config_from_cli():
    config = RawConfig()
    config.read(env_vars=False, env_files=False, cli_args=[
        'backends.default.backend=psycopg2',
        'backends.new.backend=psycopg2',
    ])
    assert config.get('backends', 'default', 'backend') == 'psycopg2'
    assert config.get('backends', 'new', 'backend') == 'psycopg2'
    assert config.keys('backends') == ['default', 'new']


def test_update_config_from_env():
    config = RawConfig()
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

    config = RawConfig()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'backend') == 'foo'
    assert config.get('backends', 'new', 'backend') == 'bar'
    assert config.keys('backends') == ['default', 'new']


def test_custom_env():
    config = RawConfig()
    config.read(env_vars=False, env_files=False, hardset={
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
    config = RawConfig()
    config.read(env_files=False, env_vars={'SPINTA_ENV': 'testing'}, hardset={
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
    config = RawConfig()
    config.read(env_files=False, env_vars={
        'SPINTA_ENV': 'testing',
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN': 'foo',
    })
    assert config.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_priority():
    config = RawConfig()
    config.read(env_files=False, env_vars={
        'SPINTA_ENV': 'testing',
        'SPINTA_BACKENDS_DEFAULT_DSN': 'bar',
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN': 'foo',
    })
    assert config.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_different_env_name():
    config = RawConfig()
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
    config = RawConfig()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile_only(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN=bar\n'
    )
    config = RawConfig()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile_fallback(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_BACKENDS_DEFAULT_DSN=bar\n'
    )
    config = RawConfig()
    config.read(env_vars=False, env_files=[str(envfile)])
    assert config.get('backends', 'default', 'dsn') == 'bar'


_TEST_CONFIG = {
    'backends': {
        'custom': {'dsn': 'config'},
    },
}


def test_custom_config():
    config = RawConfig()
    config.read(env_vars=False, env_files=False, hardset={
        'config': [f'{__name__}:_TEST_CONFIG'],
    })
    assert config.get('backends', 'custom', 'dsn') == 'config'
    assert config.keys('backends') == ['default', 'custom', 'mongo', 'fs']


def test_yaml_config(tmpdir):
    tmpdir.join('a.yml').write('wait: 1\nbackends: {default: {dsn: test}}')
    tmpdir.join('b.yml').write('wait: 2\n')
    config = RawConfig()
    config.read(env_files=False, env_vars={
        'SPINTA_CONFIG': f'{tmpdir}/a.yml,{tmpdir}/b.yml'
    })
    assert config.get('wait', cast=int) == 2
    assert config.get('backends', 'default', 'dsn') == 'test'
    assert config.get('manifests', 'default', 'backend') == 'default'


def test_custom_config_fron_environ():
    config = RawConfig()
    config.read(env_files=False, env_vars={
        'SPINTA_CONFIG': f'{__name__}:_TEST_CONFIG',
    })
    assert config.get('backends', 'custom', 'dsn') == 'config'
    assert config.keys('backends') == ['default', 'custom', 'mongo', 'fs']


def test_datasets_params():
    config = RawConfig()
    config.read(env_files=False, env_vars={
        'SPINTA_DATASETS_SQL': 'param',
    })
    assert config.get('datasets', 'sql') == 'param'


def test_remove_keys():
    config = RawConfig()
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
