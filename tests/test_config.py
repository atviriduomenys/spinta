from spinta.core.config import RawConfig, PyDict, Path, EnvVars, EnvFile, CliArgs


def test_hardset():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_MANIFESTS_NEW_PATH': 'envvars',
        }),
        PyDict('app', {
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
        }),
    ])

    assert rc.get('components', 'nodes', 'new') == 'component'
    assert rc.get('manifests', 'new', 'path') == 'here'
    assert rc.keys('manifests') == ['new']


def test_update_config_from_cli():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        CliArgs('cliargs', [
            'backends.default.backend=psycopg2',
            'backends.new.backend=psycopg2',
        ]),
    ])
    assert rc.get('backends', 'default', 'backend') == 'psycopg2'
    assert rc.get('backends', 'new', 'backend') == 'psycopg2'
    assert rc.keys('backends') == ['default', 'new']


def test_update_config_from_env():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_BACKENDS': 'default,new',
            'SPINTA_BACKENDS_DEFAULT_BACKEND': 'psycopg2',
            'SPINTA_BACKENDS_NEW_BACKEND': 'psycopg2',
        }),
    ])
    assert rc.get('backends', 'default', 'backend') == 'psycopg2'
    assert rc.get('backends', 'new', 'backend') == 'psycopg2'
    assert rc.keys('backends') == ['default', 'new']


def test_update_config_from_env_file(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        '# comment line\n'
        '\n'
        'SPINTA_BACKENDS=default,new\n'
        'SPINTA_BACKENDS_DEFAULT_BACKEND=foo\n'
        'SPINTA_BACKENDS_NEW_BACKEND=bar\n',
    )

    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvFile('envfile', str(envfile)),
    ])
    assert rc.get('backends', 'default', 'backend') == 'foo'
    assert rc.get('backends', 'new', 'backend') == 'bar'
    assert rc.keys('backends') == ['default', 'new']


def test_custom_env():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        PyDict('test', {
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
        }),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'foo'
    assert rc.get('backends', 'new', 'dsn') == 'bar'
    assert rc.keys('backends') == ['default', 'new']


def test_custom_env_from_envvar():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {'SPINTA_ENV': 'testing'}),
        PyDict('test', {
            'environments': {
                'testing': {
                    'backends': {
                        'default': {
                            'dsn': 'foo',
                        },
                    }
                }
            }
        }),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_from_envvars_only():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_ENV': 'testing',
            'SPINTA_TESTING_BACKENDS_DEFAULT_DSN': 'foo',
        }),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_priority():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_ENV': 'testing',
            'SPINTA_BACKENDS_DEFAULT_DSN': 'bar',
            'SPINTA_TESTING_BACKENDS_DEFAULT_DSN': 'foo',
        }),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'foo'


def test_custom_env_different_env_name():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_ENV': 'testing',
            'SPINTA_BACKENDS_DEFAULT_DSN': 'bar',
            'SPINTA_PROD_BACKENDS_DEFAULT_DSN': 'foo',
        }),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_BACKENDS_DEFAULT_DSN=foo\n'
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN=bar\n'
    )
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvFile('envfile', str(envfile)),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile_only(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_TESTING_BACKENDS_DEFAULT_DSN=bar\n'
    )
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvFile('envfile', str(envfile)),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'bar'


def test_custom_env_from_envfile_fallback(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        'SPINTA_ENV=testing\n'
        'SPINTA_BACKENDS_DEFAULT_DSN=bar\n'
    )
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvFile('envfile', str(envfile)),
    ])
    assert rc.get('backends', 'default', 'dsn') == 'bar'


_TEST_CONFIG = {
    'backends': {
        'custom': {'dsn': 'config'},
    },
}


def test_custom_config():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        PyDict('test', {
            'config': [f'{__name__}:_TEST_CONFIG'],
        })
    ])
    assert rc.get('backends', 'custom', 'dsn') == 'config'
    assert rc.keys('backends') == ['default', 'custom', 'mongo', 'fs']


def test_yaml_config(tmpdir):
    tmpdir.join('a.yml').write('wait: 1\nbackends: {default: {dsn: test}}')
    tmpdir.join('b.yml').write('wait: 2\n')
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_CONFIG': f'{tmpdir}/a.yml,{tmpdir}/b.yml'
        })
    ])
    assert rc.get('wait', cast=int) == 2
    assert rc.get('backends', 'default', 'dsn') == 'test'
    assert rc.get('manifests', 'default', 'backend') == 'default'


def test_custom_config_fron_environ():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_CONFIG': f'{__name__}:_TEST_CONFIG',
        })
    ])
    assert rc.get('backends', 'custom', 'dsn') == 'config'
    assert rc.keys('backends') == ['default', 'custom', 'mongo', 'fs']


def test_datasets_params():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_DATASETS_SQL': 'param',
        }),
    ])
    assert rc.get('datasets', 'sql') == 'param'


def test_remove_keys():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_BACKENDS': 'one,two',
        }),
        PyDict('test', {
            'backends': {
                'one': {'dsn': '1'},
                'two': {'dsn': '2'},
                'six': {'dsn': '6'},
                'ten': {'dsn': '0'},
            }
        })
    ])
    assert rc.keys('backends') == ['one', 'two']
