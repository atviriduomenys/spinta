import pathlib

from ruamel.yaml import YAML

from spinta.core.config import RawConfig, PyDict, Path, EnvVars, EnvFile, CliArgs

yaml = YAML(typ='safe')

SCHEMA = {
    'type': 'object',
    'items': yaml.load(pathlib.Path('spinta/config.yml').read_text()),
}


def test_envvars():
    config = EnvVars('envvars', {
        'SPINTA_MANIFESTS_DEFAULT_TYPE': 'mongo',
    })
    config.read(SCHEMA)
    assert config.config == {
        ('manifests', 'default', 'type'): 'mongo',
    }


def test_envvars_switch_case():
    config = EnvVars('envvars', {
        'SPINTA_MANIFESTS_YAML_TYPE': 'yaml',
        'SPINTA_MANIFESTS_YAML_PATH': 'manifest',
    })
    config.read(SCHEMA)
    assert config.config == {
        ('manifests', 'yaml', 'type'): 'yaml',
        ('manifests', 'yaml', 'path'): 'manifest',
    }


def test_envvars_multipart():
    config = EnvVars('envvars', {
        'SPINTA_DEFAULT_AUTH_CLIENT': 'guest',
    })
    config.read(SCHEMA)
    assert config.config == {
        ('default_auth_client',): 'guest',
    }


def test_hardset():
    rc = RawConfig()
    rc.read([
        PyDict('defaults', {
            'manifests': {
                'default': {
                    'type': 'spinta',
                    'backend': 'default',
                    'sync': 'yaml',
                },
                'yaml': {
                    'type': 'yaml',
                    'backend': 'default',
                    'path': 'manifest',
                },
            },
        }),
        EnvVars('envvars', {
            'SPINTA_MANIFESTS_NEW_PATH': 'envvars',
        }),
        PyDict('app', {
            'components.nodes.new': 'component',
            'manifests.new.path': 'here',
        }),
    ])
    assert rc.keys('manifests') == ['default', 'yaml', 'new']
    assert rc.get('components', 'nodes', 'new') == 'component'
    assert rc.get('manifests', 'new', 'path') == 'here'
    assert list(rc.getall()) == [
        (('manifests', 'default', 'type'), 'spinta'),
        (('manifests', 'default', 'backend'), 'default'),
        (('manifests', 'default', 'sync'), 'yaml'),
        (('manifests', 'yaml', 'type'), 'yaml'),
        (('manifests', 'yaml', 'backend'), 'default'),
        (('manifests', 'yaml', 'path'), 'manifest'),
        (('manifests', 'new', 'path'), 'here'),
        (('components', 'nodes', 'new'), 'component'),
    ]


def test_update_config_from_cli():
    rc = RawConfig()
    rc.read([
        PyDict('defaults', {
            'backends': {
                'default': {
                    'backend': 'mongo',
                }
            }
        }),
        CliArgs('cliargs', [
            'backends.default.backend=postgresql',
            'backends.new.backend=postgresql',
        ]),
    ])
    assert rc.keys('backends') == ['default', 'new']
    assert rc.get('backends', 'default', 'backend') == 'postgresql'
    assert rc.get('backends', 'new', 'backend') == 'postgresql'
    assert list(rc.getall()) == [
        (('backends', 'default', 'backend'), 'postgresql'),
        (('backends', 'new', 'backend'), 'postgresql'),
    ]


def test_update_config_from_env():
    rc = RawConfig()
    rc.read([
        EnvVars('envvars', {
            'SPINTA_BACKENDS_DEFAULT_TYPE': 'postgresql',
            'SPINTA_BACKENDS_NEW_TYPE': 'mongo',
        }),
    ])
    assert rc.keys('backends') == ['default', 'new']
    assert rc.get('backends', 'default', 'type') == 'postgresql'
    assert rc.get('backends', 'new', 'type') == 'mongo'
    assert list(rc.getall()) == [
        (('backends', 'default', 'type'), 'postgresql'),
        (('backends', 'new', 'type'), 'mongo'),
    ]


def test_update_config_from_env_file(tmpdir):
    envfile = tmpdir.join('.env')
    envfile.write(
        '# comment line\n'
        '\n'
        'SPINTA_BACKENDS_DEFAULT_TYPE=foo\n'
        'SPINTA_BACKENDS_NEW_TYPE=bar\n',
    )

    rc = RawConfig()
    rc.read([
        PyDict('defaults', {
            'backends': {
                'default': {'type': 'test'},
            },
        }),
        EnvFile('envfile', str(envfile)),
    ])
    assert rc.keys('backends') == ['default', 'new']
    assert rc.get('backends', 'default', 'type') == 'foo'
    assert rc.get('backends', 'new', 'type') == 'bar'


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
    assert rc.keys('backends') == ['default', 'new']
    assert rc.get('backends', 'default', 'dsn') == 'foo'
    assert rc.get('backends', 'new', 'dsn') == 'bar'
    assert list(rc.getall('backends')) == [
        (('backends', 'default', 'dsn'), 'foo'),
        (('backends', 'new', 'dsn'), 'bar'),
    ]


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
        PyDict('app', {
            'config': [f'{__name__}:_TEST_CONFIG'],
        }),
    ])
    configs = rc.get('config', cast=list, default=[])
    rc.read([Path('defaults', c) for c in configs])
    assert rc.get('backends', 'custom', 'dsn') == 'config'


def test_yaml_config(tmpdir):
    tmpdir.join('a.yml').write('wait: 1\nbackends: {default: {dsn: test}}')
    tmpdir.join('b.yml').write('wait: 2\ndebug: true')
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_CONFIG': f'{tmpdir}/a.yml,{tmpdir}/b.yml',
            'SPINTA_DEBUG': False,
        })
    ])
    configs = rc.get('config', cast=list, default=[])
    sources = [Path('defaults', c) for c in configs]
    rc.read(sources, after='defaults')
    assert rc.get('wait', cast=int) == 2
    assert rc.get('backends', 'default', 'dsn') == 'test'
    assert rc.get('debug') is False


def test_custom_config_fron_environ():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_CONFIG': f'{__name__}:_TEST_CONFIG',
        })
    ])
    configs = rc.get('config', cast=list, default=[])
    rc = RawConfig()
    rc.read([Path('defaults', c) for c in configs])
    assert rc.get('backends', 'custom', 'dsn') == 'config'


def test_datasets_params():
    rc = RawConfig()
    rc.read([
        Path('defaults', 'spinta.config:CONFIG'),
        EnvVars('envvars', {
            'SPINTA_DATASETS_MANIFEST_DATASET_RESOURCE': 'dsn',
            'SPINTA_DATASETS__DEFAULT__GOV_ORG_DATA__SQL': 'dsn',
        }),
    ])
    assert rc.get('datasets', 'manifest', 'dataset', 'resource') == 'dsn'
    assert rc.get('datasets', 'default', 'gov_org_data', 'sql') == 'dsn'
    assert rc.keys('datasets') == ['manifest', 'default']
    assert rc.keys('datasets', 'default') == ['gov_org_data']


def test_remove_keys():
    rc = RawConfig()
    rc.read([
        PyDict('test', {
            'backends': {
                'one': {'dsn': '1'},
                'two': {'dsn': '2'},
                'six': {'dsn': '6'},
                'ten': {'dsn': '0'},
            }
        }),
        EnvVars('envvars', {
            'SPINTA_BACKENDS': 'one,two',
        }),
    ])
    assert rc.keys('backends') == ['one', 'two']
    assert list(rc.getall()) == [
        (('backends', 'one', 'dsn'), '1'),
        (('backends', 'two', 'dsn'), '2'),
    ]


def test_after():
    rc = RawConfig()
    rc.read([
        PyDict('C1', {'a': 1}),
        PyDict('C2', {'a': 2}),
    ])
    rc.read([PyDict('C3', {'a': 3})], after='C1')
    assert rc.get('a', origin=True) == (2, 'C2')


def test_fork():
    rc1 = RawConfig()
    rc1.read([PyDict('C1', {'a': 1})])
    rc2 = rc1.fork([PyDict('C2', {'a': 2})])
    assert rc1.get('a') == 1
    assert rc2.get('a') == 2


def test_environments():
    rc = RawConfig()
    rc.read([
        PyDict('defaults', {
            'backends': {
                'default': {
                    'type': 'postgresql',
                },
            },
            'env': 'dev',
            'environments': {
                'dev': {
                    'backends': {
                        'mongo': {
                            'type': 'mongo',
                        },
                        'fs': {
                            'type': 'fs',
                        },
                    },
                },
                'test': {
                    'backends': {
                        'default': {
                            'type': 'postgresql',
                        },
                        'mongo': {
                            'type': 'mongo',
                        },
                        'fs': {
                            'type': 'fs',
                        },
                    },
                }
            },
        }),
    ])

    rc.add('T1', {'env': 'test'})
    assert list(rc.getall('backends')) == [
        (('backends', 'default', 'type'), 'postgresql'),
        (('backends', 'mongo', 'type'), 'mongo'),
        (('backends', 'fs', 'type'), 'fs'),
    ]

    rc.add('T2', {'env': 'dev'})
    assert list(rc.getall('backends')) == [
        (('backends', 'mongo', 'type'), 'mongo'),
        (('backends', 'fs', 'type'), 'fs'),
    ]
