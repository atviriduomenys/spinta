import pathlib

from ruamel.yaml import YAML

from spinta.core.config import SCHEMA, CliArgs, EnvFile, EnvVars, KeyFormat, Path, PyDict, RawConfig, read_config

yaml = YAML(typ="safe")


def test_envvars():
    config = EnvVars(
        "envvars",
        {
            "SPINTA_MANIFESTS__DEFAULT__TYPE": "sql",
        },
    )
    config.read(SCHEMA)
    assert config.config == {
        ("manifests", "default", "type"): "sql",
    }


def test_envvars_switch_case():
    config = EnvVars(
        "envvars",
        {
            "SPINTA_MANIFESTS__YAML__TYPE": "yaml",
            "SPINTA_MANIFESTS__YAML__PATH": "manifest",
        },
    )
    config.read(SCHEMA)
    assert config.config == {
        ("manifests", "yaml", "type"): "yaml",
        ("manifests", "yaml", "path"): "manifest",
    }


def test_envvars_multipart():
    config = EnvVars(
        "envvars",
        {
            "SPINTA_DEFAULT_AUTH_CLIENT": "guest",
            "SPINTA_DEFAULT_ACCESS_LEVEL": "public",
        },
    )
    config.read(SCHEMA)
    assert config.config == {
        ("default_auth_client",): "guest",
        ("default_access_level",): "public",
    }


def test_hardset():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "manifests": {
                        "default": {
                            "type": "internal",
                            "backend": "default",
                            "sync": "yaml",
                        },
                        "yaml": {
                            "type": "yaml",
                            "backend": "default",
                            "path": "manifest",
                        },
                    },
                },
            ),
            EnvVars(
                "envvars",
                {
                    "SPINTA_MANIFESTS__NEW__PATH": "envvars",
                },
            ),
            PyDict(
                "app",
                {
                    "components.nodes.new": "component",
                    "manifests.new.path": "here",
                },
            ),
        ]
    )
    assert rc.keys("manifests") == ["default", "yaml", "new"]
    assert rc.get("components", "nodes", "new") == "component"
    assert rc.get("manifests", "new", "path") == "here"
    assert list(rc.getall()) == [
        (("manifests", "default", "type"), "internal"),
        (("manifests", "default", "backend"), "default"),
        (("manifests", "default", "sync"), "yaml"),
        (("manifests", "yaml", "type"), "yaml"),
        (("manifests", "yaml", "backend"), "default"),
        (("manifests", "yaml", "path"), "manifest"),
        (("manifests", "new", "path"), "here"),
        (("components", "nodes", "new"), "component"),
    ]


def test_update_config_from_cli():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "default": {
                            "backend": "sql",
                        }
                    }
                },
            ),
            CliArgs(
                "cliargs",
                [
                    "backends.default.backend=postgresql",
                    "backends.new.backend=postgresql",
                ],
            ),
        ]
    )
    assert rc.keys("backends") == ["default", "new"]
    assert rc.get("backends", "default", "backend") == "postgresql"
    assert rc.get("backends", "new", "backend") == "postgresql"
    assert list(rc.getall()) == [
        (("backends", "default", "backend"), "postgresql"),
        (("backends", "new", "backend"), "postgresql"),
    ]


def test_update_config_from_env():
    rc = RawConfig()
    rc.read(
        [
            EnvVars(
                "envvars",
                {
                    "SPINTA_BACKENDS__DEFAULT__TYPE": "postgresql",
                    "SPINTA_BACKENDS__NEW__TYPE": "sql",
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["default", "new"]
    assert rc.get("backends", "default", "type") == "postgresql"
    assert rc.get("backends", "new", "type") == "sql"
    assert list(rc.getall()) == [
        (("backends", "default", "type"), "postgresql"),
        (("backends", "new", "type"), "sql"),
    ]


def test_update_config_from_env_file(tmp_path: pathlib.Path):
    envfile = tmp_path / ".env"
    envfile.write_text(
        "# comment line\n\nSPINTA_BACKENDS__DEFAULT__TYPE=foo\nSPINTA_BACKENDS__NEW__TYPE=bar\n",
    )

    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "default": {"type": "test"},
                    },
                },
            ),
            EnvFile("envfile", str(envfile)),
        ]
    )
    assert rc.keys("backends") == ["default", "new"]
    assert rc.get("backends", "default", "type") == "foo"
    assert rc.get("backends", "new", "type") == "bar"


def test_custom_env():
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            PyDict(
                "test",
                {
                    "env": "testing",
                    "environments": {
                        "testing": {
                            "backends": {
                                "default": {
                                    "dsn": "foo",
                                },
                                "new": {
                                    "dsn": "bar",
                                },
                            }
                        }
                    },
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["default", "new"]
    assert rc.get("backends", "default", "dsn") == "foo"
    assert rc.get("backends", "new", "dsn") == "bar"
    assert list(rc.getall("backends")) == [
        (("backends", "default", "dsn"), "foo"),
        (("backends", "new", "dsn"), "bar"),
    ]


def test_custom_env_from_envvar():
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars("envvars", {"SPINTA_ENV": "testing"}),
            PyDict(
                "test",
                {
                    "environments": {
                        "testing": {
                            "backends": {
                                "default": {
                                    "dsn": "foo",
                                },
                            }
                        }
                    }
                },
            ),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "foo"


def test_custom_env_from_envvars_only():
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars(
                "envvars",
                {
                    "SPINTA_ENV": "testing",
                    "SPINTA_TESTING__BACKENDS__DEFAULT__DSN": "foo",
                },
            ),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "foo"


def test_custom_env_priority():
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars(
                "envvars",
                {
                    "SPINTA_ENV": "testing",
                    "SPINTA_BACKENDS__DEFAULT__DSN": "bar",
                    "SPINTA_TESTING__BACKENDS__DEFAULT__DSN": "foo",
                },
            ),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "foo"


def test_custom_env_different_env_name():
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars(
                "envvars",
                {
                    "SPINTA_ENV": "testing",
                    "SPINTA_BACKENDS__DEFAULT__DSN": "bar",
                    "SPINTA_PROD__BACKENDS__DEFAULT__DSN": "foo",
                },
            ),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "bar"


def test_custom_env_from_envfile(tmp_path: pathlib.Path):
    envfile = tmp_path / ".env"
    envfile.write_text(
        "SPINTA_ENV=testing\nSPINTA_BACKENDS__DEFAULT__DSN=foo\nSPINTA_TESTING__BACKENDS__DEFAULT__DSN=bar\n"
    )
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvFile("envfile", str(envfile)),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "bar"


def test_custom_env_from_envfile_only(tmp_path: pathlib.Path):
    envfile = tmp_path / ".env"
    envfile.write_text("SPINTA_ENV=testing\nSPINTA_TESTING__BACKENDS__DEFAULT__DSN=bar\n")
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvFile("envfile", str(envfile)),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "bar"


def test_custom_env_from_envfile_fallback(tmp_path: pathlib.Path):
    envfile = tmp_path / ".env"
    envfile.write_text("SPINTA_ENV=testing\nSPINTA_BACKENDS__DEFAULT__DSN=bar\n")
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvFile("envfile", str(envfile)),
        ]
    )
    assert rc.get("backends", "default", "dsn") == "bar"


_TEST_CONFIG = {
    "backends": {
        "custom": {"dsn": "config"},
    },
}


def test_custom_config():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "app",
                {
                    "config": [f"{__name__}:_TEST_CONFIG"],
                },
            ),
        ]
    )
    configs = rc.get("config", cast=list, default=[])
    rc.read([Path("defaults", c) for c in configs])
    assert rc.get("backends", "custom", "dsn") == "config"


def test_yaml_config(tmp_path: pathlib.Path):
    (tmp_path / "a.yml").write_text("wait: 1\nbackends: {default: {dsn: test}}")
    (tmp_path / "b.yml").write_text("wait: 2\ndebug: true")
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars(
                "envvars",
                {
                    "SPINTA_CONFIG": f"{tmp_path}/a.yml,{tmp_path}/b.yml",
                    "SPINTA_DEBUG": False,
                },
            ),
        ]
    )
    assert rc.get("wait", cast=int) == 2
    assert rc.get("backends", "default", "dsn") == "test"
    assert rc.get("debug") is False


def test_nested_yaml_config_load_order(tmp_path: pathlib.Path):
    (tmp_path / "a.yml").write_text(f"test: 1\nconfig: [{str(tmp_path / 'a_1.yml')},{str(tmp_path / 'a_2.yml')}]")
    (tmp_path / "a_1.yml").write_text("test: 2\na: 1")
    (tmp_path / "a_2.yml").write_text(f"test: 3\na: 2\nab: 1\nconfig: [{str(tmp_path / 'a_2_1.yml')}]")
    (tmp_path / "a_2_1.yml").write_text("test: 4\na: 3\nab: 2\nabc: 1")
    (tmp_path / "b.yml").write_text(f"config: [{str(tmp_path / 'b_1.yml')}]")
    (tmp_path / "b_1.yml").write_text("debug: true")
    (tmp_path / "c.yml").write_text("debug: false")
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars(
                "envvars",
                {
                    "SPINTA_CONFIG": f"{str(tmp_path / 'a.yml')},{str(tmp_path / 'b.yml')},{str(tmp_path / 'c.yml')}",
                },
            ),
        ]
    )
    assert rc.get("test", cast=int, origin=True) == (1, str(tmp_path / "a.yml"))
    assert rc.get("a", cast=int, origin=True) == (2, str(tmp_path / "a_2.yml"))
    assert rc.get("ab", cast=int, origin=True) == (1, str(tmp_path / "a_2.yml"))
    assert rc.get("abc", cast=int, origin=True) == (1, str(tmp_path / "a_2_1.yml"))
    assert rc.get("debug", origin=True) == (False, str(tmp_path / "c.yml"))


def test_custom_config_fron_environ():
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars(
                "envvars",
                {
                    "SPINTA_CONFIG": f"{__name__}:_TEST_CONFIG",
                },
            ),
        ]
    )
    configs = rc.get("config", cast=list, default=[])
    rc = RawConfig()
    rc.read([Path("defaults", c) for c in configs])
    assert rc.get("backends", "custom", "dsn") == "config"


def test_remove_keys():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "test",
                {
                    "backends": {
                        "one": {"dsn": "1"},
                        "two": {"dsn": "2"},
                        "six": {"dsn": "6"},
                        "ten": {"dsn": "0"},
                    }
                },
            ),
            EnvVars(
                "envvars",
                {
                    "SPINTA_BACKENDS": "one,two",
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["one", "two"]
    assert list(rc.getall()) == [
        (("backends", "one", "dsn"), "1"),
        (("backends", "two", "dsn"), "2"),
    ]


def test_after():
    rc = RawConfig()
    rc.read(
        [
            PyDict("C1", {"a": 1}),
            PyDict("C2", {"a": 2}),
        ]
    )
    rc.read([PyDict("C3", {"a": 3})], after="C1")
    assert rc.get("a", origin=True) == (2, "C2")


def test_before():
    rc = RawConfig()
    rc.read(
        [
            PyDict("C1", {"a": 1, "b": 1}),
            PyDict("C2", {"a": 2}),
        ]
    )
    rc.read([PyDict("C2.1", {"a": 2.5, "b": 2})], before="C2")
    assert rc.get("a", origin=True) == (2, "C2")
    assert rc.get("b", origin=True) == (2, "C2.1")


def test_fork():
    rc1 = RawConfig()
    rc1.read([PyDict("C1", {"a": 1})])
    rc2 = rc1.fork([PyDict("C2", {"a": 2})])
    assert rc1.get("a") == 1
    assert rc2.get("a") == 2


def test_environments():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "default": {"type": "postgresql", "dsn": "localhost"},
                    },
                    "env": "dev",
                    "environments": {
                        "dev": {
                            "backends": {
                                "sql": {
                                    "type": "sql",
                                },
                            },
                        },
                        "test": {
                            "backends": {
                                "default": {
                                    "type": "postgresql",
                                },
                                "sql": {
                                    "type": "sql",
                                },
                                "fs": {
                                    "type": "fs",
                                },
                            },
                        },
                    },
                },
            ),
        ]
    )

    rc.add("T1", {"env": "test"})
    assert list(rc.getall("backends")) == [
        (("backends", "default", "type"), "postgresql"),
        (("backends", "default", "dsn"), "localhost"),
        (("backends", "sql", "type"), "sql"),
        (("backends", "fs", "type"), "fs"),
    ]

    rc.add("T2", {"env": "dev"})
    assert list(rc.getall("backends")) == [
        (("backends", "default", "type"), "postgresql"),
        (("backends", "default", "dsn"), "localhost"),
        (("backends", "sql", "type"), "sql"),
    ]


def test_environments_dotted_name():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "default": {
                            "type": "postgresql",
                        },
                    },
                    "env": "dev",
                    "environments": {
                        "dev": {
                            "backends.sql": {
                                "type": "sql",
                            },
                            "backends.fs": {
                                "type": "fs",
                            },
                        },
                        "test": {
                            "backends.default.type": "sql",
                        },
                    },
                },
            ),
        ]
    )

    rc.add("T1", {"env": "test"})
    assert list(rc.getall("backends")) == [
        (("backends", "default", "type"), "sql"),
    ]

    rc.add("T2", {"env": "dev"})
    assert list(rc.getall("backends")) == [
        (("backends", "default", "type"), "postgresql"),
        (("backends", "sql", "type"), "sql"),
        (("backends", "fs", "type"), "fs"),
    ]


def test_dump():
    rc = RawConfig()
    rc.add("defaults", {"backends.default.type": "sql"})
    assert rc.dump(file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "---------------------", "-----"),
        ("defaults", "backends.default.type", "sql"),
    ]


def test_dump_env():
    rc = RawConfig()
    rc.add("defaults", {"backends.default.type": "sql"})
    assert rc.dump(fmt=KeyFormat.env, file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "------------------------------", "-----"),
        ("defaults", "SPINTA_BACKENDS__DEFAULT__TYPE", "sql"),
    ]


def test_dump_filter():
    rc = RawConfig()
    rc.add(
        "defaults",
        {
            "backends.default.type": "postgresql",
            "backends.sql.type": "sql",
            "manifests.default.type": "yaml",
        },
    )
    assert rc.dump("backends", file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "---------------------", "----------"),
        ("defaults", "backends.default.type", "postgresql"),
        ("defaults", "backends.sql.type", "sql"),
    ]


def test_dump_filter_dots():
    rc = RawConfig()
    rc.add(
        "defaults",
        {
            "backends.default.type": "postgresql",
            "backends.default.dsn": "postgresql://",
            "manifests.default.type": "yaml",
        },
    )
    assert rc.dump("backends..type", file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "---------------------", "----------"),
        ("defaults", "backends.default.type", "postgresql"),
    ]


def test_dump_filter_dots_2():
    rc = RawConfig()
    rc.add(
        "defaults",
        {
            "backends.default.type": "postgresql",
            "backends.default.dsn": "postgresql://",
            "manifests.default.type": "yaml",
        },
    )
    assert rc.dump("..type", file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "----------------------", "----------"),
        ("defaults", "backends.default.type", "postgresql"),
        ("defaults", "manifests.default.type", "yaml"),
    ]


def test_dump_two_filter():
    rc = RawConfig()
    rc.add(
        "defaults",
        {
            "backends.default.type": "postgresql",
            "backends.default.dsn": "postgresql://",
            "manifests.default.type": "yaml",
        },
    )
    assert rc.dump("backends", "manifests", file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "----------------------", "-------------"),
        ("defaults", "backends.default.type", "postgresql"),
        ("defaults", "backends.default.dsn", "postgresql://"),
        ("defaults", "manifests.default.type", "yaml"),
    ]


def test_dump_filter_startswith():
    rc = RawConfig()
    rc.add(
        "defaults",
        {
            "backends.default.type": "postgresql",
            "backends.default.dsn": "postgresql://",
            "manifests.default.type": "yaml",
        },
    )
    assert rc.dump("ba", file=None) == [
        ("Origin", "Name", "Value"),
        ("--------", "---------------------", "-------------"),
        ("defaults", "backends.default.type", "postgresql"),
        ("defaults", "backends.default.dsn", "postgresql://"),
    ]


def test_keys_from_switch_case():
    rc = RawConfig()
    rc.add(
        "defaults",
        {
            "accesslog": {
                "type": "file",
                "file": "stdout",
                "buffer_size": 300,
            }
        },
    )
    assert rc.keys("accesslog") == ["type", "file", "buffer_size"]


def test_schema_default_value():
    rc = RawConfig()
    rc.add("defaults", {"accesslog.type": "file"})
    assert rc.get("accesslog", "buffer_size") == 300


def test_to_dict():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "manifests": {
                        "default": {
                            "type": "internal",
                            "backend": "default",
                        },
                        "yaml": {
                            "type": "yaml",
                            "backend": "default",
                            "path": "manifest",
                        },
                    },
                },
            ),
        ]
    )
    assert rc.to_dict("manifests", "default") == {
        "type": "internal",
        "backend": "default",
    }


def test_object_static_to_dynamic_keys():
    rc = RawConfig()
    config = EnvVars(
        "envvars",
        {
            "SPINTA_BACKENDS__CUSTOM__TYPE": "test",
            "SPINTA_BACKENDS__CUSTOM__DSN": "test",
        },
    )
    pydict_config = PyDict(
        "test",
        {
            "backends": {
                "custom": {"dsn": "custom@test", "optional": True},
                "test": {"type": "test", "dsn": "test@test"},
            },
        },
    )

    assert config.static_keys is True
    assert pydict_config.static_keys is False
    rc.read([config, pydict_config])
    assert list(rc.getall("backends", origin=True)) == [
        (("backends", "custom", "type"), "test", "envvars"),
        (("backends", "custom", "dsn"), "custom@test", "test"),
    ]


def test_object_dynamic_to_dynamic_keys():
    rc = RawConfig()
    config1 = PyDict(
        "custom1",
        {
            "backends": {
                "custom": {"type": "custom1", "dsn": "custom1@test"},
            },
        },
    )
    config2 = PyDict(
        "custom2",
        {
            "backends": {
                "custom": {"dsn": "custom2@test", "optional": True},
                "test": {"type": "test", "dsn": "test@test"},
            },
        },
    )

    assert config1.static_keys is False
    assert config2.static_keys is False
    rc.read([config1, config2])
    assert list(rc.getall("backends", origin=True)) == [
        (("backends", "custom", "type"), "custom1", "custom1"),
        (("backends", "custom", "dsn"), "custom2@test", "custom2"),
        (("backends", "custom", "optional"), True, "custom2"),
        (("backends", "test", "type"), "test", "custom2"),
        (("backends", "test", "dsn"), "test@test", "custom2"),
    ]


def test_object_dynamic_to_static():
    rc = RawConfig()
    config = EnvVars(
        "envvars",
        {
            "SPINTA_BACKENDS": "custom",
            "SPINTA_BACKENDS__CUSTOM__TYPE": "test",
        },
    )
    pydict_config = PyDict(
        "test",
        {
            "backends": {
                "custom": {"type": "custom", "dsn": "custom@test", "optional": True},
                "test": {"type": "test", "dsn": "test@test"},
            },
        },
    )

    assert config.static_keys is True
    assert pydict_config.static_keys is False
    rc.read([pydict_config, config])
    assert list(rc.getall("backends", origin=True)) == [
        (("backends", "custom", "type"), "test", "envvars"),
        (("backends", "custom", "dsn"), "custom@test", "test"),
        (("backends", "custom", "optional"), True, "test"),
    ]


def test_object_static_to_static():
    rc = RawConfig()
    config1 = EnvVars(
        "envvars1",
        {
            "SPINTA_BACKENDS__CUSTOM__TYPE": "test",
            "SPINTA_BACKENDS__CUSTOM__DSN": "test@test",
        },
    )
    config2 = EnvVars(
        "envvars2",
        {
            "SPINTA_BACKENDS__CUSTOM": "dsn,other",
            "SPINTA_BACKENDS__CUSTOM__DSN": "custom@test",
            "SPINTA_BACKENDS__CUSTOM__OTHER": "other",
        },
    )

    assert config1.static_keys is True
    assert config2.static_keys is True
    rc.read([config1, config2])
    assert list(rc.getall("backends", origin=True)) == [
        (("backends", "custom", "dsn"), "custom@test", "envvars2"),
        (("backends", "custom", "other"), "other", "envvars2"),
    ]


def test_nested_extension(tmp_path: pathlib.Path):
    rc = RawConfig()
    models_path = tmp_path / "models.yml"
    str_models_path = str(tmp_path / "models.yml")
    citus_path = tmp_path / "citus.yml"
    str_citus_path = str(tmp_path / "citus.yml")
    config = PyDict(
        "base",
        {"config": [str_models_path, str_citus_path]},
    )
    models_path.write_text("""
    models:
        datasets/Example:
            backend: default
            properties:
                id:
                    type: sqlalchemy.BigInteger
        datasets/Another:
            backend: sql
    """)
    citus_path.write_text("""
    models:
        datasets/Example:
            distribute:
                type: copy
        datasets/Other:
            distribute:
                type: schema
    """)

    rc.read([config])
    assert list(rc.getall("models", origin=True)) == [
        (("models", "datasets/Example", "backend"), "default", str_models_path),
        (("models", "datasets/Example", "properties", "id", "type"), "sqlalchemy.BigInteger", str_models_path),
        (("models", "datasets/Example", "distribute", "type"), "copy", str_citus_path),
        (("models", "datasets/Another", "backend"), "sql", str_models_path),
        (("models", "datasets/Other", "distribute", "type"), "schema", str_citus_path),
    ]


def test_default_anchor(tmp_path: pathlib.Path):
    env_file_1 = tmp_path / "env_file_1.yml"
    env_file_1_1 = tmp_path / "env_file_1_1.yml"
    env_file_1_2 = tmp_path / "env_file_1_2.yml"

    env_file_1.write_text(f"""
    config:
        - {str(env_file_1_1)}
        - {str(env_file_1_2)}
    
    test_order: env_file_1
    """)
    env_file_1_1.write_text("test_order: env_file_1_1")
    env_file_1_2.write_text("test_order: env_file_1_2")

    dot_env = tmp_path / ".env"
    dot_env.write_text(f"""
    SPINTA_CONFIG={env_file_1}
    """)

    arg_1 = tmp_path / "arg_1.yml"
    arg_1_1 = tmp_path / "arg_1_1.yml"
    arg_1_2 = tmp_path / "arg_1_2.yml"
    arg_2 = tmp_path / "arg_2.yml"

    arg_1.write_text(f"""
    config:
        - {str(arg_1_1)}
        - {str(arg_1_2)}

    test_order: arg_1
    """)
    arg_1_1.write_text("test_order: arg_1_1")
    arg_1_2.write_text("test_order: arg_1_2")
    arg_2.write_text("test_order: arg_2")

    rc = read_config(args=[f"config={str(arg_1)},{str(arg_2)}"], envfile=str(dot_env))
    assert rc.anchor == "envfile"
    assert rc.get_source_names() == [
        "spinta",
        str(env_file_1_1),
        str(env_file_1_2),
        str(env_file_1),
        str(arg_1_1),
        str(arg_1_2),
        str(arg_1),
        str(arg_2),
        "envfile",
        "envvars",
        "cliargs",
    ]
    assert list(rc.getall("test_order", origin=True)) == [(("test_order",), "arg_2", str(arg_2))]
