import os
import pathlib

import pytest
from ruamel.yaml import YAML
from typer.testing import CliRunner

from spinta import commands
from spinta.core.config import (
    SCHEMA,
    CliArgs,
    EnvFile,
    EnvVars,
    KeyFormat,
    Path,
    PyDict,
    RawConfig,
    read_config,
)
from spinta.testing.context import create_test_context
from spinta.utils.schema import NA

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
    envvars = {
        "SPINTA_CONFIG": f"{tmp_path}/a.yml,{tmp_path}/b.yml",
        "SPINTA_DEBUG": False,
    }
    tmp_rc = RawConfig()
    tmp_rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            EnvVars("envvars", envvars),
        ]
    )
    configs = tmp_rc.get("config", cast=list, default=[])
    rc = RawConfig()
    rc.read(
        [
            Path("defaults", "spinta.config:CONFIG"),
            *[Path("defaults", c) for c in configs],
            EnvVars("envvars", envvars),
        ]
    )
    assert rc.get("wait", cast=int) == 2
    assert rc.get("backends", "default", "dsn") == "test"
    assert rc.get("debug") is False


def test_nested_yaml_config_load_order(tmp_path: pathlib.Path):
    (tmp_path / "a.yml").write_text("test: 1\nconfig: [a_1.yml, a_2.yml]\n")
    (tmp_path / "a_1.yml").write_text("test: 2\na: 1")
    (tmp_path / "a_2.yml").write_text("test: 3\na: 2\nab: 1\nconfig: [a_2_1.yml]\n")
    (tmp_path / "a_2_1.yml").write_text("test: 4\na: 3\nabc: 1")
    (tmp_path / "b.yml").write_text("config: [b_1.yml]\n")
    (tmp_path / "b_1.yml").write_text("debug: true")
    (tmp_path / "c.yml").write_text("debug: false")

    config_paths = ",".join(str(tmp_path / name) for name in ("a.yml", "b.yml", "c.yml"))
    rc = read_config(args=[f"config={config_paths}"])

    yaml_sources = [pathlib.Path(name) for name in rc.get_source_names() if name.endswith(".yml")]
    assert yaml_sources == [
        tmp_path / "a.yml",
        tmp_path / "a_1.yml",
        tmp_path / "a_2.yml",
        tmp_path / "a_2_1.yml",
        tmp_path / "b.yml",
        tmp_path / "b_1.yml",
        tmp_path / "c.yml",
    ]
    assert rc.get("test", cast=int, origin=True) == (4, str(tmp_path / "a_2_1.yml"))
    assert rc.get("a", cast=int, origin=True) == (3, str(tmp_path / "a_2_1.yml"))
    assert rc.get("ab", cast=int, origin=True) == (1, str(tmp_path / "a_2.yml"))
    assert rc.get("abc", cast=int, origin=True) == (1, str(tmp_path / "a_2_1.yml"))
    assert rc.get("debug", origin=True) == (False, str(tmp_path / "c.yml"))


def test_nested_yaml_config_deduplicates_paths(tmp_path: pathlib.Path):
    root_path = tmp_path / "root.yml"
    defaults_path = tmp_path / "defaults.yml"
    override_path = tmp_path / "override.yml"
    defaults_path.write_text(
        "models.datasets/Country.backend: default\nmodels.datasets/Country.distribute.type: schema\n"
    )
    override_path.write_text("models.datasets/Country.distribute.type: copy\n")
    # Loading a defaults alias after the override would restore the schema distribution.
    root_path.write_text(f"""\
    config:
      - ./defaults.yml
      - override.yml
      - defaults.yml
      - "{defaults_path.as_posix()}"
    """)

    rc = read_config(args=[f"config={root_path}"])

    yaml_sources = [name for name in rc.get_source_names() if name.endswith(".yml")]
    assert [pathlib.Path(name) for name in yaml_sources] == [root_path, defaults_path, override_path]
    assert any(name.endswith("./defaults.yml") for name in yaml_sources)
    assert [(key, value, pathlib.Path(origin)) for key, value, origin in rc.getall("models", origin=True)] == [
        (("models", "datasets/Country", "backend"), "default", defaults_path),
        (("models", "datasets/Country", "distribute", "type"), "copy", override_path),
    ]


def test_nested_yaml_config_resolves_paths_from_each_file(tmp_path: pathlib.Path):
    deploy_dir = tmp_path / "deploy"
    (deploy_dir / "primary").mkdir(parents=True)
    (deploy_dir / "secondary").mkdir()
    shared_dir = tmp_path / "shared"
    shared_dir.mkdir()
    absolute_path = shared_dir / "common.yml"
    absolute_path.write_text("config: [details.yml]\n")
    (shared_dir / "details.yml").write_text("models.datasets/Place.distribute.type: copy\n")
    root_path = deploy_dir / "config.yml"
    root_path.write_text(f"""\
    config:
      - primary/models.yml
      - secondary/models.yml
      - "{absolute_path.as_posix()}"
    """)
    (deploy_dir / "primary" / "models.yml").write_text("config: [details.yml]\n")
    (deploy_dir / "primary" / "details.yml").write_text("models.datasets/Country.backend: default\n")
    (deploy_dir / "secondary" / "models.yml").write_text("models.datasets/City.backend: default\n")

    rc = read_config(args=[f"config={root_path}"])

    yaml_sources = [pathlib.Path(name) for name in rc.get_source_names() if name.endswith(".yml")]
    assert yaml_sources == [
        root_path,
        deploy_dir / "primary" / "models.yml",
        deploy_dir / "primary" / "details.yml",
        deploy_dir / "secondary" / "models.yml",
        absolute_path,
        shared_dir / "details.yml",
    ]
    assert [(key, value, pathlib.Path(origin)) for key, value, origin in rc.getall("models", origin=True)] == [
        (("models", "datasets/Country", "backend"), "default", deploy_dir / "primary" / "details.yml"),
        (("models", "datasets/City", "backend"), "default", deploy_dir / "secondary" / "models.yml"),
        (("models", "datasets/Place", "distribute", "type"), "copy", shared_dir / "details.yml"),
    ]


@pytest.mark.parametrize("cycle_target", ["root.yml", "child.yml"], ids=["parent-cycle", "self-include"])
def test_nested_yaml_config_skips_cycles(tmp_path: pathlib.Path, cycle_target: str):
    root_path = tmp_path / "root.yml"
    child_path = tmp_path / "child.yml"
    root_path.write_text("config: [child.yml]\nmodels.datasets/Country.backend: default\n")
    child_path.write_text(f"config: [{cycle_target}]\nmodels.datasets/Country.distribute.type: copy\n")

    rc = read_config(args=[f"config={root_path}"])

    yaml_sources = [pathlib.Path(name) for name in rc.get_source_names() if name.endswith(".yml")]
    assert yaml_sources == [root_path, child_path]
    assert [(key, value, pathlib.Path(origin)) for key, value, origin in rc.getall("models", origin=True)] == [
        (("models", "datasets/Country", "backend"), "default", root_path),
        (("models", "datasets/Country", "distribute", "type"), "copy", child_path),
    ]


def test_nested_yaml_config_loads_shared_include_at_first_discovery(tmp_path: pathlib.Path):
    root_path = tmp_path / "root.yml"
    primary_path = tmp_path / "primary.yml"
    secondary_path = tmp_path / "secondary.yml"
    shared_path = tmp_path / "shared.yml"
    root_path.write_text("config: [primary.yml, secondary.yml]\n")
    primary_path.write_text("config: [shared.yml]\nmodels.datasets/Country.backend: default\n")
    shared_path.write_text("models.datasets/Country.distribute.type: schema\nmodels.datasets/City.backend: default\n")
    secondary_path.write_text("config: [shared.yml]\nmodels.datasets/Country.distribute.type: copy\n")

    rc = read_config(args=[f"config={root_path}"])

    yaml_sources = [pathlib.Path(name) for name in rc.get_source_names() if name.endswith(".yml")]
    assert yaml_sources == [root_path, primary_path, shared_path, secondary_path]
    assert [(key, value, pathlib.Path(origin)) for key, value, origin in rc.getall("models", origin=True)] == [
        (("models", "datasets/Country", "backend"), "default", primary_path),
        (("models", "datasets/Country", "distribute", "type"), "copy", secondary_path),
        (("models", "datasets/City", "backend"), "default", shared_path),
    ]


def test_nested_yaml_config_raises_for_missing_include(tmp_path: pathlib.Path):
    config_dir = tmp_path / "deploy"
    config_dir.mkdir()
    root_path = config_dir / "root.yml"
    root_path.write_text("config: [missing.yml]\n")

    with pytest.raises(FileNotFoundError) as error:
        read_config(args=[f"config={root_path}"])

    assert pathlib.Path(error.value.filename) == config_dir / "missing.yml"


def test_nested_config_skips_builtin_config_reference(tmp_path: pathlib.Path):
    root_path = tmp_path / "root.yml"
    root_path.write_text("config: [spinta.config:CONFIG, spinta.config:CONFIG]\ndebug: true\n")

    rc = read_config(args=[f"config={root_path}"])

    assert rc.get_source_names() == ["spinta", str(root_path), "envfile", "envvars", "cliargs"]
    assert rc.get("debug", origin=True) == (True, str(root_path))


def test_nested_config_validates_builtin_source_label_as_include(tmp_path: pathlib.Path):
    root_path = tmp_path / "root.yml"
    root_path.write_text("config: [envvars]\n")

    with pytest.raises(Exception, match="Can't import python path: 'envvars'"):
        read_config(args=[f"config={root_path}"])


@pytest.mark.parametrize(
    ("env_config_key", "cli_override", "selected_source"),
    [
        pytest.param(None, False, "dotenv", id="dotenv"),
        pytest.param("SPINTA_CONFIG", False, "environment", id="environment-over-dotenv"),
        pytest.param("SPINTA_TESTING__CONFIG", False, "environment", id="active-environment-overlay"),
        pytest.param("SPINTA_CONFIG", True, "cli", id="cli-over-environment-and-dotenv"),
    ],
)
def test_nested_yaml_config_selects_highest_priority_source(
    tmp_path: pathlib.Path, env_config_key: str | None, cli_override: bool, selected_source: str
):
    for source in ("dotenv", "environment", "cli"):
        source_dir = tmp_path / source
        source_dir.mkdir()
        model = f"models.datasets/{source}/Country"
        (source_dir / "config.yml").write_text(f"config: [models.yml]\n{model}.backend: default\n")
        (source_dir / "models.yml").write_text(f"{model}.distribute.type: copy\n")

    envfile = tmp_path / ".env"
    envfile.write_text(f"SPINTA_CONFIG={tmp_path / 'dotenv' / 'config.yml'}\n")
    env: dict[str, str | None] = dict.fromkeys(key for key in os.environ if key.upper().startswith("SPINTA_"))
    if env_config_key is not None:
        env[env_config_key] = str(tmp_path / "environment" / "config.yml")
    if env_config_key == "SPINTA_TESTING__CONFIG":
        env["SPINTA_ENV"] = "testing"
    args = [f"config={tmp_path / 'cli' / 'config.yml'}"] if cli_override else []

    with CliRunner().isolation(env=env):
        rc = read_config(args=args, envfile=envfile)

    selected_dir = tmp_path / selected_source
    assert rc.get_source_names() == [
        "spinta",
        str(selected_dir / "config.yml"),
        str(selected_dir / "models.yml"),
        "envfile",
        "envvars",
        "cliargs",
    ]
    model = f"datasets/{selected_source}/Country"
    assert list(rc.getall("models", origin=True)) == [
        (("models", model, "backend"), "default", str(selected_dir / "config.yml")),
        (("models", model, "distribute", "type"), "copy", str(selected_dir / "models.yml")),
    ]


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
    # `after` was removed, sources argument order decides priority:
    # later sources override earlier ones.
    rc = RawConfig()
    rc.read(
        [
            PyDict("C1", {"a": 1}),
            PyDict("C2", {"a": 2}),
        ]
    )
    rc.read([PyDict("C3", {"a": 3})])
    assert rc.get("a", origin=True) == (3, "C3")


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
                    "env": "other",
                    "environments": {
                        "dev": {
                            "backends.sql.type": "sql",
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

    assert list(rc.getall("backends")) == [
        (("backends", "default", "type"), "postgresql"),
        (("backends", "default", "dsn"), "localhost"),
    ]

    rc.add("T1", {"env": "test"})
    assert list(rc.getall("backends")) == [
        (("backends", "default", "type"), "postgresql"),
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
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "---------------------", "-----"),
        ("defaults", "", "backends.default.type", "sql"),
    ]


def test_dump_env():
    rc = RawConfig()
    rc.add("defaults", {"backends.default.type": "sql"})
    assert rc.dump(fmt=KeyFormat.env, file=None) == [
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "------------------------------", "-----"),
        ("defaults", "", "SPINTA_BACKENDS__DEFAULT__TYPE", "sql"),
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
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "---------------------", "----------"),
        ("defaults", "", "backends.default.type", "postgresql"),
        ("defaults", "", "backends.sql.type", "sql"),
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
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "---------------------", "----------"),
        ("defaults", "", "backends.default.type", "postgresql"),
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
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "----------------------", "----------"),
        ("defaults", "", "backends.default.type", "postgresql"),
        ("defaults", "", "manifests.default.type", "yaml"),
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
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "----------------------", "-------------"),
        ("defaults", "", "backends.default.type", "postgresql"),
        ("defaults", "", "backends.default.dsn", "postgresql://"),
        ("defaults", "", "manifests.default.type", "yaml"),
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
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "---------------------", "-------------"),
        ("defaults", "", "backends.default.type", "postgresql"),
        ("defaults", "", "backends.default.dsn", "postgresql://"),
    ]


def test_dump_env_column():
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
                            "backends.sql.type": "sql",
                        },
                    },
                },
            ),
        ]
    )
    assert rc.dump("backends", file=None) == [
        ("Origin", "Env", "Name", "Value"),
        ("--------", "---", "---------------------", "----------"),
        ("defaults", "", "backends.default.type", "postgresql"),
        ("defaults", "dev", "backends.sql.type", "sql"),
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


def test_pydict_read_does_not_modify_input():
    config = {
        "environments": {
            "test": {
                "backends.default.dsn": "sqlite:///test.db",
            },
        },
    }
    rc = RawConfig()
    rc.read([PyDict("test", config)])
    assert config == {
        "environments": {
            "test": {
                "backends.default.dsn": "sqlite:///test.db",
            },
        },
    }
    rc.add("t", {"env": "test"})
    assert rc.get("backends", "default", "dsn") == "sqlite:///test.db"


def test_path_read_does_not_modify_imported_config():
    # `spinta.config:CONFIG` is a global dict, shared by all `RawConfig`
    # instances in the process, it must not be modified by reading it.
    # Note: `environments.*` subtree is merged into the main tree (without
    # the `environments.*` prefix), so it is not available via `get()`.
    from spinta.config import CONFIG

    assert "environments" in CONFIG
    rc1 = RawConfig()
    rc1.read([Path("defaults", "spinta.config:CONFIG")])
    assert "environments" in CONFIG
    manifest_type = rc1.get("manifests", "default", "type")

    rc2 = RawConfig()
    rc2.read([Path("defaults", "spinta.config:CONFIG")])
    assert "environments" in CONFIG
    assert rc2.get("manifests", "default", "type") == manifest_type


def test_fork_partial_subtree_keeps_lower_priority_keys():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "keymaps.default": {
                        "type": "sqlalchemy",
                        "dsn": "sqlite:///keymap.db",
                    },
                },
            ),
        ]
    )
    rc = rc.fork({"keymaps.default": {"sync_transaction_size": 4}})
    assert rc.keys("keymaps", "default") == ["type", "dsn", "sync_transaction_size"]
    assert rc.get("keymaps", "default", "dsn") == "sqlite:///keymap.db"
    assert rc.get("keymaps", "default", "sync_transaction_size") == 4


def test_fork_adds_new_keys_to_lower_priority_ones():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "one": {"dsn": "1"},
                        "two": {"dsn": "2"},
                    },
                },
            ),
        ]
    )
    rc = rc.fork({"backends.three": {"dsn": "3"}})
    assert rc.keys("backends") == ["one", "two", "three"]
    assert rc.get("backends", "one", "dsn") == "1"
    assert rc.get("backends", "three", "dsn") == "3"


def test_fork_explicit_value_still_resets_lower_priority_keys():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "one": {"dsn": "1"},
                        "two": {"dsn": "2"},
                    },
                },
            ),
        ]
    )
    rc = rc.fork({"backends": ["one"]})
    assert rc.keys("backends") == ["one"]
    assert rc.get("backends", "one", "dsn") == "1"
    assert rc.get("backends", "two", "dsn") is NA


def test_fork_scalar_value_for_keys_raises_error():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "defaults",
                {
                    "backends": {
                        "one": {"dsn": "1"},
                        "two": {"dsn": "2"},
                    },
                },
            ),
        ]
    )
    with pytest.raises(Exception, match=r"expected a mapping or a list of key names"):
        rc.fork({"backends": "one"})


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


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({"server_url": "https://old.example.com/"}, "https://old.example.com"),
        (
            {"server_url": "https://old.example.com", "resource_server_url": "https://new.example.com/"},
            "https://new.example.com",
        ),
    ],
)
def test_resource_server_url_falls_back_to_server_url(rc, overrides, expected):
    context = create_test_context(rc.fork(overrides))
    config = context.get("config")
    commands.load(context, config)
    assert config.resource_server_url == expected
    assert config.server_url == expected


@pytest.mark.parametrize(
    "overrides, expected",
    [
        (
            {"token_issuer": "https://old.example.com/", "resource_server": "https://old-rs.example.com"},
            ("https://old.example.com/", "https://old.example.com", "https://old-rs.example.com"),
        ),
        (
            {
                "token_issuer": "https://old.example.com",
                "resource_server": "https://old-rs.example.com",
                "auth_server_id": "https://id.example.com",
                "auth_server_url": "https://url.example.com",
                "resource_server_id": "https://rs.example.com",
            },
            ("https://id.example.com", "https://url.example.com", "https://rs.example.com"),
        ),
    ],
)
def test_auth_settings_fall_back_to_old_names(rc, overrides, expected):
    rc = rc.fork({"auth_server_id": None, "auth_server_url": None, "resource_server_id": None, **overrides})
    context = create_test_context(rc)
    config = context.get("config")
    commands.load(context, config)
    assert (config.auth_server_id, config.auth_server_url, config.resource_server_id) == expected


def test_nested_extension(tmp_path: pathlib.Path):
    models_path = tmp_path / "models.yml"
    citus_path = tmp_path / "citus.yml"
    models_path.write_text("""
    config: [citus.yml]
    models:
        datasets/Example:
            backend: default
            properties:
                id:
                    type: {name: sqlalchemy.BigInteger}
        datasets/Another:
            backend: sql
    """)
    citus_path.write_text("""
    models.datasets/Example.distribute:
        type: copy
    models.datasets/Other:
            distribute:
                type: schema
    """)

    rc = read_config(args=[f"config={models_path}"])

    assert list(rc.getall("models", origin=True)) == [
        (("models", "datasets/Example", "backend"), "default", str(models_path)),
        (("models", "datasets/Example", "properties", "id", "type", "name"), "sqlalchemy.BigInteger", str(models_path)),
        (("models", "datasets/Example", "distribute", "type"), "copy", str(citus_path)),
        (("models", "datasets/Another", "backend"), "sql", str(models_path)),
        (("models", "datasets/Other", "distribute", "type"), "schema", str(citus_path)),
    ]
