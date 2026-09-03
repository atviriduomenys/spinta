import pytest

from spinta.core.config import CliArgs, ConfigSource, EnvVars, PyDict, RawConfig
from spinta.utils.schema import NA

SOURCE_TYPES: list[str] = ["pydict", "envvars", "cliargs"]


def _make_config(source_type: str, name: str, config: dict) -> ConfigSource:
    """Create a configuration source of the given type.

    Takes an already flattened config, for example:

        {
            "backends.one.type": "sql",
            "backends.one.dsn": "sql@example.com",
        }

    each `source_type` produces a source with the following configuration:

        pydict:
            PyDict("base", {
                "backends": {
                    "one": {"type": "sql", "dsn": "sql@example.com"},
                },
            })

        envvars:
            EnvVars("base", {
                "SPINTA_BACKENDS__ONE__TYPE": "sql",
                "SPINTA_BACKENDS__ONE__DSN": "sql@example.com",
            })

        cliargs:
            CliArgs("base", [
                "backends.one.type=sql",
                "backends.one.dsn=sql@example.com",
            ])

    A reset value, like an empty list, is preserved as is, for example,
    `{"backends": [], "backends.one.dsn": "..."}` produces `SPINTA_BACKENDS=`
    followed by `SPINTA_BACKENDS__ONE__DSN=...`.

    A list of child key names, like `["one"]`, is preserved as a list for
    `pydict`, but is serialized as a comma separated string for `envvars` and
    `cliargs`, so `{"backends": ["one"]}` produces `SPINTA_BACKENDS=one` and
    `backends=one`.
    """
    if source_type == "pydict":
        return PyDict(name, _unflatten(config))
    elif source_type == "envvars":
        return EnvVars(
            name,
            {
                "SPINTA_" + key.replace(".", "__").upper(): _config_value_to_str(value)
                for key, value in config.items()
            },
        )
    elif source_type == "cliargs":
        return CliArgs(
            name,
            [key + "=" + _config_value_to_str(value) for key, value in config.items()],
        )
    else:
        raise ValueError(f"Unknown source type: {source_type!r}")


def _unflatten(config: dict) -> dict:
    """Convert a flat config with dotted keys into a nested config.

    For example:

        {
            "backends.one.type": "sql",
            "backends.one.dsn": "sql@example.com",
        }

    becomes:

        {
            "backends": {
                "one": {"type": "sql", "dsn": "sql@example.com"},
            },
        }

    This is needed only for `PyDict`, because a nested structure declares the
    list of child key names, which replaces child key names declared by lower
    priority sources. Dotted keys alone would declare only leaf values, and
    then `PyDict` would behave like `EnvVars` or `CliArgs`.

    A reset value, like an empty list, is replaced with nested keys if more
    specific keys are given under the same prefix, for example:

        {
            "backends": [],
            "backends.one.type": "sql",
        }

    becomes:

        {
            "backends": {
                "one": {"type": "sql"},
            },
        }

    For `PyDict` this is equivalent, because a nested structure resets child
    key names declared by lower priority sources in the same way as a reset
    value does.
    """
    result = {}
    for key, value in config.items():
        parts = key.split(".")
        node = result
        for part in parts[:-1]:
            if not isinstance(node.get(part), dict):
                node[part] = {}
            node = node[part]
        node[parts[-1]] = value
    return result


def _config_value_to_str(value: object) -> str:
    if isinstance(value, list):
        # A list of child key names, e.g. on an object-type key like
        # `backends`, can only be given as a comma separated string by
        # env vars and CLI args.
        return ",".join(value)
    else:
        return str(value)


@pytest.mark.parametrize("override_type", SOURCE_TYPES)
@pytest.mark.parametrize("base_type", SOURCE_TYPES)
def test_override_backend_values(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends.one.type": "sql",
                    "backends.one.dsn": "sql@example.com",
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends.one.type": "sqlite",
                    "backends.one.dsn": "sqlite@example.com",
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["one"]
    assert rc.keys("backends", "one") == ["type", "dsn"]
    assert rc.get("backends", "one", "type") == "sqlite"
    assert rc.get("backends", "one", "dsn") == "sqlite@example.com"
    assert rc.get("backends", "one", "type", origin=True) == ("sqlite", "override")


@pytest.mark.parametrize("override_type", SOURCE_TYPES)
@pytest.mark.parametrize("base_type", SOURCE_TYPES)
def test_add_backend(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends.one.type": "sql",
                    "backends.one.dsn": "sql@example.com",
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends.two.type": "pg",
                    "backends.two.dsn": "pg@example.com",
                },
            ),
        ]
    )
    if override_type == "pydict":
        assert rc.keys("backends") == ["two"]
        assert rc.get("backends", "one", "type") is NA
    else:
        assert rc.keys("backends") == ["one", "two"]
        assert rc.get("backends", "one", "type") == "sql"
    assert rc.get("backends", "two", "type") == "pg"
    assert rc.get("backends", "two", "dsn") == "pg@example.com"


@pytest.mark.parametrize("override_type", SOURCE_TYPES)
@pytest.mark.parametrize("base_type", SOURCE_TYPES)
def test_reset_backends(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends.one.type": "sql",
                    "backends.one.dsn": "sql@example.com",
                    "backends.two.type": "pg",
                    "backends.two.dsn": "pg@example.com",
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": [],
                    "backends.one.type": "sqlite",
                    "backends.one.dsn": "sqlite@example.com",
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["one"]
    assert rc.get("backends", "one", "type") == "sqlite"
    assert rc.get("backends", "one", "dsn") == "sqlite@example.com"
    assert rc.get("backends", "two", "type") is NA


@pytest.mark.parametrize("override_type", SOURCE_TYPES)
@pytest.mark.parametrize("base_type", SOURCE_TYPES)
def test_remove_all_backends(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends.one.type": "sql",
                    "backends.one.dsn": "sql@example.com",
                    "backends.two.type": "pg",
                    "backends.two.dsn": "pg@example.com",
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": [],
                },
            ),
        ]
    )
    assert rc.keys("backends") == []
    assert rc.get("backends") == []
    assert rc.get("backends", "one", "type") is NA
    assert rc.get("backends", "one", "dsn") is NA
    assert rc.get("backends", "two", "type") is NA
    assert rc.get("backends", "two", "dsn") is NA


@pytest.mark.parametrize("override_type", SOURCE_TYPES)
@pytest.mark.parametrize("base_type", SOURCE_TYPES)
def test_remove_backend(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends.one.type": "sql",
                    "backends.one.dsn": "sql@example.com",
                    "backends.two.type": "pg",
                    "backends.two.dsn": "pg@example.com",
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": ["one"],
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["one"]
    assert rc.get("backends", "one", "type") == "sql"
    assert rc.get("backends", "two", "type") is NA


def test_pydict_dotted_keys():
    rc = RawConfig()
    rc.read(
        [
            PyDict(
                "base",
                {
                    "backends": {
                        "one": {"type": "sql", "dsn": "sql@example.com"},
                    },
                },
            ),
            PyDict(
                "override",
                {
                    "backends.two.type": "pg",
                    "backends.two.dsn": "pg@example.com",
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["one", "two"]
    assert rc.get("backends", "two", "type") == "pg"
    assert rc.get("backends", "two", "dsn") == "pg@example.com"


def test_pydict_scalar_value_for_keys_raises_error():
    rc = RawConfig()
    with pytest.raises(Exception, match=r"expected a mapping or a list of key names"):
        rc.read([PyDict("base", {"backends": "one"})])
