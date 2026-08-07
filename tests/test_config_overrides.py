import pytest

from spinta.core.config import CliArgs, ConfigSource, EnvVars, PyDict, RawConfig
from spinta.utils.nestedstruct import flatten
from spinta.utils.schema import NA

SOURCE_TYPES: list[str] = ["pydict", "envvars", "cliargs"]


def _make_config(source_type: str, name: str, config: dict) -> ConfigSource:
    """Create a configuration source of the given type.

    For example, given a config like this:

        {
            "backends": {
                "one": {"type": "sql", "dsn": "sql@example.com"},
            },
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
    """
    if source_type == "pydict":
        return PyDict(name, config)
    elif source_type == "envvars":
        return EnvVars(
            name,
            {
                "SPINTA_" + "__".join(key.upper() for key in parts.split(".")): str(value)
                for parts, value in list(flatten(config))[0].items()
            },
        )
    elif source_type == "cliargs":
        return CliArgs(
            name,
            [key + "=" + str(value) for key, value in list(flatten(config))[0].items()],
        )
    else:
        raise ValueError(f"Unknown source type: {source_type!r}")


@pytest.mark.parametrize("base_type", SOURCE_TYPES)
@pytest.mark.parametrize("override_type", SOURCE_TYPES)
def test_override_backend_values(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends": {
                        "one": {"type": "sql", "dsn": "sql@example.com"},
                    },
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": {
                        "one": {"type": "sqlite", "dsn": "sqlite@example.com"},
                    },
                },
            ),
        ]
    )
    assert rc.keys("backends") == ["one"]
    assert rc.keys("backends", "one") == ["type", "dsn"]
    assert rc.get("backends", "one", "type") == "sqlite"
    assert rc.get("backends", "one", "dsn") == "sqlite@example.com"
    assert rc.get("backends", "one", "type", origin=True) == ("sqlite", "override")


@pytest.mark.parametrize("base_type", SOURCE_TYPES)
@pytest.mark.parametrize("override_type", SOURCE_TYPES)
def test_add_backend(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends": {
                        "one": {"type": "sql", "dsn": "sql@example.com"},
                    },
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": {
                        "two": {"type": "pg", "dsn": "pg@example.com"},
                    },
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


@pytest.mark.parametrize("base_type", SOURCE_TYPES)
@pytest.mark.parametrize("override_type", SOURCE_TYPES)
def test_reset_backends(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends": {
                        "one": {"type": "sql", "dsn": "sql@example.com"},
                        "two": {"type": "pg", "dsn": "pg@example.com"},
                    },
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": "",
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


@pytest.mark.parametrize("base_type", SOURCE_TYPES)
@pytest.mark.parametrize("override_type", SOURCE_TYPES)
def test_remove_all_backends(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends": {
                        "one": {"type": "sql", "dsn": "sql@example.com"},
                        "two": {"type": "pg", "dsn": "pg@example.com"},
                    },
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": "",
                },
            ),
        ]
    )
    assert rc.keys("backends") == []
    assert rc.get("backends") == ""
    assert rc.get("backends", "one", "type") is NA
    assert rc.get("backends", "one", "dsn") is NA
    assert rc.get("backends", "two", "type") is NA
    assert rc.get("backends", "two", "dsn") is NA


@pytest.mark.parametrize("base_type", SOURCE_TYPES)
@pytest.mark.parametrize("override_type", SOURCE_TYPES)
def test_remove_backend(base_type: str, override_type: str):
    rc = RawConfig()
    rc.read(
        [
            _make_config(
                base_type,
                "base",
                {
                    "backends": {
                        "one": {"type": "sql", "dsn": "sql@example.com"},
                        "two": {"type": "pg", "dsn": "pg@example.com"},
                    },
                },
            ),
            _make_config(
                override_type,
                "override",
                {
                    "backends": "one",
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
