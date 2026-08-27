import pathlib
import re

import pytest

from spinta.exceptions import InvalidUdtsConfig
from spinta.manifests.open_api import udts_config
from spinta.manifests.open_api.service import (
    datasets_under_service,
    find_services,
    is_service_level_path,
    relative_path,
    service_path_of,
)
from spinta.manifests.open_api.udts_config import UdtsConfig

SERVICE = "datasets/gov/rc/jadis/at280/1"


@pytest.mark.parametrize(
    "dataset_name, expected",
    [
        ("datasets/gov/rc/jadis/at280/1/at280_israsas", "datasets/gov/rc/jadis/at280/1"),
        # Version segment is optional.
        ("datasets/gov/vssa/smarty/udts_sm_test/pim", "datasets/gov/vssa/smarty/udts_sm_test"),
        ("datasets/gov/rc/jadis/at280/1/at280_israsas/sub", "datasets/gov/rc/jadis/at280/1"),
        # Version is a positive integer, so `0` is a data set name.
        ("datasets/gov/rc/jadis/at280/0/israsas", "datasets/gov/rc/jadis/at280"),
        # Data set sitting at the data service level itself.
        ("datasets/gov/rc/jadis/at280/1", "datasets/gov/rc/jadis/at280/1"),
        ("datasets/gov/rc/jadis/at280", "datasets/gov/rc/jadis/at280"),
        # Does not reach data service level.
        ("datasets/gov/example", None),
        ("services/gov/rc/jadis/at280/1/israsas", None),
    ],
)
def test_service_path_of(dataset_name, expected):
    assert service_path_of(dataset_name) == expected


@pytest.mark.parametrize(
    "path, expected",
    [
        ("datasets/gov/rc/jadis/at280/1", True),
        ("datasets/gov/rc/jadis/at280", True),
        ("datasets/gov/rc/jadis/at280/1/at280_israsas", False),
        ("datasets/gov/rc/jadis", False),
        # Version segment is a plain number, `v1` is not a version.
        ("datasets/gov/rc/jadis/at280/v1", False),
        # Version is a positive integer, `0` and non ASCII digits are not.
        ("datasets/gov/rc/jadis/at280/0", False),
        ("datasets/gov/rc/jadis/at280/²", False),
    ],
)
def test_is_service_level_path(path, expected):
    assert is_service_level_path(path) is expected


def test_datasets_under_service_matches_on_segment_boundary():
    names = [
        "datasets/gov/rc/jadis/at280/1",
        "datasets/gov/rc/jadis/at280/1/at280_israsas",
        "datasets/gov/rc/jadis/at280/10/at280_kitas",
        "datasets/gov/rc/ntr/n249/1/n249_israsas",
    ]

    assert datasets_under_service(names, SERVICE) == [
        "datasets/gov/rc/jadis/at280/1",
        "datasets/gov/rc/jadis/at280/1/at280_israsas",
    ]


def test_datasets_under_service_does_not_reach_into_a_versioned_service():
    """An unversioned path is a prefix of every versioned service of the same name."""
    names = [
        "datasets/gov/rc/jadis/at280/foo",
        "datasets/gov/rc/jadis/at280/1/bar",
    ]

    assert datasets_under_service(names, "datasets/gov/rc/jadis/at280") == ["datasets/gov/rc/jadis/at280/foo"]
    assert datasets_under_service(names, SERVICE) == ["datasets/gov/rc/jadis/at280/1/bar"]


def test_datasets_under_a_path_of_another_shape_are_matched_by_prefix():
    """Such a path is accepted with a warning, see `spinta udts oas --path`."""
    names = ["datasets/gov/rc/jadis/at280/1/at280_israsas", "datasets/gov/rc/jadis/at280/1/at280_adresai"]

    assert datasets_under_service(names, "datasets/gov/rc/jadis/at280/1/at280_israsas") == [
        "datasets/gov/rc/jadis/at280/1/at280_israsas",
    ]


def test_find_services_groups_datasets():
    names = [
        "datasets/gov/rc/jadis/at280/1/at280_israsas",
        "datasets/gov/rc/jadis/at280/1/at280_adresai",
        "datasets/gov/rc/ntr/n249/1/n249_israsas",
        "datasets/gov/example",
    ]

    assert find_services(names) == {
        "datasets/gov/rc/jadis/at280/1": [
            "datasets/gov/rc/jadis/at280/1/at280_adresai",
            "datasets/gov/rc/jadis/at280/1/at280_israsas",
        ],
        "datasets/gov/rc/ntr/n249/1": ["datasets/gov/rc/ntr/n249/1/n249_israsas"],
    }


def test_relative_path():
    assert relative_path(f"{SERVICE}/at280_israsas", SERVICE) == "at280_israsas"
    assert relative_path(SERVICE, SERVICE) == ""


def test_resolve_servers_appends_service_path():
    config = UdtsConfig(servers=[{"url": "https://get.data.gov.lt", "description": "Production"}])

    assert config.resolve_servers(SERVICE) == [
        {"url": f"https://get.data.gov.lt/{SERVICE}", "description": "Production"},
    ]


@pytest.mark.parametrize(
    "url",
    [
        "https://get.data.gov.lt",
        "https://get.data.gov.lt/",
        # A trailing slash on the path, not on the whole URL.
        "https://get.data.gov.lt/?env=prod",
    ],
)
def test_resolve_servers_appends_to_a_host_only_url(url):
    config = UdtsConfig(servers=[{"url": url}])

    resolved = config.resolve_servers(SERVICE)[0]["url"]

    assert resolved.startswith(f"https://get.data.gov.lt/{SERVICE}")
    assert config.resolve_token_url([{"url": resolved}]).startswith(f"https://get.data.gov.lt/{SERVICE}/:token")


def test_resolve_servers_keeps_given_service_path():
    config = UdtsConfig(servers=[{"url": f"https://get.data.gov.lt/{SERVICE}/"}])

    assert config.resolve_servers(SERVICE) == [{"url": f"https://get.data.gov.lt/{SERVICE}"}]


def test_resolve_servers_warns_on_a_doubled_slash():
    """An API gateway derives a context path with the doubled slash from it."""
    config = UdtsConfig(servers=[{"url": f"https://get.data.gov.lt//{SERVICE}"}])

    with pytest.warns(UserWarning, match="does not match data service path"):
        config.resolve_servers(SERVICE)


def test_resolve_servers_warns_on_different_path():
    config = UdtsConfig(servers=[{"url": "https://get.data.gov.lt/kitas/kelias"}])

    with pytest.warns(UserWarning, match="does not match data service path"):
        servers = config.resolve_servers(SERVICE)

    assert servers == [{"url": "https://get.data.gov.lt/kitas/kelias"}]


def test_config_from_path_warns_on_unknown_key(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text("servers:\n  - url: https://get.data.gov.lt\nnezinomas: 1\n")

    with pytest.warns(UserWarning, match="unknown UDTS configuration key 'nezinomas'"):
        config = UdtsConfig.from_path(path)

    assert config.servers == [{"url": "https://get.data.gov.lt"}]


def test_example_config_file_is_valid():
    """Example file shipped in the repository must stay loadable."""
    example = pathlib.Path(udts_config.__file__).parent / "udts_cfg.example.yml"
    config = UdtsConfig.from_path(example)

    assert config.info["title"]
    assert config.resolve_servers(SERVICE)[0]["url"].endswith(SERVICE)
    assert config.resolve_token_url(config.resolve_servers(SERVICE))


@pytest.mark.parametrize(
    "config, error",
    [
        # Falsy values are not a missing value, they are malformed ones.
        ("[]\n", "configuration must be a mapping"),
        ("false\n", "configuration must be a mapping"),
        ("servers: {}\n", "`servers` must be a list"),
        ("servers: false\n", "`servers` must be a list"),
        ("info: something\n", "`info` must be a mapping"),
        ("auth: something\n", "`auth` must be a mapping"),
        ("servers: something\n", "`servers` must be a list"),
        ("servers:\n  - https://get.data.gov.lt\n", "every `servers` entry must be a mapping"),
        ("servers:\n  - description: Production\n", "has no `url`"),
        # An URL without a scheme and host would silently lose the data service
        # path; `urlsplit` reads `localhost:8080` as scheme `localhost`.
        ("servers:\n  - url: get.data.gov.lt\n", "has no scheme and host"),
        ("servers:\n  - url: localhost:8080\n", "has a scheme but no host"),
        # OpenAPI schema types these as `uri`, a relative one is not enough.
        ("externalDocs:\n  url: /docs\n", "`externalDocs.url` '/docs' has no scheme and host"),
        ("info:\n  termsOfService: /terms\n", "`info.termsOfService` '/terms' has no scheme and host"),
        ("auth:\n  token_url: /auth/token\n", "`auth.token_url` '/auth/token' has no scheme and host"),
        ("servers:\n  - url: https:example.com\n", "has a scheme but no host"),
    ],
)
def test_config_rejects_malformed_values(tmp_path, config, error):
    path = tmp_path / "vartai.yml"
    path.write_text(config, encoding="utf-8")

    with pytest.raises(InvalidUdtsConfig) as raised:
        UdtsConfig.from_path(path)

    assert error in str(raised.value)


def test_config_accepts_relative_server_url(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text(f"servers:\n  - url: /{SERVICE}\n", encoding="utf-8")

    config = UdtsConfig.from_path(path)

    assert config.resolve_servers(SERVICE) == [{"url": f"/{SERVICE}"}]


def test_config_reports_missing_file(tmp_path):
    with pytest.raises(InvalidUdtsConfig, match="can not be read"):
        UdtsConfig.from_path(tmp_path / "nera.yml")


def test_config_reports_malformed_yaml(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text("info: [\n", encoding="utf-8")

    with pytest.raises(InvalidUdtsConfig, match="not a valid YAML file"):
        UdtsConfig.from_path(path)


@pytest.mark.parametrize(
    "token_url, error",
    [
        (123, "must be a non empty string"),
        ('""', "must be a non empty string"),
        ("localhost:8080", "has a scheme but no host"),
        ("http://am.example.lt/auth/token", "must use HTTPS"),
        ("ftp://am.example.lt/auth/token", "must use HTTPS"),
    ],
)
def test_config_rejects_malformed_token_url(tmp_path, token_url, error):
    path = tmp_path / "vartai.yml"
    path.write_text(f"auth:\n  token_url: {token_url}\n", encoding="utf-8")

    with pytest.raises(InvalidUdtsConfig, match=error):
        UdtsConfig.from_path(path)


def test_config_accepts_token_url(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text("auth:\n  token_url: https://am.example.lt/auth/token\n", encoding="utf-8")

    config = UdtsConfig.from_path(path)

    assert config.resolve_token_url([]) == "https://am.example.lt/auth/token"


def test_config_warns_when_the_token_url_stays_relative(tmp_path):
    """A server URL can be relative, the token endpoint of the flow can not."""
    path = tmp_path / "vartai.yml"
    path.write_text(f"servers:\n  - url: /{SERVICE}\n", encoding="utf-8")

    with pytest.warns(UserWarning, match="is left relative"):
        UdtsConfig.from_path(path)


def test_config_rejects_token_url_derived_from_insecure_server(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text("servers:\n  - url: http://get.data.gov.lt\n", encoding="utf-8")

    with pytest.raises(InvalidUdtsConfig, match="token URL derived from the first server must use HTTPS"):
        UdtsConfig.from_path(path)


def test_config_allows_insecure_server_with_separate_secure_token_url(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text(
        "servers:\n  - url: http://localhost:8000\nauth:\n  token_url: https://am.example.lt/auth/token\n",
        encoding="utf-8",
    )

    config = UdtsConfig.from_path(path)

    assert config.auth["token_url"] == "https://am.example.lt/auth/token"


@pytest.mark.parametrize("config", ["", "# only a comment\n"])
def test_config_accepts_empty_file(tmp_path, config):
    path = tmp_path / "vartai.yml"
    path.write_text(config, encoding="utf-8")

    assert UdtsConfig.from_path(path).servers == []


@pytest.mark.parametrize(
    "server, expected",
    [
        (f"https://get.data.gov.lt/{SERVICE}", f"https://get.data.gov.lt/{SERVICE}/:token"),
        # Query and fragment stay where they belong.
        (f"https://get.data.gov.lt/{SERVICE}?env=prod", f"https://get.data.gov.lt/{SERVICE}/:token?env=prod"),
        (f"https://get.data.gov.lt/{SERVICE}#frag", f"https://get.data.gov.lt/{SERVICE}/:token#frag"),
        (f"/{SERVICE}", f"/{SERVICE}/:token"),
    ],
)
def test_default_token_url_is_built_from_the_server_path(server, expected):
    config = UdtsConfig(servers=[{"url": server}])

    assert config.resolve_token_url(config.resolve_servers(SERVICE)) == expected


def test_default_token_url_without_servers():
    assert UdtsConfig().resolve_token_url([]) == "/:token"


def test_resolve_servers_drops_a_trailing_slash_of_the_path():
    """An API gateway falls back to the API title when the path is left empty."""
    config = UdtsConfig(servers=[{"url": f"https://get.data.gov.lt/{SERVICE}/?env=prod"}])

    assert config.resolve_servers(SERVICE) == [{"url": f"https://get.data.gov.lt/{SERVICE}?env=prod"}]


@pytest.mark.parametrize(
    "config, error",
    [
        # `urlsplit` raises on such a value.
        ('servers:\n  - url: "https://["\n', "is not a valid URL"),
        ("info:\n  version: 1\n", "`info.version` must be a string"),
        ("info:\n  license: something\n", "`info.license` must be a mapping"),
        ("externalDocs:\n  description: docs\n", "`externalDocs.url` must be a non empty string"),
        ("externalDocs:\n  url: https://ivpk.github.io/uapi\n  description: 1\n", "must be a string"),
        # Whole server mapping is copied into the document.
        ("servers:\n  - url: https://host\n    description: 1\n", "must be a string"),
        # A relative URL is parsed too.
        ('servers:\n  - url: "//["\n', "is not a valid URL"),
        # A port is parsed only when it is read.
        ("servers:\n  - url: https://host:abc\n", "is not a valid URL"),
        # `urlsplit` parses an URL, it does not validate one.
        ('servers:\n  - url: "https://host/a b"\n', "it holds whitespace"),
        ('servers:\n  - url: "https://host/%zz"\n', "percent sign has to start an escape"),
        # Server variables are not supported, so a template has nothing to fill it.
        ('servers:\n  - url: "https://{env}.example.lt"\n', "is a template, which is not supported"),
        ('servers:\n  - url: "https://host:{port}"\n', "is a template, which is not supported"),
        # OpenAPI License Object requires a name.
        ("info:\n  license:\n    url: https://example.com\n", "`info.license.name` must be a non empty string"),
        ("info:\n  contact:\n    name: 1\n", "`info.contact.name` must be a string"),
        # `info.contact.email` is `format: email` in the OpenAPI schema.
        ("info:\n  contact:\n    email: not-an-email\n", "is not an email address"),
        ("info:\n  termsOfService: 1\n", "`info.termsOfService` must be a non empty string"),
        # RFC 3986 does not allow these characters in URI references.
        ("servers:\n  - url: 'https://host\\evil/path'\n", "invalid character"),
        ("servers:\n  - url: 'https://host|evil/path'\n", "invalid character"),
        ("servers:\n  - url: 'https://host/ą'\n", "invalid character"),
        # OpenAPI License Object allows either one.
        (
            "info:\n  license:\n    name: CC-BY 4.0\n    identifier: CC-BY-4.0\n    url: https://example.com\n",
            "either an `identifier` or an `url`",
        ),
    ],
)
def test_config_rejects_values_openapi_would_reject(tmp_path, config, error):
    path = tmp_path / "vartai.yml"
    path.write_text(config, encoding="utf-8")

    with pytest.raises(InvalidUdtsConfig, match=error):
        UdtsConfig.from_path(path)


def test_config_warns_about_unknown_keys_of_any_type(tmp_path):
    """Keys of different types do not sort together."""
    path = tmp_path / "vartai.yml"
    path.write_text("1: x\nnezinomas: y\n", encoding="utf-8")

    with pytest.warns(UserWarning, match="unknown UDTS configuration key"):
        assert UdtsConfig.from_path(path).servers == []


def test_config_reports_a_non_utf8_file(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_bytes(b"info:\n  title: \xff\xfe\n")

    with pytest.raises(InvalidUdtsConfig, match="is not an UTF-8 file"):
        UdtsConfig.from_path(path)


@pytest.mark.parametrize(
    "config, warning, kept",
    [
        ("info:\n  titel: JADIS\n", "`info` key 'titel' is not supported", {"info": {}}),
        (
            "servers:\n  - url: https://host\n    descrption: Production\n",
            "`servers` entry key 'descrption' is not supported",
            {"servers": [{"url": "https://host"}]},
        ),
        (
            "info:\n  contact:\n    naem: RC\n",
            "`info.contact` key 'naem' is not supported",
            {"info": {"contact": {}}},
        ),
        ("auth:\n  tokenurl: https://host/auth/token\n", "`auth` key 'tokenurl' is not supported", {"auth": {}}),
    ],
)
def test_config_leaves_out_unknown_nested_keys(tmp_path, config, warning, kept):
    """Such a key is usually a typo, which would also leave the field unset."""
    path = tmp_path / "vartai.yml"
    path.write_text(config, encoding="utf-8")

    with pytest.warns(UserWarning, match=re.escape(warning)):
        config = UdtsConfig.from_path(path)

    for key, value in kept.items():
        assert getattr(config, key) == value


def test_config_leaves_out_server_variables(tmp_path):
    """An environment is described by an URL of its own, not by a template."""
    path = tmp_path / "vartai.yml"
    path.write_text(
        "servers:\n  - url: https://host\n    variables:\n      port:\n        default: '443'\n",
        encoding="utf-8",
    )

    with pytest.warns(UserWarning, match=re.escape("`servers` entry key 'variables' is not supported")):
        config = UdtsConfig.from_path(path)

    assert config.servers == [{"url": "https://host"}]


def test_config_keeps_openapi_fields_and_extensions(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text(
        "info:\n  title: JADIS\n  termsOfService: https://example.lt\n  x-vidinis: taip\n",
        encoding="utf-8",
    )

    config = UdtsConfig.from_path(path)

    assert config.info == {"title": "JADIS", "termsOfService": "https://example.lt", "x-vidinis": "taip"}


def test_config_accepts_a_percent_escaped_url(tmp_path):
    path = tmp_path / "vartai.yml"
    path.write_text("servers:\n  - url: https://host/a%20b\n", encoding="utf-8")

    assert UdtsConfig.from_path(path).servers == [{"url": "https://host/a%20b"}]


def test_config_requires_absolute_openapi_urls(tmp_path):
    """Only `servers[].url` is a `uri-reference` in the OpenAPI schema."""
    path = tmp_path / "vartai.yml"
    path.write_text(
        "info:\n"
        "  termsOfService: https://example.lt/terms\n"
        "  contact:\n"
        "    url: https://example.lt/contacts\n"
        "  license:\n"
        "    name: Example\n"
        "    url: https://example.lt/license\n"
        "externalDocs:\n"
        "  url: https://example.lt/docs\n",
        encoding="utf-8",
    )

    config = UdtsConfig.from_path(path)

    assert config.info["termsOfService"] == "https://example.lt/terms"
    assert config.external_docs["url"] == "https://example.lt/docs"

    for field in ("info:\n  contact:\n    url: contacts\n", "info:\n  license:\n    name: E\n    url: ../license\n"):
        path.write_text(field, encoding="utf-8")
        with pytest.raises(InvalidUdtsConfig, match="has no scheme and host"):
            UdtsConfig.from_path(path)


@pytest.mark.parametrize(
    "config, attribute, expected",
    [
        # A null of a known field is the field left out, OpenAPI holds no nulls.
        ("servers:\n  - url: https://host\n    description: null\n", "servers", [{"url": "https://host"}]),
        # This one used to reach `_clean_info` and be treated as a mapping.
        ("info:\n  contact: null\n", "info", {}),
        ("info:\n  title: null\n", "info", {}),
        ("auth:\n  token_url: null\n", "auth", {}),
        # An extension holds whatever it holds.
        ("info:\n  x-vidinis: null\n", "info", {"x-vidinis": None}),
    ],
)
def test_config_treats_a_null_of_a_known_field_as_absent(tmp_path, config, attribute, expected):
    path = tmp_path / "vartai.yml"
    path.write_text(config, encoding="utf-8")

    assert getattr(UdtsConfig.from_path(path), attribute) == expected
