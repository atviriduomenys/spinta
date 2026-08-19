import pathlib

import pytest

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


def test_resolve_servers_keeps_given_service_path():
    config = UdtsConfig(servers=[{"url": f"https://get.data.gov.lt/{SERVICE}/"}])

    assert config.resolve_servers(SERVICE) == [{"url": f"https://get.data.gov.lt/{SERVICE}"}]


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
