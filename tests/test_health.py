import pathlib

import pytest

from spinta.api.health import _available_memory, _cgroup_available_memory
from spinta.testing.client import TestClient

DEPENDENCIES = ["spinta", "disk", "memory"]

MB = 1024 * 1024


def _healthy(data: dict, name: str) -> bool:
    (dependency,) = [dep for dep in data["dependencies"] if dep["name"] == name]
    return dependency["healthy"]


def test_health(app: TestClient):
    resp = app.get("/health")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "no-store"

    assert resp.json() == {
        "healthy": True,
        "dependencies": [{"name": name, "healthy": True} for name in DEPENDENCIES],
    }


@pytest.mark.parametrize(
    "name, target",
    [
        ("disk", "psutil.disk_usage"),
        ("memory", "psutil.virtual_memory"),
    ],
)
def test_health_resource_check_error(app: TestClient, name: str, target: str, mocker):
    # A failing check must still produce a well formed response, otherwise the
    # probe answers with a 500 error envelope exactly when something is wrong.
    mocker.patch(target, side_effect=OSError("cannot read"))
    mocker.patch("spinta.api.health._cgroup_available_memory", return_value=None)

    resp = app.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["healthy"] is False
    assert _healthy(data, name) is False
    assert "cannot read" not in resp.text


@pytest.mark.parametrize("name", ["disk", "memory"])
def test_health_not_enough_resources(app: TestClient, name: str, mocker):
    config = app.context.get("config")
    # Require more resources than any machine running the tests can have.
    required = 1024**3
    mocker.patch.object(config, "health_min_free_disk_space", required)
    mocker.patch.object(config, "health_min_free_memory", required)

    resp = app.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["healthy"] is False
    assert _healthy(data, name) is False


def _write_cgroup(path: pathlib.Path, limit: str, usage: str, inactive_file: str = "0") -> tuple:
    (path / "memory.max").write_text(limit)
    (path / "memory.current").write_text(usage)
    (path / "memory.stat").write_text(f"anon 1000\ninactive_file {inactive_file}\nslab 20\n")
    return (path / "memory.max", path / "memory.current", path / "memory.stat")


def test_cgroup_available_memory(tmp_path: pathlib.Path, mocker):
    # A container limited to 512 MB has to be measured against that limit, not
    # against the memory of the host, which is all `/proc/meminfo` knows about.
    paths = _write_cgroup(tmp_path, limit=str(512 * MB), usage=str(200 * MB))
    mocker.patch("spinta.api.health.CGROUP_MEMORY", paths)

    assert _cgroup_available_memory() == 312 * MB


def test_cgroup_available_memory_ignores_reclaimable_cache(tmp_path: pathlib.Path, mocker):
    # Page cache is charged to the group but reclaimed instead of being killed
    # for, so counting it as used would report a busy container as unhealthy.
    paths = _write_cgroup(tmp_path, limit=str(512 * MB), usage=str(500 * MB), inactive_file=str(400 * MB))
    mocker.patch("spinta.api.health.CGROUP_MEMORY", paths)

    assert _cgroup_available_memory() == 412 * MB


@pytest.mark.parametrize("limit", ["max", "0", str(2**63 - 1)])
def test_cgroup_available_memory_without_a_limit(tmp_path: pathlib.Path, limit: str, mocker):
    paths = _write_cgroup(tmp_path, limit=limit, usage=str(200 * MB))
    mocker.patch("spinta.api.health.CGROUP_MEMORY", paths)
    mocker.patch("spinta.api.health.CGROUP_V1_MEMORY", paths)

    assert _cgroup_available_memory() is None


def test_available_memory_falls_back_to_the_host(mocker):
    mocker.patch("spinta.api.health._cgroup_available_memory", return_value=None)
    memory = mocker.patch("psutil.virtual_memory")
    memory.return_value.available = 700 * MB

    assert _available_memory() == 700
