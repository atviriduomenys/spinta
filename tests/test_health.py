import inspect
import pathlib
import threading
import time
from types import SimpleNamespace

import pytest

from spinta.api.health import check_disk, check_memory, health
from spinta.testing.client import TestClient

DEPENDENCIES = ["spinta", "disk", "memory"]

MB = 1024 * 1024


def _healthy(data: dict, name: str) -> bool:
    (dependency,) = [dep for dep in data["dependencies"] if dep["name"] == name]
    return dependency["healthy"]


def _disk_config(**kwargs) -> SimpleNamespace:
    return SimpleNamespace(data_path=pathlib.Path("/"), health_min_free_disk_space=1, **kwargs)


def test_health(app: TestClient, mocker):
    # Thresholds are put out of the way, so that the result does not depend on
    # how much disk and memory the machine running the tests happens to have.
    config = app.context.get("config")
    mocker.patch.object(config, "health_min_free_disk_space", 0)
    mocker.patch.object(config, "health_min_free_memory", 0)

    resp = app.get("/health")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "no-store"

    assert resp.json() == {
        "healthy": True,
        "dependencies": [{"name": name, "healthy": True} for name in DEPENDENCIES],
    }


def test_health_is_not_a_coroutine():
    # Starlette runs a synchronous endpoint in a worker thread, which is what
    # keeps a blocking check, `statvfs` on an unresponsive network mount for
    # one, off the event loop. Making this a coroutine would put it back on it.
    assert not inspect.iscoroutinefunction(health)


@pytest.mark.parametrize(
    "name, target",
    [
        ("disk", "psutil.disk_usage"),
        ("memory", "spinta.api.health.available_memory"),
    ],
)
def test_health_resource_check_error(app: TestClient, name: str, target: str, mocker):
    # A failing check must still produce a well formed response, otherwise the
    # probe answers with a 500 error envelope exactly when something is wrong.
    mocker.patch(target, side_effect=OSError("cannot read"))

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


def test_check_memory_reports_the_measured_amount(mocker):
    mocker.patch("spinta.api.health.available_memory", return_value=300 * MB)

    assert check_memory(SimpleNamespace(health_min_free_memory=256)) is True
    assert check_memory(SimpleNamespace(health_min_free_memory=512)) is False


def test_disk_check_waits_for_a_check_that_is_only_slow(mocker):
    # Probes overlapping is normal, so a check that is merely in progress must
    # be waited for rather than reported as a disk that is not there.
    def slow_disk_usage(path):
        time.sleep(0.1)
        return SimpleNamespace(free=100 * MB)

    mocker.patch("psutil.disk_usage", side_effect=slow_disk_usage)
    config = _disk_config()

    results = []
    probes = [threading.Thread(target=lambda: results.append(check_disk(config))) for _ in range(2)]
    for probe in probes:
        probe.start()
    for probe in probes:
        probe.join(10)

    assert results == [True, True]


def test_disk_check_does_not_pile_up(mocker):
    # `statvfs` blocks on an unresponsive mount, so without a guard every probe
    # would start another worker thread that never returns.
    mocker.patch("spinta.api.health.DISK_CHECK_WAIT", 0.05)
    blocked = threading.Event()
    release = threading.Event()

    def blocking_disk_usage(path):
        blocked.set()
        release.wait(10)
        raise OSError("mount is gone")

    mocker.patch("psutil.disk_usage", side_effect=blocking_disk_usage)
    config = _disk_config()

    stuck = threading.Thread(target=check_disk, args=(config,))
    stuck.start()
    try:
        assert blocked.wait(10)

        started = time.monotonic()
        assert check_disk(config) is False
        assert time.monotonic() - started < 1
    finally:
        release.set()
        stuck.join(10)
