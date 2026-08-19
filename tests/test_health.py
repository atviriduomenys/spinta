import asyncio
import pathlib
import time

import pytest

from spinta import commands
from spinta.api.health import HealthCache, _available_memory, _cgroup_available_memory
from spinta.backends.constants import WAIT_CONNECT_TIMEOUT
from spinta.backends.helpers import get_all_backends
from spinta.testing.client import TestClient

DEPENDENCIES = ["spinta", "backends", "disk", "memory"]

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


def test_health_backend_unavailable(app: TestClient, mocker):
    mocker.patch("spinta.commands.wait", return_value=False)

    resp = app.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["healthy"] is False
    assert _healthy(data, "backends") is False
    assert _healthy(data, "spinta") is True


def test_health_backend_error(app: TestClient, mocker):
    mocker.patch("spinta.commands.wait", side_effect=RuntimeError("connection refused"))

    resp = app.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["healthy"] is False
    assert _healthy(data, "backends") is False
    # Driver errors must not leak to an unauthenticated caller.
    assert "connection refused" not in resp.text


def test_health_bounds_the_driver_but_startup_does_not(app: TestClient, mocker):
    # Waiting for backends to come up must keep waiting for as long as the
    # caller allows, only the probe asks the driver to give up.
    context = app.context
    config = context.get("config")
    mocker.patch.object(config, "health_backend_timeout", 7)
    timeouts = []

    def wait(context, backend, **kwargs):
        timeouts.append(context.get(WAIT_CONNECT_TIMEOUT) if context.has(WAIT_CONNECT_TIMEOUT) else None)
        return True

    mocker.patch("spinta.commands.wait", side_effect=wait)

    app.get("/health")
    assert timeouts and set(timeouts) == {7}

    # The same command called the way startup calls it asks for no timeout.
    timeouts.clear()
    backend = next(iter(get_all_backends(context, context.get("store"))))
    commands.wait(context, backend, fail=False)
    assert timeouts == [None]


def test_health_has_no_timeout_of_its_own(app: TestClient, mocker):
    # Deliberate: cancelling the await cannot stop the thread doing the work, so
    # the probe waits for whatever it started. How long a check may take is the
    # driver's connect timeout, and how long the probe may take is up to whoever
    # probes. A check that ignores the timeout therefore holds up the probe.
    blocking = 0.3
    mocker.patch("spinta.commands.wait", side_effect=lambda *args, **kwargs: time.sleep(blocking))
    mocker.patch.object(app.context.get("config"), "health_backend_timeout", 0.01)

    started = time.monotonic()
    resp = app.get("/health")

    assert resp.status_code == 200
    assert time.monotonic() - started >= blocking


def test_health_backend_timeout_of_zero_lets_the_driver_wait(app: TestClient, mocker):
    # `0` means the drivers keep their own defaults, which for `psycopg2` is to
    # wait indefinitely, so it turns off bounding rather than tightening it.
    mocker.patch.object(app.context.get("config"), "health_backend_timeout", 0)
    timeouts = []

    def wait(context, backend, **kwargs):
        timeouts.append(context.get(WAIT_CONNECT_TIMEOUT) if context.has(WAIT_CONNECT_TIMEOUT) else None)
        return True

    mocker.patch("spinta.commands.wait", side_effect=wait)

    app.get("/health")

    assert timeouts and set(timeouts) == {0}


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


def test_health_is_cached(app: TestClient, mocker):
    # Without a cache anyone could use the unauthenticated probe to open as many
    # backend connections as they like.
    wait = mocker.patch("spinta.commands.wait", return_value=True)

    first = app.get("/health")
    checked = wait.call_count
    assert checked > 0

    second = app.get("/health")
    assert second.json() == first.json()
    assert wait.call_count == checked


def test_health_cache_expires(app: TestClient, mocker):
    wait = mocker.patch("spinta.commands.wait", return_value=True)
    mocker.patch.object(app.context.get("config"), "health_cache_time", 0.05)

    app.get("/health")
    checked = wait.call_count
    time.sleep(0.1)

    app.get("/health")
    assert wait.call_count > checked


def test_health_cache_can_be_disabled(app: TestClient, mocker):
    wait = mocker.patch("spinta.commands.wait", return_value=True)
    mocker.patch.object(app.context.get("config"), "health_cache_time", 0)

    app.get("/health")
    checked = wait.call_count

    app.get("/health")
    assert wait.call_count == checked * 2


def test_health_cache_shares_concurrent_probes():
    cache = HealthCache()
    checks = 0

    async def check() -> dict:
        nonlocal checks
        checks += 1
        await asyncio.sleep(0.05)
        return {"healthy": True, "dependencies": []}

    async def probe_concurrently():
        return await asyncio.gather(*(cache.get(60, check) for _ in range(10)))

    results = asyncio.run(probe_concurrently())

    assert checks == 1
    assert results == [{"healthy": True, "dependencies": []}] * 10


def test_health_cache_checks_one_probe_at_a_time_when_disabled():
    # With the cache off every probe gets its own check, but checks must still
    # not overlap, otherwise probes can be used to pile up backend connections.
    cache = HealthCache()
    running = 0
    most = 0

    async def check() -> dict:
        nonlocal running, most
        running += 1
        most = max(most, running)
        await asyncio.sleep(0.05)
        running -= 1
        return {"healthy": True, "dependencies": []}

    async def probe_concurrently():
        await asyncio.gather(*(cache.get(0, check) for _ in range(5)))

    asyncio.run(probe_concurrently())

    assert most == 1


def test_health_cache_expiry_starts_when_check_finishes():
    # Measuring the expiry from before the check means a check taking as long as
    # `cache_time` returns an already expired result, and the next probe checks
    # everything again.
    cache = HealthCache()
    cache_time = 0.2
    checks = 0

    async def check() -> dict:
        nonlocal checks
        checks += 1
        await asyncio.sleep(cache_time)
        return {"healthy": True, "dependencies": []}

    async def probe_twice():
        await cache.get(cache_time, check)
        await cache.get(cache_time, check)

    asyncio.run(probe_twice())

    assert checks == 1


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
