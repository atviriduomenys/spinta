"""`/health` probe.

Checks if Spinta and everything it depends on is operational:

- if all backends (data sources) are reachable,
- if there is enough free disk space,
- if there is enough free RAM.

The response follows the UAPI `health` schema, see
https://ivpk.github.io/uapi/#tag/utility/operation/apiHealth: a `healthy` flag
for the whole service and a `dependencies` list, where each item is a named
dependency with its own `healthy` flag.

Only these flags are reported. Since the probe is not authenticated, it must not
disclose how the service is deployed, so individual backend names, paths, free
space and driver errors are written to the log instead of to the response.

Checks block, so they run in worker threads, and they run at most one probe at a
time. Nothing here cancels a check: how long a backend check may take is the
driver's own connect timeout, see `WAIT_CONNECT_TIMEOUT`, and how long the probe
as a whole may take is up to whoever probes, which for a container or load
balancer probe is its own timeout.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import pathlib
import time
import weakref
from typing import Awaitable, Callable

import psutil
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse

from spinta import commands
from spinta.backends import Backend
from spinta.backends.constants import WAIT_CONNECT_TIMEOUT
from spinta.backends.helpers import get_all_backends
from spinta.components import Config, Context, Store

log = logging.getLogger(__name__)

MB = 1024 * 1024

# Memory accounting of the control group this process belongs to. `psutil` reads
# `/proc/meminfo`, which inside a container reports the memory of the host, not
# the limit the container was given, so a container about to be killed for using
# up its memory would still look perfectly healthy.
CGROUP_MEMORY = (
    # cgroup v2
    pathlib.Path("/sys/fs/cgroup/memory.max"),
    pathlib.Path("/sys/fs/cgroup/memory.current"),
    pathlib.Path("/sys/fs/cgroup/memory.stat"),
)
CGROUP_V1_MEMORY = (
    pathlib.Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    pathlib.Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    pathlib.Path("/sys/fs/cgroup/memory/memory.stat"),
)

# cgroup v1 reports a limit near the maximum of a 64 bit integer when there is
# none, and cgroup v2 reports `max`.
NO_CGROUP_LIMIT = 2**62


class HealthCache:
    """Keeps the health result for a short while, and checks one probe at a time.

    The probe is not authenticated and every check opens a real connection to
    every backend, so without this anyone could use the probe to open as many
    backend connections as they like. `cache_time` controls how long a finished
    result is served again; with `0` every probe gets a check of its own, still
    one at a time.
    """

    def __init__(self):
        self._result: dict | None = None
        self._expires: float = 0.0
        # An `asyncio.Lock` belongs to the event loop it was created in. A served
        # app has a single loop; more than one appears in tests, where each probe
        # may get its own, and those then share the result but not the lock.
        self._locks: weakref.WeakKeyDictionary = weakref.WeakKeyDictionary()

    def _lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if (lock := self._locks.get(loop)) is None:
            lock = self._locks[loop] = asyncio.Lock()
        return lock

    async def get(self, cache_time: float, check: Callable[[], Awaitable[dict]]) -> dict:
        async with self._lock():
            if self._result is not None and time.monotonic() < self._expires:
                return self._result

            result = await check()
            self._result = result if cache_time > 0 else None
            # Measured from when the check finished, not from when it started,
            # otherwise a check taking as long as `cache_time` returns a result
            # that has already expired.
            self._expires = time.monotonic() + cache_time
            return result


def _dependency(name: str, healthy: bool) -> dict:
    return {"name": name, "healthy": healthy}


async def _check_backend(context: Context, backend: Backend, timeout: float) -> bool:
    # Checks run concurrently, in threads, so each one gets its own context
    # state instead of mutating the shared request context.
    with context.fork("health") as fork:
        # Waiting for backends to come up lets the driver wait as long as it
        # likes, but a probe should not wait for a backend that is not answering
        # much longer than it takes to find that out.
        fork.set(WAIT_CONNECT_TIMEOUT, timeout)
        try:
            return await run_in_threadpool(commands.wait, fork, backend, fail=False)
        except Exception:
            log.exception("Error while checking if backend %r is available.", backend.name)
            return False


async def _check_backends(context: Context, timeout: float) -> bool:
    """Check every backend, reporting them as a single dependency.

    Which backends are configured is a deployment detail, so they are checked
    individually, but reported as one flag, which is only healthy if every
    backend is.
    """
    store: Store = context.get("store")
    try:
        backends = get_all_backends(context, store)
    except Exception:
        log.exception("Error while collecting backends.")
        return False

    results = await asyncio.gather(*(_check_backend(context, backend, timeout) for backend in backends))
    return all(results)


def _existing_path(path: pathlib.Path) -> pathlib.Path:
    """Return the closest existing directory of a possibly not yet created path."""
    for candidate in (path, *path.parents):
        if candidate.exists():
            return candidate
    return pathlib.Path(path.anchor or ".")


def _check_disk(config: Config) -> bool:
    path = config.data_path
    try:
        # `Path.exists()` raises, for example when a parent directory may not be
        # searched, so resolving the path has to be guarded as well.
        path = _existing_path(path)
        free = psutil.disk_usage(str(path)).free // MB
    except Exception:
        log.exception("Error while checking free disk space of %s.", path)
        return False

    if free < config.health_min_free_disk_space:
        log.error(
            "Not enough free disk space on %s: %s MB free, %s MB required.",
            path,
            free,
            config.health_min_free_disk_space,
        )
        return False
    return True


def _read_int(path: pathlib.Path) -> int | None:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def _read_inactive_file(path: pathlib.Path) -> int:
    """Page cache that can be reclaimed, so it does not count as used memory."""
    try:
        stat = path.read_text()
    except OSError:
        return 0

    for line in stat.splitlines():
        # `inactive_file` in cgroup v2, `total_inactive_file` in cgroup v1.
        name, _, value = line.partition(" ")
        if name in ("inactive_file", "total_inactive_file"):
            try:
                return int(value)
            except ValueError:
                return 0
    return 0


def _cgroup_available_memory() -> int | None:
    """Memory left within this control group's limit, or None if not limited."""
    for limit_path, usage_path, stat_path in (CGROUP_MEMORY, CGROUP_V1_MEMORY):
        limit = _read_int(limit_path)
        usage = _read_int(usage_path)
        if limit is None or usage is None or limit <= 0 or limit >= NO_CGROUP_LIMIT:
            continue

        # What the kernel considers used when deciding to kill the container.
        working_set = max(usage - _read_inactive_file(stat_path), 0)
        return max(limit - working_set, 0)
    return None


def _available_memory() -> int:
    available = _cgroup_available_memory()
    if available is None:
        available = psutil.virtual_memory().available
    return available // MB


def _check_memory(config: Config) -> bool:
    try:
        available = _available_memory()
    except Exception:
        log.exception("Error while checking available memory.")
        return False

    if available < config.health_min_free_memory:
        log.error(
            "Not enough available memory: %s MB available, %s MB required.",
            available,
            config.health_min_free_memory,
        )
        return False
    return True


async def _check_health(context: Context, config: Config) -> dict:
    # Both resource checks read from the file system, which can block, for
    # example on a hung mount, so neither may run on the event loop.
    disk, memory = await asyncio.gather(
        run_in_threadpool(_check_disk, config),
        run_in_threadpool(_check_memory, config),
    )
    dependencies = [
        # Spinta itself answered, everything else it needs is checked separately.
        _dependency("spinta", True),
        _dependency("backends", await _check_backends(context, config.health_backend_timeout)),
        _dependency("disk", disk),
        _dependency("memory", memory),
    ]
    return {
        "healthy": all(dependency["healthy"] for dependency in dependencies),
        "dependencies": dependencies,
    }


async def health(request: Request) -> JSONResponse:
    context: Context = request.state.context
    config: Config = context.get("config")
    cache: HealthCache = request.app.state.health_cache

    return JSONResponse(
        await cache.get(config.health_cache_time, functools.partial(_check_health, context, config)),
        # However long the result is kept here, it must never be kept by a shared
        # cache, otherwise probes stop seeing the current state.
        headers={"Cache-Control": "no-store"},
    )
