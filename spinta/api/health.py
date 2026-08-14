"""`/health` probe.

Checks if Spinta is operational:

- if there is enough free disk space,
- if there is enough free RAM.

The response follows the UAPI `health` schema, see
https://ivpk.github.io/uapi/#tag/utility/operation/apiHealth: a `healthy` flag
for the whole service and a `dependencies` list, where each item is a named
dependency with its own `healthy` flag.

Only these flags are reported. Since the probe is not authenticated, it must not
disclose how the service is deployed, so paths, free space and errors are
written to the log instead of to the response.

Checks read from the file system, which can block, so they run in worker threads
rather than on the event loop.
"""

from __future__ import annotations

import asyncio
import logging
import pathlib

import psutil
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse

from spinta.components import Config, Context

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


def _dependency(name: str, healthy: bool) -> dict:
    return {"name": name, "healthy": healthy}


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


async def _check_health(config: Config) -> dict:
    disk, memory = await asyncio.gather(
        run_in_threadpool(_check_disk, config),
        run_in_threadpool(_check_memory, config),
    )
    dependencies = [
        # Spinta itself answered, everything else it needs is checked separately.
        _dependency("spinta", True),
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

    return JSONResponse(
        await _check_health(config),
        # Probes must always get the current state, never a cached one.
        headers={"Cache-Control": "no-store"},
    )
