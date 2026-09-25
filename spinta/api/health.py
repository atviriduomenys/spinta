"""`/health` probe.

Reports whether Spinta is operational: whether there is enough free disk space
and enough available memory. The response format, the configuration options and
how the thresholds are chosen are documented in
`docs/en/manual/configuration/health.md`.

The checks are a handful of file system reads, so this is a plain synchronous
handler, which Starlette runs in a worker thread. That keeps a check that does
block, `statvfs` on an unresponsive network mount for one, off the event loop.

Details of a failed check go to the log and never to the response: the probe is
not authenticated, so it must not disclose how the service is deployed.
"""

import logging
import pathlib
import threading

import psutil
from starlette.requests import Request
from starlette.responses import JSONResponse

from spinta.components import Config, Context
from spinta.utils.memory import available_memory

log = logging.getLogger(__name__)

MB = 1024 * 1024

# Only one disk check runs at a time, because `statvfs` blocks on an
# unresponsive mount and every probe would otherwise start another worker thread
# that never returns. Waiting a moment for a check that is merely in progress
# keeps two overlapping probes from reporting a healthy disk as unhealthy.
DISK_CHECK_WAIT = 1
_disk_check = threading.Lock()


def _existing_path(path: pathlib.Path) -> pathlib.Path:
    """Return the closest existing directory of a possibly not yet created path."""
    for candidate in (path, *path.parents):
        if candidate.exists():
            return candidate
    return pathlib.Path(path.anchor or ".")


def check_disk(config: Config) -> bool:
    """Check that enough disk space is left where Spinta keeps its data."""
    if not _disk_check.acquire(timeout=DISK_CHECK_WAIT):
        log.error("Skipping the disk check, the previous one has not finished yet.")
        return False
    try:
        return _free_disk_space_is_enough(config)
    finally:
        _disk_check.release()


def _free_disk_space_is_enough(config: Config) -> bool:
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


def check_memory(config: Config) -> bool:
    """Check that enough memory is left for Spinta to keep working."""
    try:
        available = available_memory() // MB
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


def health(request: Request) -> JSONResponse:
    context: Context = request.state.context
    config: Config = context.get("config")

    dependencies = [
        # Spinta itself answered, everything else it needs is checked separately.
        {"name": "spinta", "healthy": True},
        {"name": "disk", "healthy": check_disk(config)},
        {"name": "memory", "healthy": check_memory(config)},
    ]

    return JSONResponse(
        {
            "healthy": all(dependency["healthy"] for dependency in dependencies),
            "dependencies": dependencies,
        },
        # Probes must always get the current state, never a cached one.
        headers={"Cache-Control": "no-store"},
    )
