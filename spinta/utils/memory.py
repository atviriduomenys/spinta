"""How much memory this process is still allowed to use.

`psutil` reads `/proc/meminfo`, which is not namespaced: inside a container it
reports the memory of the host, not the limit the container was given. A
container about to be killed for using up its memory would therefore look
perfectly healthy. So the limit of the control group this process belongs to is
taken into account when it has one.

Neither number replaces the other. A cgroup limit is a ceiling, not a
reservation, so a container with room to spare is still killed when the machine
itself runs out of memory. What this process can really use is whichever of the
two is tighter.
"""

import pathlib
from typing import NamedTuple

import psutil


class CgroupLayout(NamedTuple):
    """Where one cgroup version keeps its memory accounting."""

    root: pathlib.Path
    # Name this version goes by in `/proc/self/cgroup`, empty for cgroup v2.
    controller: str
    limit: str
    usage: str
    stat: str


CGROUP_LAYOUTS = (
    CgroupLayout(pathlib.Path("/sys/fs/cgroup"), "", "memory.max", "memory.current", "memory.stat"),
    CgroupLayout(
        pathlib.Path("/sys/fs/cgroup/memory"),
        "memory",
        "memory.limit_in_bytes",
        "memory.usage_in_bytes",
        "memory.stat",
    ),
)

# Tells which cgroup this process belongs to. A container sees its own limit at
# the mount root, but a systemd service does not: it runs in a nested cgroup,
# such as `/system.slice/spinta.service`, while the root stays unlimited.
PROC_CGROUP = pathlib.Path("/proc/self/cgroup")

# cgroup v1 reports a limit near the maximum of a 64 bit integer when there is
# none, and cgroup v2 reports `max`.
NO_CGROUP_LIMIT = 2**62


def _read_int(path: pathlib.Path) -> int | None:
    """Read a cgroup number, or None when there is no limit to read.

    Only a missing file and the `max` marker mean that. Anything else that goes
    wrong is left to raise, so that a cgroup which cannot be read is reported as
    unhealthy rather than mistaken for an unlimited one.
    """
    try:
        value = path.read_text().strip()
    except (FileNotFoundError, NotADirectoryError):
        return None

    return None if value == "max" else int(value)


def _read_inactive_file(path: pathlib.Path) -> int:
    """Page cache that can be reclaimed, so it does not count as used memory."""
    try:
        lines = path.read_text().splitlines()
    except OSError:
        return 0

    fields = {}
    for line in lines:
        name, _, value = line.partition(" ")
        if name in ("inactive_file", "total_inactive_file"):
            fields[name] = value

    # cgroup v1 reports both: `inactive_file` for this group alone and
    # `total_inactive_file` including its descendants, which is what its
    # `memory.usage_in_bytes` counts, so the totals are the matching pair.
    # cgroup v2 reports only `inactive_file`, and its `memory.current` already
    # includes descendants.
    try:
        return int(fields.get("total_inactive_file", fields.get("inactive_file")))
    except (TypeError, ValueError):
        return 0


def _cgroup_of_this_process(controller: str) -> str:
    """Path of this process within the cgroup hierarchy of `controller`.

    A missing file means the system has no cgroups at all. Any other failure is
    left to raise, so that an unreadable file is reported as unhealthy rather
    than mistaken for the root cgroup, which is usually unlimited.
    """
    try:
        lines = PROC_CGROUP.read_text().splitlines()
    except FileNotFoundError:
        return ""

    for line in lines:
        # Each line is `hierarchy:controllers:path`, with no controllers listed
        # for cgroup v2.
        _, _, rest = line.partition(":")
        controllers, _, path = rest.partition(":")
        if controller in controllers.split(","):
            return path
    return ""


def _cgroup_dirs(layout: CgroupLayout) -> list[pathlib.Path]:
    """This process' cgroup and its ancestors, closest one first."""
    parts = [part for part in _cgroup_of_this_process(layout.controller).split("/") if part]
    return [layout.root.joinpath(*parts[:depth]) for depth in range(len(parts), -1, -1)]


def _available_within(layout: CgroupLayout, directory: pathlib.Path) -> int | None:
    """Memory left within one cgroup's limit, or None if it has no limit."""
    limit = _read_int(directory / layout.limit)
    usage = _read_int(directory / layout.usage)
    if limit is None or usage is None or limit >= NO_CGROUP_LIMIT:
        return None

    # What the kernel considers used when deciding to kill the container.
    working_set = max(usage - _read_inactive_file(directory / layout.stat), 0)
    return max(limit - working_set, 0)


def _cgroup_available_memory() -> int | None:
    """Memory left within this process' cgroup limits, or None if not limited."""
    for layout in CGROUP_LAYOUTS:
        # A process is bound by its own cgroup and by every ancestor of it, so
        # the tightest of them is what it really has left. An ancestor's usage
        # counts its other children as well, and their memory is not available
        # to this process either.
        headroom = [
            available
            for directory in _cgroup_dirs(layout)
            if (available := _available_within(layout, directory)) is not None
        ]
        if headroom:
            return min(headroom)
    return None


def available_memory() -> int:
    """Memory, in bytes, this process can still use before it is killed."""
    host = psutil.virtual_memory().available
    cgroup = _cgroup_available_memory()
    return host if cgroup is None else min(cgroup, host)
