import pathlib

import pytest

from spinta.utils.memory import CgroupLayout, available_memory

MB = 1024 * 1024

V2 = CgroupLayout(pathlib.Path(), "", "memory.max", "memory.current", "memory.stat")
V1 = CgroupLayout(pathlib.Path(), "memory", "memory.limit_in_bytes", "memory.usage_in_bytes", "memory.stat")


def _write_cgroup(path: pathlib.Path, limit: str, usage: str, layout: CgroupLayout = V2, **stat: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / layout.limit).write_text(limit)
    (path / layout.usage).write_text(usage)
    fields = "".join(f"{name} {value}\n" for name, value in stat.items())
    (path / layout.stat).write_text(f"anon 1000\n{fields}slab 20\n")


def _fake_cgroup(
    mocker,
    tmp_path: pathlib.Path,
    cgroup: str = "/",
    layout: CgroupLayout = V2,
) -> pathlib.Path:
    """Point the memory check at a fake cgroup tree, entered from `cgroup`."""
    root = tmp_path / "cgroup"
    root.mkdir(exist_ok=True)

    proc = tmp_path / "proc-self-cgroup"
    proc.write_text(f"9:{layout.controller}:{cgroup}\n" if layout.controller else f"0::{cgroup}\n")
    mocker.patch("spinta.utils.memory.PROC_CGROUP", proc)
    mocker.patch("spinta.utils.memory.CGROUP_LAYOUTS", (layout._replace(root=root),))
    return root


def _host_memory(mocker, available: int) -> None:
    mocker.patch("psutil.virtual_memory").return_value.available = available


def test_available_memory_of_a_container(tmp_path: pathlib.Path, mocker):
    # A container limited to 512 MB has to be measured against that limit, not
    # against the memory of the host, which is all `/proc/meminfo` knows about.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path)
    _write_cgroup(root, limit=str(512 * MB), usage=str(200 * MB))

    assert available_memory() == 312 * MB


def test_available_memory_of_a_nested_cgroup(tmp_path: pathlib.Path, mocker):
    # A systemd service runs in a nested cgroup while the mount root stays
    # unlimited, so looking only at the root would miss its `MemoryMax`.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path, cgroup="/system.slice/spinta.service")
    _write_cgroup(root, limit="max", usage=str(8000 * MB))
    _write_cgroup(root / "system.slice" / "spinta.service", limit=str(512 * MB), usage=str(200 * MB))

    assert available_memory() == 312 * MB


def test_available_memory_inherits_an_ancestor_limit(tmp_path: pathlib.Path, mocker):
    # The limit can sit on a parent slice rather than on the process' own cgroup.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path, cgroup="/system.slice/spinta.service")
    _write_cgroup(root / "system.slice", limit=str(512 * MB), usage=str(200 * MB))

    assert available_memory() == 312 * MB


def test_available_memory_uses_the_tightest_limit(tmp_path: pathlib.Path, mocker):
    # Every ancestor constrains the process, not only the closest cgroup, and an
    # ancestor's usage counts its other children too. Here the parent has less
    # left than the child's own limit allows.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path, cgroup="/system.slice/spinta.service")
    _write_cgroup(root / "system.slice", limit=str(512 * MB), usage=str(400 * MB))
    _write_cgroup(root / "system.slice" / "spinta.service", limit=str(1024 * MB), usage=str(100 * MB))

    assert available_memory() == 112 * MB


def test_available_memory_ignores_reclaimable_cache(tmp_path: pathlib.Path, mocker):
    # Page cache is charged to the group but reclaimed instead of being killed
    # for, so counting it as used would report a busy container as unhealthy.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path)
    _write_cgroup(root, limit=str(512 * MB), usage=str(500 * MB), inactive_file=str(400 * MB))

    assert available_memory() == 412 * MB


def test_available_memory_of_a_cgroup_v1_container(tmp_path: pathlib.Path, mocker):
    # cgroup v1 counts descendants in `memory.usage_in_bytes`, so the matching
    # reclaimable cache is `total_inactive_file`. Taking `inactive_file`, which
    # covers this group alone, would report 112 MB here instead of 412 MB.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path, cgroup="/system.slice/spinta.service", layout=V1)
    _write_cgroup(
        root / "system.slice" / "spinta.service",
        limit=str(512 * MB),
        usage=str(500 * MB),
        layout=V1,
        inactive_file=str(100 * MB),
        total_inactive_file=str(400 * MB),
    )

    assert available_memory() == 412 * MB


def test_available_memory_is_bound_by_the_host(tmp_path: pathlib.Path, mocker):
    # A cgroup limit is a ceiling, not a reservation: a container with gigabytes
    # of headroom is still killed when the machine itself runs out of memory.
    _host_memory(mocker, 100 * MB)
    root = _fake_cgroup(mocker, tmp_path)
    _write_cgroup(root, limit=str(8000 * MB), usage=str(200 * MB))

    assert available_memory() == 100 * MB


def test_available_memory_with_a_zero_limit(tmp_path: pathlib.Path, mocker):
    # A cgroup allowed no memory at all is unusable, not unlimited, so it must
    # not fall back to the memory of the host.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path)
    _write_cgroup(root, limit="0", usage=str(100 * MB))

    assert available_memory() == 0


@pytest.mark.parametrize("limit", ["max", str(2**63 - 1)])
def test_available_memory_falls_back_to_the_host(tmp_path: pathlib.Path, limit: str, mocker):
    _host_memory(mocker, 700 * MB)
    root = _fake_cgroup(mocker, tmp_path)
    _write_cgroup(root, limit=limit, usage=str(200 * MB))

    assert available_memory() == 700 * MB


def test_available_memory_raises_when_a_cgroup_cannot_be_read(tmp_path: pathlib.Path, mocker):
    # A cgroup that cannot be read is not an unlimited one. Falling back to the
    # memory of the host would hide that the real headroom is unknown.
    _host_memory(mocker, 8000 * MB)
    root = _fake_cgroup(mocker, tmp_path)
    _write_cgroup(root, limit=str(512 * MB), usage=str(200 * MB))
    mocker.patch("pathlib.Path.read_text", side_effect=PermissionError("denied"))

    with pytest.raises(PermissionError):
        available_memory()

    assert root.exists()
