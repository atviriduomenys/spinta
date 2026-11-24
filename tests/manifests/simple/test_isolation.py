"""`spinta.lib` reads no configuration and writes nothing (3.6, D15).

Each check runs in a new interpreter, in a directory that holds a trap `.env`,
with `SPINTA_*` variables and `HOME` and `XDG_*` pointing to an empty folder.
The first import of `spinta.lib` is part of the run.
"""

import json
import os
import subprocess
import sys
import textwrap

import pytest

from tests.manifests.simple.test_check import CITIES
from tests.manifests.simple.test_read import CSV_TABLE

SCRIPT = textwrap.dedent(
    """
    import json, logging, re, sys, warnings
    before = (list(logging.getLogger().handlers), warnings.showwarning)

    from spinta.lib import Spinta

    spinta = Spinta()
    doc = spinta.copy(sys.stdin.read().encode("utf-8"), "csv")
    again = spinta.copy(doc, "csv")
    errors = spinta.errors(doc)
    checked = spinta.check(doc)
    downgraded = spinta.check(doc, strict_references=False)

    after = (list(logging.getLogger().handlers), warnings.showwarning)
    print(json.dumps({
        "doc": doc,
        # The `created` date of an implied comment is the time of the load, so it is masked.
        "csv": re.sub(r"\d{4}-\d{2}-\d{2}T[0-9:.+]+", "<time>", again.decode("utf-8")),
        "errors": errors,
        "checked": checked,
        "downgraded": downgraded,
        "handlers_unchanged": before[0] == after[0],
        "warnings_unchanged": before[1] is after[1],
    }))
    """
)

TRAP_ENV = {
    "SPINTA_CHECK_NAMES": "false",
    "SPINTA_UNRESOLVED_REFERENCES": "error",
    "SPINTA_SETUP_LOGGING": "true",
    "SPINTA_CONFIG": "trap.yml",
}


def _run(cwd, home, extra_env=None, text=CITIES):
    # The interpreter's own environment (the Nix shell's library paths) is kept,
    # but no `SPINTA_*` variable of the caller is, and the home folders are new.
    env = {k: v for k, v in os.environ.items() if not k.startswith("SPINTA_")}
    env.update(
        {
            "HOME": str(home),
            "XDG_CONFIG_HOME": str(home),
            "XDG_DATA_HOME": str(home),
            "XDG_CACHE_HOME": str(home),
            "XDG_STATE_HOME": str(home),
            **(extra_env or {}),
        }
    )
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        input=text,
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def _tree(path):
    return sorted(str(p.relative_to(path)) for p in path.rglob("*"))


@pytest.fixture
def trap(tmp_path):
    work = tmp_path / "work"
    home = tmp_path / "home"
    work.mkdir()
    home.mkdir()
    (work / ".env").write_text("\n".join(f"{k}={v}" for k, v in TRAP_ENV.items()) + "\n", encoding="utf-8")
    (work / "spinta.yml").write_text("config: trap\n", encoding="utf-8")
    return work, home


def test_results_are_the_same_with_and_without_the_trap(tmp_path, trap):
    work, home = trap
    clean_work = tmp_path / "clean"
    clean_work.mkdir()
    clean_home = tmp_path / "clean_home"
    clean_home.mkdir()

    with_trap = _run(work, home, TRAP_ENV)
    without = _run(clean_work, clean_home)
    assert with_trap == without


def test_no_file_or_directory_is_created(trap):
    work, home = trap
    before_work, before_home = _tree(work), _tree(home)
    _run(work, home, TRAP_ENV)
    assert _tree(work) == before_work
    assert _tree(home) == before_home


def test_no_root_log_handler_and_no_captured_warnings(trap):
    work, home = trap
    result = _run(work, home, TRAP_ENV)
    assert result["handlers_unchanged"] is True
    assert result["warnings_unchanged"] is True


def test_trap_settings_do_not_change_the_document(trap):
    work, home = trap
    result = _run(work, home, TRAP_ENV, text=CSV_TABLE)
    assert result["errors"] == []
    assert next(iter(result["doc"]["manifest"].values()))["columns"][0] == "id"


def test_check_does_not_read_the_trap_settings(tmp_path, trap):
    # The trap sets `unresolved_references = error`. `check` takes its mode from
    # its argument only, so the downgraded result is still a warning.
    work, home = trap
    result = _run(work, home, TRAP_ENV)
    manifest = next(iter(result["checked"]["manifest"].values()))
    assert [e["severity"] for e in manifest["errors"]] == []
    model = result["checked"]["property"]["datasets/gov/cities/City/location"]
    assert [e["severity"] for e in model["errors"]] == ["error"]
    downgraded = result["downgraded"]["property"]["datasets/gov/cities/City/location"]
    assert [e["severity"] for e in downgraded["errors"]] == ["warning"]
