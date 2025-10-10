from pathlib import Path

import pytest

from spinta.datasets.backends.helpers import is_file_path


@pytest.mark.parametrize(
    "path",
    [
        "",
        ".",
        "foo",
        "/foo/",
        "<foo><bar></bar></foo>",
        "{}",
        "a" * 1000,
    ],
)
def test_is_file_path_false(path: str):
    assert is_file_path(path) is False


def tst_is_file_path_true(tmp_path: Path):
    assert is_file_path(str(tmp_path / "manifest.csv")) is True
