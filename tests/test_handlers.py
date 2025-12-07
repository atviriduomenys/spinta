import pytest

from spinta.exceptions import InvalidValue
from spinta.handlers import CLIErrorHandler, ErrorManager


def test_handle_error_increments_count():
    handler = CLIErrorHandler()
    handler.handle_error(InvalidValue("bad value"), file="data.csv")

    counts = handler.get_counts()
    assert counts["data.csv"] == 1


def test_handle_error_multiple_files():
    handler = CLIErrorHandler()
    handler.handle_error(InvalidValue("bad"), file="data1.csv")
    handler.handle_error(InvalidValue("bad"), file="data1.csv")
    handler.handle_error(InvalidValue("bad"), file="data2.csv")

    counts = handler.get_counts()
    assert counts["data1.csv"] == 2
    assert counts["data2.csv"] == 1


def test_post_process_summary(capsys):
    handler = CLIErrorHandler()
    handler.handle_error(InvalidValue("bad"), file="data.csv")
    handler.post_process()
    captured = capsys.readouterr()
    output = captured.out

    assert "Total errors: 1" in output
    assert "- data.csv: 1 error(s)" in output


def test_manager_delegates_to_handler():
    handler = CLIErrorHandler()
    manager = ErrorManager(handler)
    manager.current_file = "data.csv"

    manager.handle_error(InvalidValue("oops"))

    counts = handler.get_counts()
    assert counts["data.csv"] == 1


def test_manager_reraises_when_enabled():
    handler = CLIErrorHandler()
    manager = ErrorManager(handler, re_raise=True)

    with pytest.raises(InvalidValue):
        manager.handle_error(InvalidValue("oops"))
