import uuid

import pytest

from spinta.components import Context, Model
from spinta.core.enums import load_level, Level
from spinta.exceptions import InvalidLevel


@pytest.mark.parametrize(
    "given_value, result",
    [
        (Level.absent, Level.absent),
        (Level.available, Level.available),
        (Level.structured, Level.structured),
        (Level.open, Level.open),
        (Level.identifiable, Level.identifiable),
        (Level.linked, Level.linked),
        ("", None),
        ("0", Level.absent),
        ("1", Level.available),
        ("2", Level.structured),
        ("3", Level.open),
        ("4", Level.identifiable),
        ("5", Level.linked),
        ("6", None),
        (-1, None),
        (0, Level.absent),
        (1, Level.available),
        (2, Level.structured),
        (3, Level.open),
        (4, Level.identifiable),
        (5, Level.linked),
        (6, None),
        (None, None),
    ],
)
def test_load_level_success(context: Context, given_value: Level | int | str | None, result: Level):
    model = Model()
    model.eid = str(uuid.uuid4())

    load_level(context, model, given_value)
    assert model.level == result


@pytest.mark.parametrize("given_value", ["foo", "-1"])
def test_load_level_error(context: Context, given_value: Level | int | str | None):
    model = Model()
    model.eid = str(uuid.uuid4())

    with pytest.raises(InvalidLevel):
        load_level(context, model, given_value)
