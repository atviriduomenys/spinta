import pytest

from spinta.backends import get_model_reserved_props
from spinta.components import Model, Context
from spinta.core.enums import Action, Level


@pytest.mark.parametrize(
    "action, include_page, result",
    [
        (Action.GETALL, True, ["_type", "_revision", "_page"]),
        (Action.GETALL, False, ["_type", "_revision"]),
        (Action.SEARCH, True, ["_type", "_revision", "_base", "_page"]),
        (Action.SEARCH, False, ["_type", "_revision", "_base"]),
        (Action.CHANGES, True, ["_cid", "_created", "_op", "_id", "_txn", "_revision", "_same_as"]),
        (Action.CHANGES, False, ["_cid", "_created", "_op", "_id", "_txn", "_revision", "_same_as"]),
        (Action.MOVE, True, ["_type", "_revision", "_id", "_same_as"]),
        (Action.MOVE, False, ["_type", "_revision", "_id", "_same_as"]),
        (Action.INSERT, True, ["_type", "_revision", "_page"]),
        (Action.INSERT, False, ["_type", "_revision"]),
        (Action.GETONE, True, ["_type", "_revision", "_page"]),
        (Action.GETONE, False, ["_type", "_revision"]),
    ],
)
def test_get_model_reserved_props_model_level_less_or_3(
    action: Action, include_page: bool, result: list[str], context: Context
):
    model = Model()
    model.level = Level.open

    assert get_model_reserved_props(model, action, include_page) == result


@pytest.mark.parametrize(
    "action, include_page, result",
    [
        (Action.GETALL, True, ["_type", "_id", "_revision", "_page"]),
        (Action.GETALL, False, ["_type", "_id", "_revision"]),
        (Action.SEARCH, True, ["_type", "_id", "_revision", "_base", "_page"]),
        (Action.SEARCH, False, ["_type", "_id", "_revision", "_base"]),
        (Action.CHANGES, True, ["_cid", "_created", "_op", "_id", "_txn", "_revision", "_same_as"]),
        (Action.CHANGES, False, ["_cid", "_created", "_op", "_id", "_txn", "_revision", "_same_as"]),
        (Action.MOVE, True, ["_type", "_revision", "_id", "_same_as"]),
        (Action.MOVE, False, ["_type", "_revision", "_id", "_same_as"]),
        (Action.INSERT, True, ["_type", "_id", "_revision", "_page"]),
        (Action.INSERT, False, ["_type", "_id", "_revision"]),
        (Action.GETONE, True, ["_type", "_id", "_revision", "_page"]),
        (Action.GETONE, False, ["_type", "_id", "_revision"]),
    ],
)
@pytest.mark.parametrize("model_level", [None, Level.identifiable])
def test_get_model_reserved_props_model_level_4_or_more(
    action: Action, include_page: bool, result: list[str], model_level: Level | None, context: Context
):
    model = Model()
    model.type = "model"
    model.level = model_level

    assert get_model_reserved_props(model, action, include_page) == result
