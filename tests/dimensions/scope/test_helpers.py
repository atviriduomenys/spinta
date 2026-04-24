from typing import Any

import pytest

from spinta.components import Context, Model
from spinta.core.access import Access
from spinta.dimensions.scope.components import Scope
from spinta.dimensions.scope.helpers import load_scopes
from spinta.spyna import parse


@pytest.fixture
def model() -> Model:
    model = Model()
    model.name = "example/City"
    model.access = Access.public
    return model


@pytest.fixture
def scope_data() -> dict[str, dict[str, Any]]:
    return {
        "ltu": {
            "name": "ltu",
            "prepare": parse("country.code = 'lt'"),
            "access": "private",
            "title": "",
            "description": "",
            "eli": "",
        },
    }


class TestLoadScopes:
    @pytest.mark.parametrize("scopes", [None, {}])
    def test_returns_empty_dict_when_no_scopes(
        self,
        context: Context,
        model: Model,
        scopes: dict | None,
    ) -> None:
        assert load_scopes(context, [model], scopes) == {}

    def test_loads_scope_as_instance(
        self,
        context: Context,
        model: Model,
        scope_data: dict[str, dict[str, Any]],
    ) -> None:
        result = load_scopes(context, [model], scope_data)

        assert isinstance(result["ltu"], Scope)
        assert result["ltu"].name == "ltu"

    def test_links_scope_to_model(
        self,
        context: Context,
        model: Model,
        scope_data: dict[str, dict[str, Any]],
    ) -> None:
        result = load_scopes(context, [model], scope_data)

        assert result["ltu"].model is model

    def test_preserves_given_access(
        self,
        context: Context,
        model: Model,
        scope_data: dict[str, dict[str, Any]],
    ) -> None:
        result = load_scopes(context, [model], scope_data)

        assert result["ltu"].given.access == "private"

    def test_loads_multiple_scopes(
        self,
        context: Context,
        model: Model,
    ) -> None:
        data = {
            "ltu": {
                "name": "ltu",
                "prepare": parse("country.code = 'lt'"),
                "access": "private",
                "title": "",
                "description": "",
                "eli": "",
            },
            "eu": {
                "name": "eu",
                "prepare": parse("country.region = 'EU'"),
                "access": "protected",
                "title": "",
                "description": "",
                "eli": "",
            },
        }

        result = load_scopes(context, [model], data)

        assert set(result.keys()) == {"ltu", "eu"}
        assert result["ltu"].given.access == "private"
        assert result["eu"].given.access == "protected"
