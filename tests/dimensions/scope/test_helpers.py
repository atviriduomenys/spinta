from typing import Any

import pytest

from spinta.components import Context, Model
from spinta.core.access import Access
from spinta.dimensions.scope.components import Scope
from spinta.dimensions.scope.helpers import link_scopes, load_scopes
from spinta.exceptions import FieldNotInResource, PropertyNotFound
from spinta.manifests.components import Manifest
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

    def test_prepare_stored_as_ast_dict(
        self,
        context: Context,
        model: Model,
        scope_data: dict[str, dict[str, Any]],
    ) -> None:
        result = load_scopes(context, [model], scope_data)

        assert isinstance(result["ltu"].prepare, dict)
        assert result["ltu"].prepare["name"] == "eq"

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


class TestLinkScopes:
    @pytest.fixture
    def model_with_props(self) -> Model:
        model = Model()
        model.name = "example/City"
        model.eid = "test/example/City"
        props = {"id": None, "code": None, "name": None}
        model.properties = props
        model.flatprops = props
        return model

    @pytest.fixture
    def manifest(self) -> Manifest:
        return Manifest()

    def _build_scope(self, model: Model, name: str, prepare) -> Scope:
        scope = Scope()
        scope.name = name
        scope.model = model
        scope.prepare = prepare
        return scope

    def test_valid_local_property_passes(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "valid_scope",
            parse("code = 'lt'"),
        )
        link_scopes(context, model_with_props, {"valid_scope": scope})

    def test_unknown_local_property_raises(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "bad_scope",
            parse("country = 'lt'"),
        )
        with pytest.raises((PropertyNotFound, FieldNotInResource)):
            link_scopes(context, model_with_props, {"bad_scope": scope})

    def test_unknown_property_inside_select_raises(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "bad_scope",
            parse("select(country)"),
        )
        with pytest.raises((PropertyNotFound, FieldNotInResource)):
            link_scopes(context, model_with_props, {"bad_scope": scope})

    def test_getattr_head_validated(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "bad_scope",
            parse("country.name = 'lt'"),
        )
        with pytest.raises((PropertyNotFound, FieldNotInResource)):
            link_scopes(context, model_with_props, {"bad_scope": scope})

    def test_first_invalid_scope_raises(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        good_scope = self._build_scope(
            model_with_props,
            "good_scope",
            parse("code = 'lt'"),
        )
        bad_scope = self._build_scope(
            model_with_props,
            "bad_scope",
            parse("country = 'lt'"),
        )
        with pytest.raises((PropertyNotFound, FieldNotInResource)):
            link_scopes(
                context,
                model_with_props,
                {"good_scope": good_scope, "bad_scope": bad_scope},
            )
