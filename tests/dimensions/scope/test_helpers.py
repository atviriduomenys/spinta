from typing import Any

import pytest

from spinta.components import Context, Model
from spinta.core.access import Access
from spinta.core.ufuncs import Expr, Bind
from spinta.dimensions.scope.components import Scope
from spinta.dimensions.scope.helpers import load_scopes, load_prepare, link_scopes
from spinta.exceptions import PropertyNotFound
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


class TestLoadPrepare:
    def test_sets_prepare_as_expr(
        self,
        context: Context,
        model: Model,
    ) -> None:
        scope = Scope()
        scope.name = "ltu"
        scope.model = model

        load_prepare(context, scope, parse("code = 'lt'"))

        assert isinstance(scope.prepare, Expr)

    def test_top_level_op_name_matches_formula(
        self,
        context: Context,
        model: Model,
    ) -> None:
        scope = Scope()
        scope.name = "ltu"
        scope.model = model

        load_prepare(context, scope, parse("code = 'lt'"))

        assert scope.prepare.name == "eq"

    def test_nested_getattr_preserved(
        self,
        context: Context,
        model: Model,
    ) -> None:
        scope = Scope()
        scope.name = "ltu"
        scope.model = model

        load_prepare(context, scope, parse("country.code = 'lt'"))

        assert scope.prepare.name == "eq"
        assert isinstance(scope.prepare.args[0], Expr)
        assert scope.prepare.args[0].name == "getattr"

    def test_bind_arg_inside_expr(
        self,
        context: Context,
        model: Model,
    ) -> None:
        scope = Scope()
        scope.name = "ltu"
        scope.model = model

        load_prepare(context, scope, parse("select(code)"))

        assert scope.prepare.name == "select"
        inner = scope.prepare.args[0]
        assert isinstance(inner, Expr)
        assert inner.name == "bind"
        assert inner.args[0] == "code"


class TestLinkScopes:
    @pytest.fixture
    def model_with_props(self) -> Model:
        model = Model()
        model.name = "example/City"
        model.eid = "test/example/City"
        model.properties = {"id": None, "code": None, "name": None}
        return model

    @pytest.fixture
    def manifest(self) -> Manifest:
        return Manifest()

    def _build_scope(self, model: Model, name: str, prepare: Expr) -> Scope:
        scope = Scope()
        scope.name = name
        scope.model = model
        scope.prepare = prepare
        return scope

    def test_unknown_property_inside_select_raises(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = Scope()
        scope.name = "bad_scope"
        scope.model = model_with_props
        load_prepare(context, scope, parse("select(country)"))

        with pytest.raises(PropertyNotFound):
            link_scopes(context, manifest, {"bad_scope": scope})

    def test_valid_bind_passes(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "valid_scope",
            Expr("eq", Bind("code"), "lt"),
        )
        # Should not raise.
        link_scopes(context, manifest, {"valid_scope": scope})

    def test_unknown_property_raises(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "bad_scope",
            Expr("eq", Bind("country"), "lt"),
        )
        with pytest.raises(PropertyNotFound):
            link_scopes(context, manifest, {"bad_scope": scope})

    def test_nested_expr_is_walked(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "bad_scope",
            Expr("select", Bind("country")),
        )
        with pytest.raises(PropertyNotFound):
            link_scopes(context, manifest, {"bad_scope": scope})

    def test_getattr_head_validated(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "bad_scope",
            Expr("getattr", Bind("country"), Bind("name")),
        )
        with pytest.raises(PropertyNotFound):
            link_scopes(context, manifest, {"bad_scope": scope})

    def test_literal_args_ignored(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        scope = self._build_scope(
            model_with_props,
            "good_scope",
            Expr("eq", Bind("code"), "lt"),
        )
        link_scopes(context, manifest, {"good_scope": scope})

    def test_first_invalid_scope_raises(
        self,
        context: Context,
        manifest: Manifest,
        model_with_props: Model,
    ) -> None:
        good_scope = self._build_scope(
            model_with_props,
            "good_scope",
            Expr("eq", Bind("code"), "lt"),
        )
        bad_scope = self._build_scope(
            model_with_props,
            "bad_scope",
            Expr("eq", Bind("country"), "lt"),
        )
        with pytest.raises(PropertyNotFound):
            link_scopes(context, manifest, {"good_scope": good_scope, "bad_scope": bad_scope})
