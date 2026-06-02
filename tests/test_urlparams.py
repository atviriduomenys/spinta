import pytest

from spinta import spyna
from spinta.components import Context
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import UrlParams
from spinta.components import Version
from spinta.commands import prepare
from spinta.dimensions.scope.components import Scope
from spinta.exceptions import InvalidValue, PropertiesNotFound, PropertyNotFound
from spinta.testing.request import make_get_request
from spinta.urlparams import _apply_custom_scope, _prepare_urlparams_from_path


def _parse(
    context: Context,
    query: str,
    accept="application/json",
) -> UrlParams:
    request = make_get_request("", query, {"Accept": accept})
    return prepare(context, UrlParams(), Version(), request)


def test_format(context):
    assert _parse(context, "format(csv,width(42))").format == "csv"
    assert _parse(context, "format(width(42))").formatparams == {
        "width": 42,
    }
    assert _parse(context, "format(csv,width(42))").formatparams == {
        "width": 42,
    }


def test_limit(context):
    assert _parse(context, "limit(1)").limit == 1
    with pytest.raises(InvalidValue):
        _parse(context, "limit(0)")
    with pytest.raises(InvalidValue):
        _parse(context, "limit(-1)")


@pytest.mark.parametrize(
    "url_query",
    [
        "format(csv,title(%27%3c%3fxml+version%3d%221.0%22+encoding%3d%22UTF-8%22+standalone%3d%22yes%22%3f%3e%27))",
        "format(csv,title(%27%3c%3fxml version%3d%221.0%22 encoding%3d%22UTF-8%22 standalone%3d%22yes%22%3f%3e%27))",
        (
            "format(csv,title(%27%3c%3fxml%20version%3d%221.0%22%20encoding%3d%22UTF-8%22%20"
            "standalone%3d%22yes%22%3f%3e%27))"
        ),
    ],
)
def test_encoded_plus_for(context: Context, url_query: str):
    params_space = _parse(context, url_query)
    assert params_space.formatparams["title"] == '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'


def test_prepare_urlparams_sets_custom_scope():
    params = UrlParams()
    params.parsetree = [
        {"name": "path", "args": ["example", "City"]},
        {"name": "custom-scope", "args": ["ltu"]},
    ]
    _prepare_urlparams_from_path(params)
    assert params.custom_scope == "ltu"


def test_prepare_urlparams_raises_if_custom_scope_set_twice():
    params = UrlParams()
    params.parsetree = [
        {"name": "path", "args": ["example", "City"]},
        {"name": "custom-scope", "args": ["ltu"]},
        {"name": "custom-scope", "args": ["est"]},
    ]
    with pytest.raises(InvalidValue):
        _prepare_urlparams_from_path(params)


def test_prepare_urlparams_raises_if_custom_scope_has_no_args():
    params = UrlParams()
    params.parsetree = [
        {"name": "path", "args": ["example", "City"]},
        {"name": "custom-scope", "args": []},
    ]
    with pytest.raises(InvalidValue):
        _prepare_urlparams_from_path(params)


def _make_model(*scope_names_and_prepares: tuple[str, str]) -> Model:
    model = Model()
    model.name = "example/City"
    model.scopes = {}
    for name, prepare_expr in scope_names_and_prepares:
        scope = Scope()
        scope.name = name
        scope.prepare = spyna.parse(prepare_expr)
        model.scopes[name] = scope
    return model


def _params(
    model: Model,
    scope: str | None = None,
    query: list | None = None,
    select: list | None = None,
    sort: list | None = None,
) -> UrlParams:
    params = UrlParams()
    params.model = model
    params.custom_scope = scope
    params.query = [spyna.parse(q) for q in query] if query else None
    params.select = [spyna.parse(s) for s in select] if select else None
    params.sort = [spyna.parse(s) for s in sort] if sort else None
    return params


class TestApplyCustomScopeRowFilter:
    def test_scope_filter_applied_when_no_user_query(self):
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope="ltu")

        _apply_custom_scope(params)

        assert params.query is not None
        assert len(params.query) == 1
        assert spyna.unparse(params.query[0]) == "country_code='lt'"

    def test_scope_filter_appended_to_user_query(self):
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope="ltu", query=["name='Vilnius'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "name='Vilnius'" in unparsed
        assert "country_code='lt'" in unparsed
        assert len(unparsed) == 2

    def test_scope_and_conditions_both_appended(self):
        model = _make_model(("ltu", "country_code='lt'&status='active'"))
        params = _params(model, scope="ltu")

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "country_code='lt'" in unparsed
        assert "status='active'" in unparsed
        assert len(unparsed) == 2

    def test_scope_and_conditions_merged_with_user_query(self):
        model = _make_model(("ltu", "country_code='lt'&status='active'"))
        params = _params(model, scope="ltu", query=["name='Vilnius'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "country_code='lt'" in unparsed
        assert "status='active'" in unparsed
        assert "name='Vilnius'" in unparsed
        assert len(unparsed) == 3

    def test_no_scope_leaves_query_unchanged(self):
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope=None, query=["name='Vilnius'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert unparsed == ["name='Vilnius'"]

    def test_unknown_scope_name_leaves_query_unchanged(self):
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope="nonexistent", query=["name='Vilnius'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert unparsed == ["name='Vilnius'"]

    def test_conflicting_user_filter_both_appended(self):
        # User sends country_code='de' but scope requires country_code='lt'.
        # Both must appear in query — the scope filter is never dropped, so the
        # conjunction produces zero rows rather than letting the user bypass the scope.
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope="ltu", query=["country_code='de'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "country_code='de'" in unparsed
        assert "country_code='lt'" in unparsed
        assert len(unparsed) == 2

    def test_scope_or_filter_appended_as_single_condition(self):
        model = _make_model(("baltic", "or(country_code='lt',country_code='lv')"))
        params = _params(model, scope="baltic")

        _apply_custom_scope(params)

        assert params.query is not None
        assert len(params.query) == 1
        assert spyna.unparse(params.query[0]) == "or(country_code='lt', country_code='lv')"

    def test_scope_or_filter_appended_to_user_query(self):
        model = _make_model(("baltic", "or(country_code='lt',country_code='lv')"))
        params = _params(model, scope="baltic", query=["status='active'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "status='active'" in unparsed
        assert "or(country_code='lt', country_code='lv')" in unparsed
        assert len(unparsed) == 2


class TestApplyCustomScopeSelect:
    def test_scope_select_applied_when_no_user_select(self):
        model = _make_model(("pub", "select(name)"))
        params = _params(model, scope="pub")

        _apply_custom_scope(params)

        assert params.select is not None
        assert [spyna.unparse(s) for s in params.select] == ["name"]

    def test_scope_select_intersected_with_user_select(self):
        model = _make_model(("pub", "select(name,country_code)"))
        params = _params(model, scope="pub", select=["name", "country_code", "status"])

        _apply_custom_scope(params)

        result = {spyna.unparse(s) for s in params.select}
        assert result == {"name", "country_code"}

    def test_user_select_subset_of_scope_select_kept(self):
        model = _make_model(("pub", "select(name,country_code,status)"))
        params = _params(model, scope="pub", select=["name"])

        _apply_custom_scope(params)

        assert [spyna.unparse(s) for s in params.select] == ["name"]

    def test_no_overlap_raises_property_not_found(self):
        model = _make_model(("pub", "select(name)"))
        params = _params(model, scope="pub", select=["country_code"])

        with pytest.raises(PropertyNotFound):
            _apply_custom_scope(params)

    def test_scope_select_and_filter_combined(self):
        model = _make_model(("pub", "select(name)&country_code='lt'"))
        params = _params(model, scope="pub")

        _apply_custom_scope(params)

        assert [spyna.unparse(s) for s in params.select] == ["name"]
        assert params.query is not None
        assert spyna.unparse(params.query[0]) == "country_code='lt'"

    def test_scope_select_filter_and_user_select_filter_combined(self):
        # Scope restricts to select(name) and rows where country_code='lt'.
        # User requests select(name) and filters name='Vilnius'.
        # Expected: select=[name], query=[country_code='lt', name='Vilnius'].
        model = _make_model(("pub", "select(name)&country_code='lt'"))
        params = _params(model, scope="pub", select=["name"], query=["name='Vilnius'"])

        _apply_custom_scope(params)

        assert [spyna.unparse(s) for s in params.select] == ["name"]
        unparsed = [spyna.unparse(q) for q in params.query]
        assert "country_code='lt'" in unparsed
        assert "name='Vilnius'" in unparsed
        assert len(unparsed) == 2

    def test_scope_select_narrows_user_select_and_filter_combined(self):
        # Scope restricts to select(name, country_code) and rows where status='active'.
        # User requests select(name) and filters country_code='lt'.
        # Expected: select narrows to [name], query=[status='active', country_code='lt'].
        model = _make_model(("pub", "select(name,country_code)&status='active'"))
        params = _params(model, scope="pub", select=["name"], query=["country_code='lt'"])

        _apply_custom_scope(params)

        assert [spyna.unparse(s) for s in params.select] == ["name"]
        unparsed = [spyna.unparse(q) for q in params.query]
        assert "status='active'" in unparsed
        assert "country_code='lt'" in unparsed
        assert len(unparsed) == 2


class TestApplyCustomScopeFieldAccess:
    def test_filter_on_hidden_field_raises_property_not_found(self):
        model = _make_model(("pub", "select(name)"))
        params = _params(model, scope="pub", query=["country_code='lt'"])

        with pytest.raises(PropertiesNotFound):
            _apply_custom_scope(params)

    def test_filter_on_allowed_field_passes(self):
        model = _make_model(("pub", "select(name)"))
        params = _params(model, scope="pub", query=["name='Vilnius'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "name='Vilnius'" in unparsed

    def test_sort_on_hidden_field_raises_property_not_found(self):
        model = _make_model(("pub", "select(name)"))
        params = _params(model, scope="pub", sort=["country_code"])

        with pytest.raises(PropertiesNotFound):
            _apply_custom_scope(params)

    def test_sort_on_allowed_field_passes(self):
        model = _make_model(("pub", "select(name)"))
        params = _params(model, scope="pub", sort=["name"])

        _apply_custom_scope(params)

        assert params.sort is not None

    def test_scope_with_row_filter_and_select_blocks_filter_on_hidden_field(self):
        # Scope has both a row-filter and a select; user filter references a hidden field.
        model = _make_model(("pub", "select(name)&country_code='lt'"))
        params = _params(model, scope="pub", query=["status='active'"])

        with pytest.raises(PropertiesNotFound):
            _apply_custom_scope(params)

    def test_scope_with_row_filter_and_select_allows_filter_on_visible_field(self):
        # Scope has both a row-filter and a select; user filter references an allowed field.
        model = _make_model(("pub", "select(name)&country_code='lt'"))
        params = _params(model, scope="pub", query=["name='Vilnius'"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "country_code='lt'" in unparsed
        assert "name='Vilnius'" in unparsed
        assert len(unparsed) == 2

    def test_user_filter_on_scope_row_filter_field_raises_even_with_matching_value(self):
        # Scope uses country_code='lt' as its own row-filter but hides country_code via
        # select(name). The user is not allowed to reference country_code at all — even
        # supplying the exact same value must raise PropertyNotFound, because the field
        # is not in the visible set.
        model = _make_model(("pub", "select(name)&country_code='lt'"))
        params = _params(model, scope="pub", query=["country_code='lt'"])

        with pytest.raises(PropertiesNotFound):
            _apply_custom_scope(params)

    def test_scope_row_filter_only_allows_any_field_in_user_query(self):
        # When a scope has only a row-filter and no select(), there is no field
        # restriction — the user may filter or sort on any field.
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope="ltu", query=["status='active'"], sort=["population"])

        _apply_custom_scope(params)

        unparsed = [spyna.unparse(q) for q in params.query]
        assert "status='active'" in unparsed
        assert "country_code='lt'" in unparsed
        assert params.sort is not None

    def test_scope_row_filter_only_allows_any_field_in_user_select(self):
        # No select in scope → user select is not restricted.
        model = _make_model(("ltu", "country_code='lt'"))
        params = _params(model, scope="ltu", select=["name", "status", "population"])

        _apply_custom_scope(params)

        result = [spyna.unparse(s) for s in params.select]
        assert result == ["name", "status", "population"]


class TestApplyCustomScopeWithNamespace:
    def test_namespace_model_is_ignored(self):
        ns = Namespace()
        params = UrlParams()
        params.model = ns
        params.custom_scope = "ltu"
        params.query = None

        _apply_custom_scope(params)

        assert params.query is None
