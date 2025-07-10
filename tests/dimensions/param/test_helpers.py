from spinta.components import Context
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Param
from spinta.dimensions.param.helpers import load_param_formulas_and_sources
from spinta.spyna import parse


class TestLoadParamFormulas:
    def test_add_to_formula_and_source_if_formula_cannot_be_resolved_by_load_builder(self, context: Context) -> None:
        param = Param()

        ast = parse("read()")
        load_param_formulas_and_sources(context, param, [ast], ["test_source"])

        assert param.formulas == [asttoexpr(ast)]
        assert param.sources == ["test_source"]

    def test_add_multiple_formula_and_source_if_formula_cannot_be_resolved_by_load_builder(
        self, context: Context
    ) -> None:
        param = Param()

        ast = parse("read()")
        load_param_formulas_and_sources(context, param, [ast, ast], ["test_source1", "test_source2"])

        assert param.formulas == [asttoexpr(ast), asttoexpr(ast)]
        assert param.sources == ["test_source1", "test_source2"]

    def test_do_not_add_formula_and_source_if_formula_is_resolved_by_load_builder(self, context: Context) -> None:
        param = Param()

        ast = parse("input()")
        load_param_formulas_and_sources(context, param, [ast], ["test_source"])

        assert param.formulas == []
        assert param.sources == []
