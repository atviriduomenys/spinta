from spinta.components import Context
from spinta.core.ufuncs import asttoexpr
from spinta.datasets.components import Param
from spinta.dimensions.param.helpers import load_param_formulas
from spinta.spyna import parse


class TestLoadParamFormulas:
    def test_add_expr_to_formula_if_it_cannot_be_resolved_by_load_builder(self, context: Context) -> None:
        param = Param()
        param.sources = ["test_source"]

        ast = parse("read()")
        load_param_formulas(context, param, [ast])

        assert param.formulas == [asttoexpr(ast)]

    def test_add_multiple_formulas_if_it_cannot_be_resolved_by_load_builder(self, context: Context) -> None:
        param = Param()
        param.sources = ["test_source1", "test_source2"]

        ast = parse("read()")
        load_param_formulas(context, param, [ast, ast])

        assert param.formulas == [asttoexpr(ast), asttoexpr(ast)]

    def test_do_not_add_formula_if_it_is_resolved_by_load_builder(self, context: Context) -> None:
        param = Param()
        param.sources = ["test_source"]

        ast = parse("input()")
        load_param_formulas(context, param, [ast])

        assert param.formulas == []
