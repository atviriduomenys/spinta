from spinta import spyna
from spinta.core.ufuncs import UFuncRegistry, Env, asttoexpr


class UFuncTester:

    def __init__(self, Env=Env, context=None):
        self.Env = Env
        self.context = context
        self.resolver = UFuncRegistry()
        self.executor = UFuncRegistry()

    def __call__(self, expr, **scope):
        env = self.Env(
            self.context,
            self.resolver.ufuncs(),
            self.executor.ufuncs(),
        )
        env.update(**scope)
        ast = spyna.parse(expr)
        expr = asttoexpr(ast)
        expr = env.resolve(expr)
        return env.execute(expr)
