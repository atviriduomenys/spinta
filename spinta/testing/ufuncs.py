from spinta import spyna
from spinta.core.ufuncs import Env, asttoexpr, ufunc


class UFuncTester:

    def __init__(
        self,
        Env=Env,
        context=None,
        resolver=ufunc.resolver,
        executor=ufunc.executor,
        scope=None,
    ):
        self.Env = Env
        self.context = context
        self.resolver = resolver
        self.executor = executor
        self.scope = scope or {}

    def __call__(self, expr, **scope):
        env = self.Env(
            self.context,
            self.resolver.ufuncs(),
            self.executor.ufuncs(),
            self.scope,
        )
        env.update(**scope)
        ast = spyna.parse(expr)
        expr = asttoexpr(ast)
        expr = env.resolve(expr)
        return env.execute(expr)
