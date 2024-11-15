from typing import Any

from spinta.components import Model, Config, PageInfo
from spinta.core.ufuncs import Env, Expr
from spinta.exceptions import FieldNotInResource
from spinta.ufuncs.loadbuilder.helpers import page_contains_unsupported_keys


class LoadBuilder(Env):
    model: Model

    def resolve(self, expr: Any):
        if not isinstance(expr, Expr):
            # Expression is already resolved, return resolved value.
            return expr

        if expr.name in self._resolvers:
            ufunc = self._resolvers[expr.name]

        else:
            args, kwargs = expr.resolve(self)
            return self.default_resolver(expr, *args, **kwargs)

        if ufunc.autoargs:
            # Resolve arguments automatically.
            args, kwargs = expr.resolve(self)
            try:
                return ufunc(self, *args, **kwargs)
            except NotImplementedError:
                return self.default_resolver(expr, *args, **kwargs)

        else:
            # Resolve arguments manually.
            try:
                return ufunc(self, expr)
            except NotImplementedError:
                pass

    def load_page(self):
        config: Config = self.context.get('config')
        page = PageInfo(
            self.model,
            enabled=config.enable_pagination
        )
        page_given = False

        if self.model.external and self.model.external.prepare:
            resolved = self.resolve(self.model.external.prepare)
            if not isinstance(resolved, list):
                resolved = [resolved]
            for item in resolved:
                if isinstance(item, PageInfo):
                    page = item
                    page_given = True
                    break
        if not page_given:
            args = ['_id']
            if self.model.given.pkeys:
                if isinstance(self.model.given.pkeys, list):
                    args = self.model.given.pkeys
                else:
                    args = [self.model.given.pkeys]
                if '_id' in args:
                    args.remove('_id')
            for arg in args:
                key = arg
                if arg in self.model.properties:
                    prop = self.model.properties[arg]
                    page.keys.update({
                        key: prop
                    })
                else:
                    raise FieldNotInResource(self.model, property=arg)

        # Disable page if given properties are not possible to access
        if page_contains_unsupported_keys(page):
            page.enabled = False

        # Set default page size if nothing was given
        if page.size is None:
            page.size = config.default_page_size

        self.model.page = page


