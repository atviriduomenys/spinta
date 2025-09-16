from typing import cast

import pytest

from spinta import commands
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.core.ufuncs import Bind
from spinta.core.ufuncs import Env
from spinta.core.ufuncs import Expr
from spinta.core.ufuncs import Pair
from spinta.core.ufuncs import UFuncRegistry
from spinta.exceptions import IncompatibleForeignProperties
from spinta.testing.manifest import load_manifest_and_context
from spinta.testing.manifest import load_manifest_get_context
from spinta.testing.ufuncs import UFuncTester
from spinta.types.datatype import Ref
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.changebase.helpers import change_base_model


@pytest.fixture()
def ufunc():
    return UFuncTester(
        resolver=UFuncRegistry(),
        executor=UFuncRegistry(),
    )


def test_resolve_value(ufunc):
    assert ufunc("2") == 2


def test_resolve_expr(ufunc):
    @ufunc.resolver(Env, int, int)
    def add(env, a, b):
        return a + b

    assert ufunc("2 + 2") == 4


def test_resolve_nested_expr(ufunc):
    @ufunc.resolver(Env, int, int)
    def add(env, a, b):
        return a + b

    assert ufunc("2 + 2 + 2 + 2") == 8


def test_resolve_args(ufunc):
    @ufunc.resolver(Env, Expr)
    def add(env, expr):
        args, kwargs = expr.resolve(env)
        return sum(args)

    assert ufunc("add(2, 2, 2, 2)") == 8


def test_resolve_kwargs(ufunc):
    @ufunc.resolver(Env)
    def kwargs(env, **kwargs):  # noqa
        return kwargs

    @ufunc.resolver(Env, str, object)
    def bind(env, name, value):
        return Pair(name, value)

    assert ufunc("kwargs(a: 2, b: 3)") == {"a": 2, "b": 3}


def test_execute_expr(ufunc):
    @ufunc.executor(Env, int, int)
    def add(env, a, b):  # noqa
        return a + b

    assert ufunc("2 + 2") == 4


def test_execute_this(ufunc):
    @ufunc.executor(Env, name="len")
    def len_(env):  # noqa
        return len(env.this)

    @ufunc.executor(Env, str, name="len")
    def len_(env, s):  # noqa
        return len(s)

    assert ufunc("len()", this="abc") == 3
    assert ufunc('len("abcd")', this="abc") == 4


def test_resolve_attrs(ufunc):
    @ufunc.resolver(Env, str)
    def bind(env, key):
        return Bind(key)

    @ufunc.resolver(Env, int, int)
    def add(env, a, b):
        return a + b

    @ufunc.resolver(Env, dict, Bind)
    def getattr(env, obj, bind):
        return obj[bind.name]

    @ufunc.resolver(Env, Bind, Bind)
    def getattr(env, item, bind):  # noqa
        return env.this[item.name][bind.name]

    data = {
        "a": {
            "b": {
                "c": 2,
                "d": 6,
            }
        }
    }
    assert ufunc("a.b.c + a.b.d", this=data) == 8


def test_execute_attrs(ufunc):
    @ufunc.resolver(Env, str)
    def bind(env, key):
        return Bind(key)

    @ufunc.executor(Env, int, int)
    def add(env, a, b):
        return a + b

    @ufunc.executor(Env, dict, Bind)
    def getattr(env, obj, bind):
        return obj[bind.name]

    @ufunc.executor(Env, Bind, Bind)
    def getattr(env, item, bind):  # noqa
        return env.this[item.name][bind.name]

    data = {
        "a": {
            "b": {
                "c": 2,
                "d": 6,
            }
        }
    }
    assert ufunc("a.b.c + a.b.d", this=data) == 8


def test_fpr_get_bind_expr(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | m | property  | type   | ref
    datasets/gov/example  |        |
      | resource          | sql    |
      |   | Planet        |        | name
      |   |   | name      | string |

      |   | Continent     |        | name
      |   |   | name      | string |
      |   |   | planet    | ref    | Planet

      |   | Country       |        | name
      |   |   | name      | string |
      |   |   | continent | ref    | Continent
                          |        |
      |   | City          |        | name
      |   |   | name      | string |
      |   |   | country   | ref    | Country
    """,
    )

    planet = commands.get_model(context, manifest, "datasets/gov/example/Planet")
    continent = commands.get_model(context, manifest, "datasets/gov/example/Continent")
    country = commands.get_model(context, manifest, "datasets/gov/example/Country")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr = ForeignProperty(
        None,
        cast(Ref, city.properties["country"].dtype),
        country.properties["name"].dtype,
    )
    assert str(fpr.get_bind_expr()) == "country.name"

    fpr = fpr.swap(country.properties["continent"])
    assert str(fpr.get_bind_expr()) == "country.continent"

    fpr = fpr.push(continent.properties["name"])
    assert str(fpr.get_bind_expr()) == "country.continent.name"

    fpr = fpr.swap(continent.properties["planet"])
    assert str(fpr.get_bind_expr()) == "country.continent.planet"

    fpr = fpr.push(planet.properties["name"])
    assert str(fpr.get_bind_expr()) == "country.continent.planet.name"


def test_fpr_join(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | m | property  | type   | ref
    datasets/gov/example  |        |
      | resource          | sql    |
      |   | Continent     |        | name
      |   |   | name      | string |

      |   | Country       |        | name
      |   |   | name      | string |
      |   |   | continent | ref    | Continent
                          |        |
      |   | City          |        | name
      |   |   | name      | string |
      |   |   | country   | ref    | Country
    """,
    )

    continent = commands.get_model(context, manifest, "datasets/gov/example/Continent")
    country = commands.get_model(context, manifest, "datasets/gov/example/Country")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr1 = ForeignProperty(
        None,
        cast(Ref, city.properties["country"].dtype),
        country.properties["continent"].dtype,
    )
    fpr2 = ForeignProperty(
        None,
        cast(Ref, country.properties["continent"].dtype),
        continent.properties["name"].dtype,
    )
    fpr = fpr1.join(fpr2)
    assert str(fpr) == ("ForeignProperty(datasets/gov/example/City, country -> continent -> name:string)")
    assert str(fpr.get_bind_expr()) == "country.continent.name"


def test_fpr_join_no_right(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | m | property  | type   | ref
    datasets/gov/example  |        |
      | resource          | sql    |
      |   | Continent     |        | name
      |   |   | name      | string |

      |   | Country       |        | name
      |   |   | name      | string |
      |   |   | continent | ref    | Continent
                          |        |
      |   | City          |        | name
      |   |   | name      | string |
      |   |   | country   | ref    | Country
    """,
    )

    continent = commands.get_model(context, manifest, "datasets/gov/example/Continent")
    country = commands.get_model(context, manifest, "datasets/gov/example/Country")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr1 = ForeignProperty(
        None,
        cast(Ref, city.properties["country"].dtype),
    )
    fpr2 = ForeignProperty(
        None,
        cast(Ref, country.properties["continent"].dtype),
        continent.properties["name"].dtype,
    )
    fpr = fpr1.join(fpr2)
    assert str(fpr) == ("ForeignProperty(datasets/gov/example/City, country -> continent -> name:string)")
    assert str(fpr.get_bind_expr()) == "country.continent.name"


def test_fpr_join_incompatible_refs(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | m | property  | type   | ref
    datasets/gov/example  |        |
      | resource          | sql    |
      |   | Planet        |        | name
      |   |   | name      | string |

      |   | Continent     |        | name
      |   |   | name      | string |
      |   |   | planet    | ref    | Planet

      |   | Country       |        | name
      |   |   | name      | string |
      |   |   | continent | ref    | Continent
                          |        |
      |   | City          |        | name
      |   |   | name      | string |
      |   |   | country   | ref    | Country
    """,
    )

    continent = commands.get_model(context, manifest, "datasets/gov/example/Continent")
    country = commands.get_model(context, manifest, "datasets/gov/example/Country")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr1 = ForeignProperty(
        None,
        cast(Ref, city.properties["country"].dtype),
        country.properties["name"].dtype,
    )
    fpr2 = ForeignProperty(
        None,
        cast(Ref, country.properties["continent"].dtype),
        continent.properties["name"].dtype,
    )
    with pytest.raises(IncompatibleForeignProperties):
        fpr1.join(fpr2)


def test_fpr_join_incompatible_refs_no_right(rc: RawConfig):
    context, manifest = load_manifest_and_context(
        rc,
        """
    d | r | m | property  | type   | ref
    datasets/gov/example  |        |
      | resource          | sql    |
      |   | Planet        |        | name
      |   |   | name      | string |

      |   | Continent     |        | name
      |   |   | name      | string |
      |   |   | planet    | ref    | Planet

      |   | Country       |        | name
      |   |   | name      | string |
      |   |   | continent | ref    | Continent
                          |        |
      |   | City          |        | name
      |   |   | name      | string |
      |   |   | country   | ref    | Country
    """,
    )

    continent = commands.get_model(context, manifest, "datasets/gov/example/Continent")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr1 = ForeignProperty(
        None,
        cast(Ref, city.properties["country"].dtype),
    )
    fpr2 = ForeignProperty(
        None,
        cast(Ref, continent.properties["planet"].dtype),
        continent.properties["name"].dtype,
    )
    with pytest.raises(IncompatibleForeignProperties):
        fpr1.join(fpr2)


def test_change_base_model(rc: RawConfig):
    context = load_manifest_get_context(
        rc,
        """
    d | r | m | property  | type   | ref       | prepare
    datasets/gov/example  |        |           |
      | resource          | sql    |           |
      |   | Continent     |        | name      |
      |   |   | name      | string |           |
                          |        |           |
      |   | Country       |        | name      | continent.name = 'Europe'
      |   |   | name      | string |           |
      |   |   | continent | ref    | Continent |
                          |        |           |
      |   | City          |        | name      |
      |   |   | name      | string |           |
      |   |   | country   | ref    | Country   |
    """,
    )

    store: Store = context.get("store")
    manifest = store.manifest

    country = commands.get_model(context, manifest, "datasets/gov/example/Country")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr = ForeignProperty(None, cast(Ref, city.properties["country"].dtype))
    assert str(change_base_model(context, country, fpr)) == ("country.continent.name='Europe'")


def test_change_base_model_non_ref(rc: RawConfig):
    context = load_manifest_get_context(
        rc,
        """
    d | r | m | property  | type   | ref       | prepare
    datasets/gov/example  |        |           |
      | resource          | sql    |           |
      |   | Continent     |        | name      | name = 'Europe'
      |   |   | name      | string |           |
                          |        |           |
      |   | Country       |        | name      |
      |   |   | name      | string |           |
      |   |   | continent | ref    | Continent |
                          |        |           |
      |   | City          |        | name      |
      |   |   | name      | string |           |
      |   |   | country   | ref    | Country   |
    """,
    )

    store: Store = context.get("store")
    manifest = store.manifest

    continent = commands.get_model(context, manifest, "datasets/gov/example/Continent")
    country = commands.get_model(context, manifest, "datasets/gov/example/Country")
    city = commands.get_model(context, manifest, "datasets/gov/example/City")

    fpr = ForeignProperty(None, cast(Ref, city.properties["country"].dtype))
    fpr = fpr.push(country.properties["continent"])
    assert str(change_base_model(context, continent, fpr)) == ("country.continent.name='Europe'")
