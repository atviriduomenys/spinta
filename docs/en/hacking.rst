.. default-role:: literal

Hacking
#######

Here you will find information about Spinta internals. This information might
be useful if you want to understand how things works under the hood or you
want to contribute code to the project.

Keep in mind, that mostly this part of documentation is used and a notes
taking place during development. So things might be inconsistent and outdated.


Composite primary keys
======================

If we have table like this::

    d | r | m | property | source | prepare         | type   | ref     | access
    datasets/ds          |        |                 |        |         |
      | rs               |        |                 | sql    |         |
      |   | t1           | t1     |                 |        | a1      | open
      |   |   | p1       | p1     | self.split()[0] | string |         |
      |   |   | p2       | p2     |                 | string |         |
      |   |   | a1       |        | p1, p2          | ref    | t2      |
      |   | t2           | t2     |                 |        | p3, p4  | open
      |   |   | p3       | p3     |                 | string |         |
      |   |   | p4       | p4     |                 | string |         |

Here we have two tables, `t1` and `t2`. Primary key of `t1` table is `a1`.
`a1` points to a property that does not have `source`, but have `prepare`.
`prepare` of `a1` points to `p1` and `p2` properties. So the question is, how
to we interpret this in code?

If we would query this model using `/datasets/ds/t1` query, then following
code will be executed:

.. code-block:: python

    @commands.getall.register(Context, Model, Sql)
    def getall(context: Context, model: Model, backend: Sql):
        builder = SqlQueryBuilder(context)
        builder.update(model=model)
        for params in iterparams(model):
            table = model.external.name.format(**params)
            table = backend.get_table(model, table)

            env = builder.init(backend, table)
            expr = env.resolve(query)
            where = env.execute(expr)
            qry = env.build(where)

            for row in conn.execute(qry):
                res = {
                    '_type': model.model_type(),
                }
                for key, sel in env.select.items():
                    val = _get_row_value(row, sel.items[0])
                    res[key] = val
                res = commands.cast_backend_to_python(context, model, backend, res)
                yield res

This is a very simplified version of the code, just to illustrate what wil
happen. And this that happen are:

- Initialize a query builder, which is used as interpreters context environment.
  Specifically SqlQueryBuilder is responsible for building sqlalchemy query and
  while it reads query passed to URL resulting query is stored in
  SqlQueryBuilder class context.

- Then built sqlalchemy query is executed.

- And then each row does another round of processing.

So for our example, following sqlalchemy query should be constructed:

.. code-block:: python

    t1 = schema.tables['t1']
    qry = sa.select([
        t1.c.p1,
        t1.c.p2,
    ])

Also `env.select` will look as fallows:

.. code-block:: python

    env.columns = [
        t1.c.p1,
        t1.c.p2,
        sa.func.count(),
    ]
    env.resolved = {
        '_id': Expr('prop', model.properties['a1'].dtype),
        'p1': Expr(
            'cast',
            Expr(
                'getitem',
                Expr(
                    'split',
                    Expr('item', t1.c.p1),
                ),
                0,
            ),
            Bind('int'),
        ),
        'p2': Expr('item', t1.c.p2),
        'a1': [
            Expr('prop', model.properties['p1'].dtype),
            Expr('prop', model.properties['p2'].dtype),
        ],
    }
    env.selected = {
        '_id': Expr('prop', model.properties['_id'].dtype),
        'p1': Expr('prop', model.properties['p1'].dtype),
        'p2': Expr('prop', model.properties['p2'].dtype),
        'a1': Expr('prop', model.properties['a1'].dtype),
        'count()': Expr('item', sa.func.count()),
    }

Then the final result will look like this:

.. code-block:: python

    [
        {
            '_id': [1, 2],
            'p1': 1,
            'p2': 2,
            'a1': [1, 2],
        }
    ]


Original data::

    \
     p1   | p2
    -------|-----
     "1 a" | 2


How do we resolve all this?

.. code-block:: python

    _id
    bind('_id')
    prop('_id')
        a1
        bind('a1')
        prop('a1')
            p1, p2
                bind('p1')
                prop('p1')
                    self.split()[0]
                        bind('self')
                        prop('p1')

        env.resolved['_id'] = prop('_id', prop('a1'))
            prop('a1')
                env.resolved['a1'] = [prop('p1'), prop('p2')]
                    prop('p1')
                        env(this=prop('p1'))
                            self.split()[0]
                                bind('self')
                                prop('p1')
                            getitem(split(prop('p1')), 0)
                        env.resolved['p1'] = prop().split()[0]

.. code-block:: python

    # C01
    # self.selected = None
    def build(self, where):
        if self.selected is None:
            # If select list was not explicitly given, select all properties.
            env.call('select', Expr('select')) # -> C02

    # C02 <- C01
    # expr = Expr('select')
    def select(env, expr: Expr)
        args, kwargs = expr.resolve(env)
        if len(args) == 0:
            # Go through all model properties and call select again.
            for prop in take(['_id', all], env.model.properties).values():
                if authorized(env.context, prop, Action.GETALL):
                    env.selected[prop.place] = env.call('select', prop) # -> C03
                    # env.selected['_id'] = Expr('prop', Property('a1', dtype=Ref))
                    # env.resolved['a1'] = result = (
                    #     Expr('prop', Property('p1', dtype=String)),
                    #     Expr('prop', Property('p2', dtype=String)),
                    # )

    # C03 <- C02
    # prop = Property('_id', dtype=PrimaryKey)
    def select(env, prop: Property):
        if prop.place not in env.resolved:
            if prop.external.prepare is None:
                result = env.call('select', prop.dtype) # -> C04
                # result = Expr('prop', Property('a1', dtype=Ref))
            env.resolved[prop.place] = result
        return Expr('prop', prop.dtype)

    # C04 <- C03
    # dtype = Property('_id', dtype=PrimaryKey)
    # env.model.external.pkeys = [Property('a1', dtype=Ref)]
    def select(env, dtype: PrimaryKey):
        pkeys = env.model.external.pkeys
        if len(pkeys) == 1:
            prop = pkeys[0]
            return env.call('select', prop) # -> C05
            # return Expr('prop', Property('a1', dtype=Ref))

    # C05 <- C04
    # prop = Property('a1', dtype=Ref)
    def select(env, prop: Property):
        if prop.place not in env.resolved:
            if prop.external.prepare is not None:
                result = env.call('select', prop.dtype, prop.external .prepare)
                # -> C06
                # result = (
                #     Expr('prop', Property('p1', dtype=String)),
                #     Expr('prop', Property('p2', dtype=String)),
                # )
            env.resolved[prop.place] = result
        return Expr('prop', prop)
        # return Expr('prop', Property('a1', dtype=Ref))

    # C06 <- C05
    # prop = Property('a1', dtype=Ref)
    # prep = p1, p2
    #      ~ testlist(bind('p1'), bind('p2'))
    #      ~ Expr('testlist', Expr('bind', 'p1'), Expr('bind', 'p2'))
    def select(env, prop: Property, prep: Expr):
        result = env.resolve(prep)          # -> C07
        # result = Bind('p1'), Bind('p2')
        return env.call('select', result)   # -> C10
        # return (
        #     Expr('prop', Property('p1', dtype=String)),
        #     Expr('prop', Property('p2', dtype=String)),
        # )

    # C07 <- C06
    # expr = p1, p2
    #      ~ testlist(bind('p1'), bind('p2'))
    #      ~ Expr('testlist', Expr('bind', 'p1'), Expr('bind', 'p2'))
    def testlist(env, expr: Expr) -> tuple:
        args, kwargs = expr.resolve(env)    # -> C08, C09
        return tuple(args)
        # return Bind('p1'), Bind('p2')

    # C08 <- C07
    # name = 'p1'
    def bind(env, name: str) -> Bind:
        return Bind(name)
        # return Bind('p1')

    # C09 <- C07
    # name = 'p2'
    def bind(env, name: str) -> Bind:
        return Bind(name)
        # return Bind('p2')

    # C10 <- C06
    # value = Bind('p1'), Bind('p2')
    def select(env, item: tuple) -> tuple:
        return tuple(env.call('select', i) for i in item)  # -> C11
        # return (
        #     Expr('prop', Property('p1', dtype=String)),
        #     Expr('prop', Property('p2', dtype=String)),
        # )

    # C11 <- C06
    # item = Bind('p1')
    def select(env, item: Bind):
        prop = env.model.flatprops.get(item.name)
        # prop = Property('p1', dtype=String)
        return env.call('select', prop)     # -> C12
        # return Expr('prop', Property('p1', dtype=String))

    # C12 <- C11
    # prop = Property('p1', dtype=String)
    # prop.external.prepare = self.split()[0]
    # env.resolved['p1] = na
    def select(env, prop: Property):
        if prop.place not in env.resolved:
            if prop.external.prepare is not None:
                result = env.call('select', prop, prop.external.prepare)
                # -> C13
                # result = Expr('getitem', Expr('split', Property('p1', dtype=String)), 0)
            env.resolved[prop.place] = result
        return Expr('prop', prop)
        # return Expr('prop', Property('p1', dtype=String))

    # C13 <- C12
    # prop = Property('p1', dtype=String)
    # prep = self.split()[0]
    #      ~ bind('self').split().getitem(0)
    #      ~ getitem(
    #            split(
    #                bind('self'),
    #            ),
    #            0,
    #        )
    def select(env, prop: Property, prep: Expr):
        return env(this=prop).resolve(expr)     # -> C14
        result = env.resolve(expr)
        # result = Expr('getitem', Expr('split', Bind('self')), 0)
        return env(this=prop).call('select', result)    # -> C14
        # return Expr('getitem', Expr('split', Property('p1', dtype=String)), 0)

    # C14 <- C13
    # item = 'self'
    def bind(env, item: str):
        if item == 'self' and 'this' in env:
            return env.this
            # return Property('p1', dtype=String)


.. code-block:: python

    # C01
    # self.selected = None
    def build(self, where):
        if self.selected is None:
            env.resolve(Expr('select'))  # -> C02

    # C02 <- C01
    # expr = Expr('select')
    def select(env, expr: Expr)
        args, kwargs = expr.resolve(env)
        if len(args) == 0:
            for prop in take(['_id', all], env.model.properties).values():
                if authorized(env.context, prop, Action.GETALL):
                    env.selected[prop.place] = env.resolve('prop', prop)
                    # -> C03
                    # env.selected['_id'] = Expr('prop', Property('a1', dtype=Ref))
                    # env.resolved['a1'] = result = (
                    #     Expr('prop', Property('p1', dtype=String)),
                    #     Expr('prop', Property('p2', dtype=String)),
                    # )

    # C03 <- C02
    # prop = Property('_id', dtype=PrimaryKey)
    def prop(env, prop_: Property):
        if prop_.place not in env.resolved:
            if prop_.external.prepare is None:
                result = env.call('prop', prop_.dtype) # -> C04
                # result = Expr('prop', Property('a1', dtype=Ref))
            env.resolved[prop_.place] = result
        return Expr('prop', prop_.dtype, env.resolved[prop_.place])

    # C04 <- C03
    # dtype = Property('_id', dtype=PrimaryKey)
    # env.model.external.pkeys = [Property('a1', dtype=Ref)]
    def prop(env, dtype: PrimaryKey):
        pkeys = env.model.external.pkeys
        if len(pkeys) == 1:
            prop_ = pkeys[0]
            return env.call('prop', prop_) # -> C05
            # return Expr('prop', Property('a1', dtype=Ref))

    # C05 <- C04
    # prop = Property('a1', dtype=Ref)
    def prop(env, prop_: Property):
        if prop_.place not in env.resolved:
            if prop_.external.prepare is not None:
                result = env(this=prop_).resolve(prop_.external.prepare)
            env.resolved[prop_.place] = result
        return Expr('prop', prop_, env.resolved[prop_.place])
        # return Expr('prop', Property('a1', dtype=Ref))

    # C07 <- C06
    # expr = p1, p2
    #      ~ testlist(bind('p1'), bind('p2'))
    def testlist(env, expr: Expr) -> tuple:
        args, kwargs = expr.resolve(env)    # -> C08, C09
        return tuple(args)
        # return Bind('p1'), Bind('p2')

    # C08 <- C07
    # name = 'p1'
    def bind(env, name: str) -> Bind:
        if name in env.model.properties:
            return env.call('prop', env.model.properties[name])
        # return ?

    # prop_ = Property('p1', dtype=String)
    # prop_.external.prepare = self.split()[0]
    #                        ~ bind('self').split().getitem(0)
    #                        ~ getitem(
    #                              split(
    #                                  bind('self'),
    #                              ),
    #                              0,
    #                          )
    def prop(env, prop_: Property):
        if prop_.place not in env.resolved:
            if prop_.external.prepare is not None:
                this = Expr('prop', prop_.dtype)
                result = env(this=this).resolve(prop_.external.prepare)
            env.resolved[prop_.place] = result
        return env.resolved[prop_.place]
        # return getitem(
        #     split(
        #         Field(Property('p1', dtype=String), sa.Column('p1')),
        #         0,
        #     )
        # )

    # name = 'self'
    # env.this = prop(Property('p1', dtype=String).dtype)
    def bind(env, name: str):
        if name == 'self' and 'this' in env:
            if isinstance(env.this, Expr):
                return env.resolve(env.this)
                # return Field(Property('p1', dtype=String), sa.Column('p1'))

    # dtype = Property('p1', dtype=String).dtype
    def prop(env, dtype: DataType):
        table = env.backend.get_table(env.model)
        column = env.backend.get_column(table, dtype.prop, select=True)
        return Field(dtype.prop, column)
        # return Field(Property('p1', dtype=String), sa.Column('p1'))


    def getall(context: Context, model: Model, backend: Sql):
        builder = SqlQueryBuilder(context)
        builder = builder.init(backend, table)
        expr = builder.resolve(query)
        where = builder.execute(expr)
        qry = builder.build(where)

        for row in conn.execute(qry):
            # row = {
            #     '_id': (
            #     ),
            # }
            preparer = SqlResultsPreparer(context).init(builder, row)
            res = {
                '_type': model.model_type(),
            }
            for key, item in query.selected.items():
                res[key] = preparer.resolve(Expr('prepare', item))


    def prepare(env: SqlResultsPreparer, item: Field):
        pass



.. code-block:: python

    select(1) =
        env.resolved = {
            '1': Selected(item=None, prop=None, prep=1),
        }
        env.selected = {
            '1': env.resolved['1'],
        }
        env.received = [
        ]


    select(count(*)) =
        env.resolved = {
            'count(*)': Selected(item=0, prop=None, prep=None),
        }
        env.selected = {
            'count(*)': env.resolved['count(*)'],
        }
        env.received = [
            func.count(),
        ]


    select(count(p1)) =
        env.resolved = {
            'count(p1)': Selected(item=0, prop=None, prep=None),
        }
        env.selected = {
            'count(p1)': env.resolved['count(p1)'],
        }
        env.received = [
            func.count(t.c.p1),
        ]


    select(_id) =
        env.resolved = {
            '_id': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'a1': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'p1': Selected(item=0, prop=p1, prep=None),
            'p2': Selected(item=1, prop=p2, prep=None),
        }
        env.selected = {
            '_id': env.resolved['_id'],
        }
        env.received = [
            t.c.p1,
            t.c.p2,
        ]


    select(_id, a1, p1) =
        env.resolved = {
            '_id': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'a1': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'p1': Selected(item=0, prop=p1, prep=None),
            'p2': Selected(item=1, prop=p2, prep=None),
        }
        env.selected = {
            '_id': env.resolved['_id'],
            'a1': env.resolved['a1'],
            'p1': env.resolved['p1'],
        }
        env.received = [
            t.c.p1,
            t.c.p2,
        ]


    select(x: _id) =
        env.resolved = {
            '_id': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'a1': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'p1': Selected(item=0, prop=p1, prep=None),
            'p2': Selected(item=1, prop=p2, prep=None),
        }
        env.selected = {
            'x': env.resolved['_id'],
        }
        env.received = [
            t.c.p1,
            t.c.p2,
        ]


    p1 = self.split()[1]
    select(_id) =
        env.resolved = {
            '_id': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'a1': (
                Selected(item=0, prop=p1, prep=None),
                Selected(item=1, prop=p2, prep=None),
            ),
            'p1': Selected(item=0, prop=p1, prep=self.split()[1]),
            'p2': Selected(item=1, prop=p2, prep=None),
        }
        env.selected = {
            '_id': env.resolved['_id'],
        }
        env.received = [
            t.c.p1,
            t.c.p2,
        ]


    p1 = self.split()[1]
    select(p1.upper()) =
        env.resolved = {
            'p1': Selected(item=0, prop=p1, prep=self.split()[1].upper()),
        }
        env.selected = {
            'p1': env.resolved['p1'],
        }
        env.received = [
            t.c.p1,
        ]


    select(x: {y: [p1]}) =
        env.resolved = {
            'p1': Selected(item=0, prop=p1, prep=None),
        }
        env.selected = {
            'x': {'y': [env.resolved['p1']]},
        }
        env.received = [
            t.c.p1,
        ]


    p2 = p1.len()
    select(p1)&p2=3 =
        env.resolved = {
            'p1': Selected(item=0, prop=p1, prep=None),
        }
        env.selected = {
            'p1': env.resolved['p1'],
        }
        env.received = [
            t.c.p1,
        ]


.. code-block:: python

    select(1) =
        env.selected = {
            '1': Selected(item=0, prop=None, prep=None),
        }
        env.columns = [
            1,
        ]
        return {
            '1': env.columns[env.selected['1'].item]
        }

    select(_id) =
        env.selected = {
            '_id': Selected(
                prop=_id,
                prep=(
                    Selected(item=0, prop=p1, prep=None),
                    Selected(item=1, prop=p2, prep=None),
                )
            ),
        }
        env.columns = [
            1,
        ]
        return {
            '1': (
                env.columns[env.selected['_id'][0].item],
                env.columns[env.selected['_id'][1].item],
            )
        }

