.. default-role:: literal

#########
Manifests
#########

Manifest is a source, where metadata are read from. Spinta by default reads
metada from tabular manifests, for example from CSV or XLSX files, but there
can be other metadat sources, for example metadata could be read from SQL
databases, from RDFS or OWL ontologies, etc.

Spinta works with following metadata:


.. image:: /static/manifest-model.png
   :target: /_static/manifest-model.png


Adding new manifest type
************************

New manifests are added under `spinta/manifests` directory.

`components.py` file should contain your manifest class component, for example:

.. code-block:: python

    class MyManifest(Manifest):
        type: str = 'my'

        @staticmethod
        def detect_from_path(path: str) -> bool:
            return path.endswith('.my')


Manifests must implement following commands:

.. code-block:: python

   # commands/configure.py

    from spinta import commands
    from spinta.components import Context
    from spinta.core.config import RawConfig
    from spinta.manifests.my.components import MyManifest


    @commands.configure.register(Context, MyManifest)
    def configure(context: Context, manifest: MyManifest):
        rc: RawConfig = context.get('rc')
        manifest.path = rc.get('manifests', manifest.name, 'path')


.. code-block:: python

   # commands/load.py

    from spinta import commands
    from spinta.components import Context
    from spinta.manifests.components import Manifest
    from spinta.manifests.helpers import load_manifest_nodes
    from spinta.manifests.my.components import MyManifest
    from spinta.manifests.my.helpers import read_my_manifest


    @commands.load.register(Context, MyManifest)
    def load(
        context: Context,
        manifest: MyManifest,
        *,
        into: Manifest = None,
        freezed: bool = True,
        rename_duplicates: bool = False,
        load_internal: bool = True,
    ):
        if load_internal:
            target = into or manifest
            if '_schema' not in target.models:
                store = context.get('store')
                commands.load(context, store.internal, into=target)

        if manifest.path is None and file is None:
            return

        schemas = read_my_manifest(manifest.path, rename_duplicates=rename_duplicates)

        if into:
            load_manifest_nodes(context, into, schemas, source=manifest)
        else:
            load_manifest_nodes(context, manifest, schemas)

        for source in manifest.sync:
            commands.load(
                context, source,
                into=into or manifest,
                freezed=freezed,
                rename_duplicates=rename_duplicates,
                load_internal=load_internal,
            )

Here, you need to implement `read_my_manifest` function, which is responsible,
for producing iterator with following items:

.. code-block:: python

   (location, schema)

Here, `location` should be a line number, database primary key or other
identifyer, where this manifest item can be located.

`schema` is a dict, that must contain at least `type` key. `type` must be
defined in `spinta.comfig:CONFIG['components']['nodes']`, which at the time of
writing this text, looks like this:

.. code-block:: python

    {
        'ns': 'spinta.components:Namespace',
        'dataset': 'spinta.datasets.components:Dataset',
        'base': 'spinta.components:Base',
        'model': 'spinta.components:Model',
    }

Components can be added via configuration, for example, using environment variables:

.. code-block:: sh

    SPINTA_COMPONENTS__NODES__MY=myproject.components:My

See :ref:`configuration` for more information.

Each component, has `schema` attrubute, where you can find, what can be used in
`schema` for each `type`.

Here is an example for `type: model`:

.. code-block:: python

    from spinta.utils.schema import NA

    {
        'type': 'model',
        'name': 'City',
        'title': 'City',
        'description': '',
        'external': {
            'dataset': 'example/dataset',
            'resource': 'myres',
            'pk': ['id'],
            'name': 'my://cities',
            'prepare': NA,
        },
        'properties': {
            'id': {
                'type': 'integer',
                'type_args': None,
                'required': True,
                'unique': True,
                'external': {
                    'name': 'ID',
                    'prepare': NA,
                }
            },
        },
    }

A good place too look for examples, whould be
`spinta/manifests/tabular/helpers.py`_, look for `*Reader` classes.

.. _spinta/manifests/tabular/helpers.py: https://github.com/atviriduomenys/spinta/blob/master/spinta/manifests/tabular/helpers.py

Once you produce schemas as described above, then `load_manifest_nodes`
function, will take care of everything else.
