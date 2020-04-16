from typing import Any, Iterator, Dict

import sqlalchemy as sa

from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.backends.postgresql.components import PostgreSQL


@commands.load.register(Context, BackendManifest, PostgreSQL)
def load(
    context: Context,
    manifest: BackendManifest,
    backend: PostgreSQL,
) -> Iterator[Dict[str, Any]]:
    conn = context.get(f'transaction.{backend.name}')
    model = manifest.objects['model']['_schema']
    table = backend.get_table(model)
    qry = sa.select([table])
    for row in conn.execute(qry):
        yield dict(row)
