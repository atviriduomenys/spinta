from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import Optional
from typing import Tuple

import sqlparse
from sqlparse.sql import Identifier
from sqlparse.sql import Statement
from sqlparse.sql import Token
from sqlparse.sql import TokenList
from sqlparse.tokens import DDL
from sqlparse.tokens import Keyword

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.sqldump.components import SqlDump
from spinta.datasets.components import Resource
from spinta.manifests.helpers import load_manifest_nodes
from spinta.utils.naming import to_model_name


class ReadTokens:

    def __init__(self, token_list: TokenList, idx=0):
        self.token_list = token_list
        self.idx = idx

    def find(self, **kwargs) -> Optional[Token]:
        idx, token = self.token_list.token_next_by(idx=self.idx, **kwargs)
        if idx is not None:
            self.idx = idx
            return token


def _read_sql_statement(
    resource: Optional[Resource],
    statement: Statement,
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    tokens = ReadTokens(statement)
    if tokens.find(m=(DDL, 'CREATE')) and tokens.find(m=(Keyword, 'TABLE')):
        identifier: Optional[Identifier] = tokens.find(i=Identifier)
        if identifier is None:
            return
        real_name = identifier.get_real_name()
        name = to_model_name(real_name)
        if resource:
            name = f'{resource.dataset.name}/{name}'
        yield 0, {
            'type': 'model',
            'name': name,
            'external': {
                'dataset': resource.dataset.name if resource else None,
                'resource': resource.name if resource else None,
                'name': real_name,
                'pk': [],
            },
            'properties': {},
        }


def _read_sql_ast(
    resource: Resource,
    ast: Iterable[Statement],
) -> Iterator[Tuple[int, Dict[str, Any]]]:
    for statement in ast:
        yield from _read_sql_statement(resource, statement)


@commands.inspect.register(Context, Resource, SqlDump)
def inspect(context: Context, resource: Resource, backend: SqlDump):
    ast = sqlparse.parsestream(backend.stream)
    schemas = _read_sql_ast(resource, ast)
    load_manifest_nodes(
        context,
        resource.dataset.manifest,
        schemas,
        link=True,
    )
