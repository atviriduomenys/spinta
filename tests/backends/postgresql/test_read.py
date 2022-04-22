from typing import Any
from typing import Dict
from typing import List
from unittest.mock import Mock

from spinta import commands
from spinta import spyna
from spinta.components import Context
from spinta.auth import AdminToken
from spinta.testing.manifest import prepare_manifest
from spinta.core.config import RawConfig
from spinta.core.ufuncs import asttoexpr


def _prep_context(context: Context):
    context.set('auth.token', AdminToken())


def _prep_conn(context: Context, data: List[Dict[str, Any]]):
    txn = Mock()
    conn = txn.connection.execution_options.return_value = Mock()
    conn.execute.return_value = data
    context.set('transaction', txn)
    return conn


def test_getall(rc: RawConfig):
    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property   | type    | ref     | access
    example                    |         |         |
      |   |   | Country        |         |         |
      |   |   |   | name       | string  |         | open
      |   |   | City           |         |         |
      |   |   |   | name       | string  |         | open
      |   |   |   | country    | ref     | Country | open
    ''')
    _prep_context(context)
    conn = _prep_conn(context, [
        {
            '_id': '3aed7394-18da-4c17-ac29-d501d5dd0ed7',
            '_revision': '9f308d61-5401-4bc2-a9da-bc9de85ad91d',
            'country.name': 'Lithuania',
        }
    ])
    model = manifest.models['example/City']
    backend = model.backend
    query = asttoexpr(spyna.parse('select(_id, country.name)'))
    rows = commands.getall(context, model, backend, query=query)
    rows = list(rows)

    assert str(conn.execute.call_args.args[0]) == (
        'SELECT'
        ' "example/City"._id,'
        ' "example/City"._revision,'
        ' "example/Country_1".name AS "country.name" \n'
        'FROM'
        ' "example/City" '
        'LEFT OUTER JOIN "example/Country" AS "example/Country_1"'
        ' ON "example/City"."country._id" = "example/Country_1"._id'
    )

    assert rows == [
        {
            '_id': '3aed7394-18da-4c17-ac29-d501d5dd0ed7',
            '_revision': '9f308d61-5401-4bc2-a9da-bc9de85ad91d',
            '_type': 'example/City',
            'country': {'name': 'Lithuania'},
        },
    ]
