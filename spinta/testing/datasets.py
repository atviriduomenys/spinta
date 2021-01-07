import contextlib
import itertools
import json
import os
import tempfile
from typing import Dict
from typing import List

import sqlalchemy as sa

from spinta.cli.pull import pull as pull_

Schema = Dict[str, List[sa.Column]]


def pull(cli, rc, dataset, model=None, *, push=True):
    cmd = [
        [dataset],
        ['--push'] if push else [],
        ['--model', model] if model else [],
        ['-e', 'stdout:jsonl'],
    ]
    cmd = list(itertools.chain(*cmd))
    result = cli.invoke(rc, pull_, cmd)
    data = []
    for line in result.stdout.splitlines():
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            print(line)
        else:
            data.append(d)
    return data


class Sqlite:

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.engine = sa.create_engine(dsn)
        self.schema = sa.MetaData(self.engine)
        self.tables = {}

    def init(self, tables: Schema):
        self.tables = {
            k: sa.Table(k, self.schema, *v)
            for k, v in tables.items()
        }
        self.schema.create_all()

    def write(self, table, data):
        table = self.tables[table]
        with self.engine.begin() as conn:
            conn.execute(table.insert(), data)


@contextlib.contextmanager
def create_sqlite_db(tables: Schema):
    with tempfile.TemporaryDirectory() as tmpdir:
        db = Sqlite('sqlite:///' + os.path.join(tmpdir, 'db.sqlite'))
        db.init(tables)
        yield db
