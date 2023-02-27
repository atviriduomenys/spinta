import datetime
import hashlib
import textwrap
import uuid

import itertools
import json
import logging
import pathlib

import pprintpp
import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import TypedDict
from typing import NamedTuple

import msgpack
import requests
import sqlalchemy as sa
import tqdm
from requests import HTTPError
from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import Exit
from typer import echo

from spinta import exceptions
from spinta import spyna
from spinta.cli.helpers.auth import require_auth
from spinta.cli.helpers.data import ModelRow
from spinta.cli.helpers.data import count_rows
from spinta.cli.helpers.data import ensure_data_dir
from spinta.cli.helpers.data import iter_model_rows
from spinta.cli.helpers.store import prepare_manifest
from spinta.client import get_access_token
from spinta.client import get_client_credentials
from spinta.components import Action
from spinta.components import Config
from spinta.components import Context
from spinta.components import Mode
from spinta.components import Model
from spinta.components import Store
from spinta.core.context import configure_context
from spinta.manifests.components import Manifest
from spinta.types.namespace import sort_models_by_refs
from spinta.utils.data import take
from spinta.utils.json import fix_data_for_json
from spinta.utils.nestedstruct import flatten
from spinta.utils.units import tobytes
from spinta.utils.units import toseconds

log = logging.getLogger(__name__)


def push(
    ctx: TyperContext,
    manifests: Optional[List[str]] = Argument(None, help=(
        "Source manifest files to copy from"
    )),
    output: Optional[str] = Option(None, '-o', '--output', help=(
        "Output data to a given location, by default outputs to stdout"
    )),
    credentials: str = Option(None, '--credentials', help=(
        "Credentials file, defaults to {config_path}/credentials.cfg"
    )),
    dataset: str = Option(None, '-d', '--dataset', help=(
        "Push only specified dataset"
    )),
    auth: str = Option(None, '-a', '--auth', help=(
        "Authorize as a client, defaults to {default_auth_client}"
    )),
    limit: int = Option(None, help=(
        "Limit number of rows read from each model"
    )),
    chunk_size: str = Option('1m', help=(
        "Push data in chunks (1b, 1k, 2m, ...), default: 1m"
    )),
    stop_time: str = Option(None, help=(
        "Stop pushing after given time (1s, 1m, 2h, ...), by default does not "
        "stops until all data is pushed"
    )),
    stop_row: int = Option(None, help=(
        "Stop after pushing n rows, by default does not stop until all data "
        "is pushed"
    )),
    state: pathlib.Path = Option(None, help=(
        "Save push state into a file, by default state is saved to "
        "{data_path}/push/{remote}.db SQLite database file"
    )),
    mode: Mode = Option('external', help=(
        "Mode of backend operation, default: external"
    )),
    dry_run: bool = Option(False, '--dry-run', help=(
        "Read data to be pushed, but do not push or write data to the "
        "destination."
    )),
    stop_on_error: bool = Option(False, '--stop-on-error', help=(
        "Exit immediately on first error."
    )),
    repeat_count: int = Option(5, '--repeat-count', help=(
        "Repeat push until this count if there are errors."
    )),
):
    """Push data to external data store"""
    if chunk_size:
        chunk_size = tobytes(chunk_size)

    if stop_time:
        stop_time = toseconds(stop_time)

    context = configure_context(ctx.obj, manifests, mode=mode)
    store = prepare_manifest(context)
    config: Config = context.get('config')

    if credentials:
        credsfile = pathlib.Path(credentials)
        if not credsfile.exists():
            echo(f"Credentials file {credsfile} does not exit.")
            raise Exit(code=1)
    else:
        credsfile = config.credentials_file
    # TODO: Read client credentials only if a Spinta URL is given.
    creds = get_client_credentials(credsfile, output)

    if not state:
        ensure_data_dir(config.data_path / 'push')
        state = config.data_path / 'push' / f'{creds.remote}.db'
    state = f'sqlite:///{state}'

    manifest = store.manifest
    if dataset and dataset not in manifest.datasets:
        echo(str(exceptions.NodeNotFound(manifest, type='dataset', name=dataset)))
        raise Exit(code=1)

    ns = manifest.namespaces['']

    echo(f"Get access token from {creds.server}")
    token = get_access_token(creds)

    client = requests.Session()
    client.headers['Content-Type'] = 'application/json'
    client.headers['Authorization'] = f'Bearer {token}'

    with context:
        auth_client = auth or config.default_auth_client
        require_auth(context, auth_client)

        _attach_backends(context, store, manifest)
        _attach_keymaps(context, store)

        from spinta.types.namespace import traverse_ns_models

        models = traverse_ns_models(context, ns, Action.SEARCH, dataset)
        models = sort_models_by_refs(models)
        models = list(reversed(list(models)))
        counts = count_rows(
            context,
            models,
            limit,
            stop_on_error=stop_on_error,
        )

        repeat = repeat_count
        while repeat:
            rows = iter_model_rows(
                context,
                models,
                counts,
                limit,
                stop_on_error=stop_on_error,
            )
            if repeat < repeat_count:
                rows = _get_rows_with_errors(
                    client,
                    creds.server,
                    context,
                    state.metadata,
                    rows
                )

            rows = tqdm.tqdm(rows, 'PUSH', ascii=True, total=sum(counts.values()))

            if state and repeat == repeat_count:
                state = _State(*_init_push_state(state, models))
                context.attach('push.state.conn', state.engine.begin)
                _reset_pushed(context, models, state.metadata)

            _push(
                context,
                client,
                creds.server,
                models,
                rows,
                state=state,
                stop_time=stop_time,
                stop_row=stop_row,
                chunk_size=chunk_size,
                dry_run=dry_run,
                stop_on_error=stop_on_error,
            )
            repeat -= 1

        _clean_up(
            context,
            client,
            creds.server,
            models,
            state=state,
            chunk_size=chunk_size,
            dry_run=dry_run,
            stop_on_error=stop_on_error,
        )


class _State(NamedTuple):
    engine: sa.engine.Engine
    metadata: sa.MetaData


def _push(
    context: Context,
    client: requests.Session,
    server: str,  # https://example.com/
    models: List[Model],
    rows: Iterable[ModelRow],
    *,
    state: Optional[_State] = None,
    stop_time: Optional[int] = None,    # seconds
    stop_row: Optional[int] = None,     # stop aftern given number of rows
    chunk_size: Optional[int] = None,   # split into chunks of given size in bytes
    dry_run: bool = False,              # do not send or write anything
    stop_on_error: bool = False,        # raise error immediately
) -> None:
    rows = _rows_to_push_rows(rows)

    if stop_time:
        rows = _add_stop_time(rows, stop_time)

    if state:
        rows = _check_push_state(context, rows, state.metadata)

    rows = _prepare_rows_for_push(rows)

    if stop_row:
        rows = itertools.islice(rows, stop_row)

    rows = _push_to_remote_spinta(
        client,
        server,
        rows,
        chunk_size,
        dry_run=dry_run,
    )

    if state and not dry_run:
        rows = _save_push_state(context, rows, state.metadata)

    _push_rows(rows, stop_on_error)


def _clean_up(
    context: Context,
    client: requests.Session,
    server: str,
    models: List[Model],
    *,
    state: Optional[_State] = None,
    chunk_size: Optional[int] = None,
    dry_run: bool = False,
    stop_on_error: bool = False,
):
    deleted_rows = _get_deleted_rows(
        models,
        context,
        state.metadata
    )
    deleted_rows = _push_to_remote_spinta(
        client,
        server,
        deleted_rows,
        chunk_size,
        dry_run=dry_run,
    )

    deleted_rows = _save_state_after_delete(
        deleted_rows,
        context,
        state.metadata
    )

    _push_rows(deleted_rows, stop_on_error)


class _PushRow:
    model: Model
    data: Dict[str, Any]
    # SHA1 checksum of data generated with _get_data_rev.
    checksum: Optional[str]
    # True if data has already been sent.
    saved: bool = False
    # If push request received an error
    error: bool = False
    # True if object has been deleted
    deleted: bool = False

    def __init__(self, model: Model, data: Dict[str, Any]):
        self.model = model
        self.data = data
        self.checksum = None
        self.saved = False


def _push_rows(rows: Iterator[_PushRow], stop_on_error: bool = False) -> None:
    while True:
        try:
            next(rows)
        except StopIteration:
            break
        except:
            if stop_on_error:
                raise
            log.exception("Error while reading data.")


def _attach_backends(context: Context, store: Store, manifest: Manifest) -> None:
    context.attach('transaction', manifest.backend.transaction)
    backends = set()
    for backend in store.backends.values():
        backends.add(backend.name)
        context.attach(f'transaction.{backend.name}', backend.begin)
    for backend in manifest.backends.values():
        backends.add(backend.name)
        context.attach(f'transaction.{backend.name}', backend.begin)
    for dataset_ in manifest.datasets.values():
        for resource in dataset_.resources.values():
            if resource.backend and resource.backend.name not in backends:
                backends.add(resource.backend.name)
                context.attach(f'transaction.{resource.backend.name}', resource.backend.begin)


def _attach_keymaps(context: Context, store: Store) -> None:
    for keymap in store.keymaps.values():
        context.attach(f'keymap.{keymap.name}', lambda: keymap)


def _get_rows_with_errors(
    client: requests.Session,
    server: str,
    context: Context,
    metadata: sa.MetaData,
    rows: Iterable[ModelRow]
) -> Iterable[ModelRow]:
    conn = context.get('push.state.conn')
    for model, row in rows:
        table = metadata.tables[row['_type']]
        row_with_errors = conn.execute(
            sa.select([table.c.id]).
            where(
                table.c.error.is_(True) &
                (table.c.id == row['_id'])
            )
        )
        if len(row_with_errors.all()) > 0:
            resp = client.get(f'{server}/{row["_type"]}/{row["_id"]}')
            # if we get response with status_code 200,
            # it means that row is already saved in target server
            # and there was actually no error
            if resp.status_code == 200:
                conn.execute(
                    table.update().
                    where(
                        (table.c.id == row['_id'])
                    ).
                    values(
                        revision=resp.json()['_revision'],
                        error=False
                    )
                )
            # if we get response with status_code 404,
            # it means that row is not saved in target server
            # and we need to push it again
            elif resp.status_code == 404:
                yield model, row


def _rows_to_push_rows(rows: Iterable[ModelRow]) -> Iterator[_PushRow]:
    for model, row in rows:
        yield _PushRow(model, row)


def _prepare_rows_for_push(rows: Iterable[_PushRow]) -> Iterator[_PushRow]:
    for row in rows:
        if row.saved:
            row.data['_op'] = 'patch'

            where = {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['_id']},
                    row.data['_id'],
                ]
            }
            row.data['_where'] = spyna.unparse(where)
        else:
            row.data['_op'] = 'insert'

            # if _revision is passed insert action gives ManagedProperty error
            if '_revision' in row.data:
                row.data.pop('_revision')
        yield row


def _push_to_remote_spinta(
    client: requests.Session,
    server: str,
    rows: Iterable[_PushRow],
    chunk_size: int,
    *,
    dry_run: bool = False,
    stop_on_error: bool = False,
) -> Iterator[_PushRow]:
    prefix = '{"_data":['
    suffix = ']}'
    slen = len(suffix)
    chunk = prefix
    ready: List[_PushRow] = []

    for row in rows:
        data = fix_data_for_json(row.data)
        data = json.dumps(data, ensure_ascii=False)
        if ready and len(chunk) + len(data) + slen > chunk_size:
            yield from _send_and_receive(
                client,
                server,
                ready,
                chunk + suffix,
                dry_run=dry_run,
                stop_on_error=stop_on_error,
            )
            chunk = prefix
            ready = []
        chunk += (',' if ready else '') + data
        ready.append(row)

    if ready:
        yield from _send_and_receive(
            client,
            server,
            ready,
            chunk + suffix,
            dry_run=dry_run,
            stop_on_error=stop_on_error,
        )


def _send_and_receive(
    client: requests.Session,
    server: str,
    rows: List[_PushRow],
    data: str,
    *,
    dry_run: bool = False,
    stop_on_error: bool = False,
) -> Iterator[_PushRow]:
    if dry_run:
        recv = _send_data_dry_run(data)
    else:
        recv = _send_data(
            client,
            server,
            rows,
            data,
            stop_on_error=stop_on_error,
        )
    yield from _map_sent_and_recv(rows, recv)


def _send_data_dry_run(
    data: str,
) -> Optional[List[Dict[str, Any]]]:
    """Pretend data has been sent to a target location."""
    recv = json.loads(data)['_data']
    for row in recv:
        if '_id' not in row:
            row['_id'] = str(uuid.uuid4())
        row['_rev'] = str(uuid.uuid4())
    return recv


def _send_data(
    client: requests.Session,
    server: str,
    rows: List[_PushRow],
    data: str,
    *,
    stop_on_error: bool = False,
) -> Optional[List[Dict[str, Any]]]:
    data = data.encode('utf-8')

    try:
        resp = client.post(server, data=data)
    except IOError as e:
        log.error(
            (
                "Error when sending and receiving data.%s\n"
                "Error: %s"
            ),
            _get_row_for_error(rows),
            e,
        )
        if stop_on_error:
            raise
        return

    try:
        resp.raise_for_status()
    except HTTPError:
        try:
            recv = resp.json()
        except requests.JSONDecodeError:
            log.error(
                (
                    "Error when sending and receiving data.\n"
                    "Server response (status=%s):\n%s"
                ),
                resp.status_code,
                textwrap.indent(resp.text, '    '),
            )
            if stop_on_error:
                raise
        else:
            errors = recv.get('errors')
            log.error(
                (
                    "Error when sending and receiving data.%s\n"
                    "Server response (status=%s):\n%s"
                ),
                _get_row_for_error(rows, errors),
                resp.status_code,
                textwrap.indent(pprintpp.pformat(recv), '    '),
            )
        if stop_on_error:
            raise
        return

    return resp.json()['_data']


class _ErrorContext(TypedDict):
    # Manifest name
    manifest: str

    # Dataset name
    dataset: str

    # Absolute model name (but does not start with /)
    model: str

    # Property name
    property: str

    # Property data type
    type: str

    # Model name in data source
    entity: str

    # Property name in data source
    attribute: str

    # Full class path of component involved in the error.
    component: str

    # Line number or other location (depends on manifest) node in manifest
    # involved in the error.
    schema: str

    # External object id
    id: str


class _Error(TypedDict):
    # Error type
    type: str

    # Error code (exception class name)
    code: str

    # Error context.
    context: _ErrorContext

    # Message template.
    tempalte: str

    # Message compiled from templates and context.
    message: str


def _get_row_for_error(
    rows: List[_PushRow],
    errors: Optional[List[_Error]] = None,
) -> str:
    message = []
    size = len(rows)

    if errors:
        rows_by_id = {
            row.data['_id']: row
            for row in rows
            if row.data and '_id' in row.data
        }
        for error in errors:
            ctx = error.get('context') or {}
            if 'id' in ctx and ctx['id'] in rows_by_id:
                row = rows_by_id[ctx['id']]
                message += [
                    f" Model {row.model.name}, data:",
                    ' ' + pprintpp.pformat(row.data),
                ]

    if len(message) == 0 and size > 0:
        row = rows[0]
        data = pprintpp.pformat(row.data)
        message += [
            (
                f" Model {row.model.name},"
                f" items in chunk: {size},"
                f" first item in chunk:"
            ),
            data
        ]

    return '\n'.join(message)


def _map_sent_and_recv(
    sent: List[_PushRow],
    recv: Optional[List[Dict[str, Any]]],
) -> Iterator[_PushRow]:
    if recv is None:
        # We don't have a response, because there was an error while
        # communicating with the target server.
        for row in sent:
            row.error = True
            yield row
    else:
        assert len(sent) == len(recv), (
            f"len(sent) = {len(sent)}, len(received) = {len(recv)}"
        )
        for sent_row, recv_row in zip(sent, recv):
            if '_id' in sent_row.data:
                _id = sent_row.data['_id']
            else:
                # after delete
                _id = spyna.parse(sent_row.data['_where'])['args'][1]
                sent_row.deleted = True

            assert _id == recv_row['_id'], (
                f"sent._id = {_id}, "
                f"received._id = {recv_row['_id']}"
            )
            sent_row.data['_revision'] = recv_row['_revision']
            yield sent_row


def _add_stop_time(
    rows: Iterable[_PushRow],
    stop: int,  # seconds
) -> Iterator[_PushRow]:
    start = time.time()
    for row in rows:
        yield row
        if time.time() - start > stop:
            break


def _init_push_state(
    dburi: str,
    models: List[Model],
) -> Tuple[sa.engine.Engine, sa.MetaData]:
    engine = sa.create_engine(dburi)
    metadata = sa.MetaData(engine)
    for model in models:
        table = sa.Table(
            model.name, metadata,
            sa.Column('id', sa.Unicode, primary_key=True),
        )
        table.create(checkfirst=True)
        metadata.reflect(only=[model.name], extend_existing=True)

        renamed = _rename_column(engine, table, 'rev', 'checksum')
        if not renamed:
            _add_column(engine, table, sa.Column('checksum', sa.Unicode))
        _add_column(engine, table, sa.Column('revision', sa.Unicode))
        _add_column(engine, table, sa.Column('pushed', sa.DateTime))
        _add_column(engine, table, sa.Column('deleted', sa.Boolean))
        _add_column(engine, table, sa.Column('error', sa.Boolean))
        metadata.reflect(only=[table.name], extend_existing=True)

    return engine, metadata


def _add_column(
    engine: sa.engine.Engine,
    table: sa.Table,
    column: sa.Column,
):
    column_type = column.type.compile(engine.dialect)
    if column.name not in table.c:
        engine.execute('ALTER TABLE "%s" ADD COLUMN "%s" %s' % (
            table.name,
            column.name,
            column_type
        ))


def _rename_column(
    engine: sa.engine.Engine,
    table: sa.Table,
    old_column_name: str,
    new_column_name: str
) -> bool:  # return True if column was renamed
    if old_column_name in table.c and new_column_name not in table.c:
        engine.execute('ALTER TABLE "%s" RENAME COLUMN "%s" TO "%s"' % (
            table.name,
            old_column_name,
            new_column_name
        ))
        return True
    return False


def _get_model_type(row: _PushRow) -> str:
    return row.data['_type']


def _get_data_checksum(row: _PushRow):
    data = fix_data_for_json(take(row.data))
    data = flatten([data])
    data = [[k, v] for x in data for k, v in sorted(x.items())]
    data = msgpack.dumps(data, strict_types=True)
    checksum = hashlib.sha1(data).hexdigest()
    return checksum


class _Saved(NamedTuple):
    revision: str
    checksum: str


def _reset_pushed(
    context: Context,
    models: List[Model],
    metadata: sa.MetaData,
):
    conn = context.get('push.state.conn')
    for model in models:
        table = metadata.tables[model.name]

        # reset pushed so we could see which objects were deleted
        conn.execute(
            table.update().
            where(table.c.deleted.is_(False)).
            values(pushed=None)
        )


def _check_push_state(
    context: Context,
    rows: Iterable[_PushRow],
    metadata: sa.MetaData,
):
    conn = context.get('push.state.conn')

    for model_type, group in itertools.groupby(rows, key=_get_model_type):
        table = metadata.tables[model_type]

        query = sa.select([table.c.id, table.c.revision, table.c.checksum])
        saved_rows = {
            state[table.c.id]: _Saved(
                state[table.c.revision],
                state[table.c.checksum],
            )
            for state in conn.execute(query)
        }

        for row in group:
            _id = row.data['_id']
            row.checksum = _get_data_checksum(row)
            saved = saved_rows.get(_id)
            if saved is None:
                row.saved = False
            else:
                row.saved = True
                row.data['_revision'] = saved.revision
                if saved.checksum == row.checksum:
                    conn.execute(
                        table.update().
                        where(table.c.id == _id).
                        values(
                            pushed=datetime.datetime.now()
                        )
                    )
                    continue  # Nothing has changed.

            yield row


def _save_push_state(
    context: Context,
    rows: Iterable[_PushRow],
    metadata: sa.MetaData,
) -> Iterator[_PushRow]:
    conn = context.get('push.state.conn')
    for row in rows:
        table = metadata.tables[row.data['_type']]
        if row.saved:
            conn.execute(
                table.update().
                where(table.c.id == row.data['_id']).
                values(
                    id=row.data['_id'],
                    revision=row.data['_revision'],
                    checksum=row.checksum,
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    deleted=row.deleted,
                )
            )
        else:
            conn.execute(
                table.insert().
                values(
                    id=row.data['_id'],
                    revision=row.data.get('_revision'),
                    checksum=row.checksum,
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    deleted=row.deleted,
                )
            )
        yield row


def _prepare_deleted_rows(
    rows,
    model: Model,
    table
) -> Iterable[_PushRow]:
    for row in rows:
        where = {
            'name': 'eq',
            'args': [
                {'name': 'bind', 'args': ['_id']},
                row[table.c.id],
            ]
        }
        yield _PushRow(model, {
            '_op': 'delete',
            '_type': model.name,
            '_where': spyna.unparse(where)
        })


def _get_deleted_rows(
    models: List[Model],
    context: Context,
    metadata: sa.MetaData,
) -> Iterable[_PushRow]:
    conn = context.get('push.state.conn')
    for model in models:
        table = metadata.tables[model.name]
        deleted_rows = conn.execute(
            sa.select([table.c.id]).
            where(
                table.c.pushed.is_(None) &
                table.c.deleted.is_(False)
            )
        )
        yield from _prepare_deleted_rows(deleted_rows, model, table)


def _save_state_after_delete(
    rows: Iterable[_PushRow],
    context: Context,
    metadata: sa.MetaData,
) -> Iterator[_PushRow]:
    conn = context.get('push.state.conn')
    for row in rows:
        table = metadata.tables[row.data['_type']]
        _id = spyna.parse(row.data['_where'])['args'][1]
        if row.error:
            conn.execute(
                table.update().
                where(table.c.id == _id).
                values(
                    pushed=datetime.datetime.now(),
                    error=row.error,
                )
            )
        else:
            conn.execute(
                table.update().
                where(table.c.id == _id).
                values(
                    revision=row.data['_revision'],
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    deleted=row.deleted,
                )
            )
        yield row
