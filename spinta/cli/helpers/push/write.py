import itertools
import json
import textwrap
import time
import uuid
from copy import copy
from typing import Any, Tuple
from typing import Dict
from typing import Iterable
from typing import Iterator
from typing import List
from typing import Optional


import pprintpp

import requests
from requests import HTTPError

import sqlalchemy as sa
import spinta.cli.push as cli_push

from spinta import spyna
from spinta.cli.helpers.data import ModelRow
from spinta.cli.helpers.errors import ErrorCounter
from spinta.cli.helpers.push.components import State, PushRow, Error
from spinta.cli.helpers.push.state import check_push_state, save_push_state, save_page_values
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.components import Context
from spinta.components import Model
from spinta.core.ufuncs import asttoexpr
from spinta.utils.json import fix_data_for_json


def _prepare_rows_for_push(rows: Iterable[PushRow]) -> Iterator[PushRow]:
    for row in rows:
        row.data['_op'] = row.op
        if row.op == 'patch':
            where = {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['_id']},
                    row.data['_id'],
                ]
            }
            row.data['_where'] = spyna.unparse(where)
        elif row.op == 'insert':
            # if _revision is passed insert action gives ManagedProperty error
            if '_revision' in row.data:
                row.data.pop('_revision')
        elif row.op == 'delete' or not row.send:
            pass
        else:
            raise NotImplementedError(row.op)
        yield row


def prepare_rows_with_errors(
    client: requests.Session,
    server: str,
    context: Context,
    rows,
    model: Model,
    table: sa.Table,
    timeout: Tuple[float, float],
    error_counter: ErrorCounter = None
) -> Iterable[ModelRow]:
    conn = context.get('push.state.conn')
    for row in rows:
        type = model.model_type()
        _id = row[table.c.id]
        checksum = row[table.c.checksum]
        data = json.loads(row[table.c.data]) if row[table.c.data] else {}

        status_code, resp = send_request(
            client,
            f'{server}/{type}/{_id}',
            "GET",
            rows=[],
            data="",
            ignore_errors=[404],
            error_counter=error_counter,
            timeout=timeout
        )

        if status_code == 200:

            # Was deleted on local server, but found on target server,
            # which means we need to delete it
            if not data:
                yield prepare_rows_for_deletion(model, _id, error=True)
            # Was inserted or updated without errors
            elif checksum == get_data_checksum(resp, model):
                conn.execute(
                    table.update().
                    where(
                        (table.c.id == _id)
                    ).
                    values(
                        revision=resp['_revision'],
                        error=False,
                        data=None
                    )
                )
                yield PushRow(
                    model,
                    {'_type': type},
                    send=False
                )
                # Need to push again
            else:
                data['_revision'] = resp['_revision']
                yield PushRow(
                    model,
                    data,
                    checksum=checksum,
                    saved=True,
                    op="patch",
                    error=True
                )

        elif status_code == 404:
            # Was deleted on both - local and target servers
            if not data:
                conn.execute(
                    table.delete().
                    where(
                        (table.c.id == _id)
                    )
                )
                yield PushRow(
                    model,
                    {'_type': type},
                    send=False
                )
            # Need to push again
            else:
                yield PushRow(
                    model,
                    data,
                    checksum=checksum,
                    saved=True,
                    op="insert",
                    error=True
                )


def prepare_rows_for_deletion(
    model: Model,
    _id: str,
    error: bool = False
):
    where = {
        'name': 'eq',
        'args': [
            {'name': 'bind', 'args': ['_id']},
            _id,
        ]
    }
    return PushRow(
        model,
        {
            '_type': model.name,
            '_where': spyna.unparse(where)
        },
        saved=True,
        op="delete",
        error=error
    )


def get_row_for_error(
    rows: List[PushRow],
    errors: Optional[List[Error]] = None,
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


def _push_to_remote_spinta(
    client: requests.Session,
    server: str,
    rows: Iterable[PushRow],
    chunk_size: int,
    timeout: Tuple[float, float],
    *,
    dry_run: bool = False,
    stop_on_error: bool = False,
    error_counter: ErrorCounter = None,
) -> Iterator[PushRow]:
    prefix = '{"_data":['
    suffix = ']}'
    slen = len(suffix)
    chunk = prefix
    ready: List[PushRow] = []

    for row in rows:
        if error_counter:
            if error_counter.has_reached_max():
                break

        data = fix_data_for_json(row.data)
        tmp = data
        if '_page' in data:
            tmp = copy(data)
            tmp.pop('_page')
        data = json.dumps(tmp, ensure_ascii=False)

        if ready and (
            len(chunk) + len(data) + slen > chunk_size or
            row.push
        ):
            yield from _send_and_receive(
                client,
                server,
                ready,
                chunk + suffix,
                dry_run=dry_run,
                stop_on_error=stop_on_error,
                error_counter=error_counter,
                timeout=timeout
            )
            chunk = prefix
            ready = []
        if not row.push and row.send:
            chunk += (',' if ready else '') + data
            ready.append(row)

    if ready:
        if error_counter:
            if not error_counter.has_reached_max():
                yield from _send_and_receive(
                    client,
                    server,
                    ready,
                    chunk + suffix,
                    dry_run=dry_run,
                    stop_on_error=stop_on_error,
                    error_counter=error_counter,
                    timeout=timeout
                )
        else:
            yield from _send_and_receive(
                client,
                server,
                ready,
                chunk + suffix,
                dry_run=dry_run,
                stop_on_error=stop_on_error,
                error_counter=error_counter,
                timeout=timeout
            )


def _send_and_receive(
    client: requests.Session,
    server: str,
    rows: List[PushRow],
    data: str,
    timeout: Tuple[float, float],
    *,
    dry_run: bool = False,
    stop_on_error: bool = False,
    error_counter: ErrorCounter = None,
) -> Iterator[PushRow]:
    if dry_run:
        recv = _send_data_dry_run(data)
    else:
        _, recv = send_request(
            client,
            server,
            "POST",
            rows,
            data,
            stop_on_error=stop_on_error,
            error_counter=error_counter,
            timeout = timeout
        )
        if recv:
            recv = recv['_data']
    yield from _map_sent_and_recv(rows, recv)


def _send_data_dry_run(
    data: str,
) -> Optional[List[Dict[str, Any]]]:
    """Pretend data has been sent to a target location."""
    recv = json.loads(data)['_data']
    for row in recv:
        # Extract id from delete case, it is stored in _where eq expression
        if '_op' in row and row['_op'] == 'delete' and '_where' in row:
            id_ = spyna.parse(asttoexpr(row['_where']))['args'][1]
            row['_id'] = id_

        if '_id' not in row:
            row['_id'] = str(uuid.uuid4())
        if '_revision' not in row:
            row['_revision'] = str(uuid.uuid4())
    return recv


def _map_sent_and_recv(
    sent: List[PushRow],
    recv: Optional[List[Dict[str, Any]]],
) -> Iterator[PushRow]:
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

            assert _id == recv_row['_id'], (
                f"sent._id = {_id}, "
                f"received._id = {recv_row['_id']}"
            )
            sent_row.data['_revision'] = recv_row['_revision']
            sent_row.error = False
            yield sent_row


def _push_rows(
    rows: Iterator[PushRow],
    stop_on_error: bool = False,
    error_counter: ErrorCounter = None,
) -> None:
    while True:
        try:
            next(rows)
        except StopIteration:
            break
        except:
            if error_counter:
                error_counter.increase()

            if stop_on_error:
                raise
            cli_push.log.exception("Error while reading data.")
        if error_counter and error_counter.has_reached_max():
            break


def send_request(
    client: requests.Session,
    server: str,
    method: str,
    rows: List[PushRow],
    data: str,
    timeout: Tuple[float, float],
    *,
    stop_on_error: bool = False,
    ignore_errors: Optional[List[int]] = None,
    error_counter: ErrorCounter = None,
) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    data = data.encode('utf-8')
    if not ignore_errors:
        ignore_errors = []

    try:
        resp = client.request(method, server, data=data, timeout=timeout)
    except requests.exceptions.ReadTimeout as e:
        if error_counter:
            error_counter.increase()
        cli_push.log.error(
                f"Read timeout occurred. Consider using a smaller --chunk-size to avoid timeouts. Current timeout settings are (connect: {timeout[0]}s, read: {timeout[1]}s)."
        )
        if stop_on_error:
            raise
        return None, None
    except requests.exceptions.ConnectTimeout as e:
        if error_counter:
            error_counter.increase()
        cli_push.log.error(
                f"Connect timeout occurred. Current timeout settings are (connect: {timeout[0]}s, read: {timeout[1]}s)."
        )
        if stop_on_error:
            raise
        return None, None
    except IOError as e:
        if error_counter:
            error_counter.increase()
        cli_push.log.error(
            (
                "Error when sending and receiving data.%s\n"
                "Error: %s"
            ),
            get_row_for_error(rows),
            e,
        )
        if stop_on_error:
            raise
        return None, None

    try:
        resp.raise_for_status()
    except HTTPError:
        if resp.status_code not in ignore_errors:
            if error_counter:
                error_counter.increase()
            try:
                recv = resp.json()
            except requests.JSONDecodeError:
                cli_push.log.error(
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
                cli_push.log.error(
                    (
                        "Error when sending and receiving data.%s\n"
                        "Server response (status=%s):\n%s"
                    ),
                    get_row_for_error(rows, errors),
                    resp.status_code,
                    textwrap.indent(pprintpp.pformat(recv), '    '),
                )
            if stop_on_error:
                raise
        return resp.status_code, None

    return resp.status_code, resp.json()


def push(
    context: Context,
    client: requests.Session,
    server: str,  # https://example.com/
    models: List[Model],
    rows: Iterable[PushRow],
    timeout: Tuple[float, float],
    *,
    state: Optional[State] = None,
    stop_time: Optional[int] = None,    # seconds
    stop_row: Optional[int] = None,     # stop aftern given number of rows
    chunk_size: Optional[int] = None,   # split into chunks of given size in bytes
    dry_run: bool = False,              # do not send or write anything
    stop_on_error: bool = False,        # raise error immediately
    error_counter: ErrorCounter = None,
) -> None:
    if stop_time:
        rows = _add_stop_time(rows, stop_time)

    if state:
        rows = check_push_state(context, rows, state.metadata)

    rows = _prepare_rows_for_push(rows)

    if stop_row:
        rows = itertools.islice(rows, stop_row)

    rows = _push_to_remote_spinta(
        client,
        server,
        rows,
        chunk_size,
        dry_run=dry_run,
        error_counter=error_counter,
        timeout=timeout
    )
    if state and not dry_run:
        rows = save_push_state(context, rows, state.metadata)

    _push_rows(rows, stop_on_error, error_counter)


def _add_stop_time(
    rows: Iterable[PushRow],
    stop: int,  # seconds
) -> Iterator[PushRow]:
    start = time.time()
    for row in rows:
        yield row
        if time.time() - start > stop:
            break
