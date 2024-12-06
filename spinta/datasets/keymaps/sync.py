import dataclasses
import datetime
from collections.abc import Generator

from typer import echo
from typing import List, Iterable

from spinta import commands
from spinta.auth import authorized
from spinta.cli.helpers.errors import ErrorCounter
from spinta.components import Context, Model, Action, Property, Config
from spinta.datasets.backends.helpers import extract_values_from_row
from spinta.datasets.keymaps.components import KeyMap
import tqdm

from spinta.utils.response import get_request


def _build_changelog_url(
    server: str,
    model: str,
    offset_cid: int,
    limit: int
) -> str:
    url = f'{server}/{model}/:changes'
    if offset_cid:
        url = f'{url}/{offset_cid + 1}'
    url = f'{url}?limit({limit})'
    return url


def _fetch_changelog_data(
    config: Config,
    model: Model,
    client,
    server: str,
    offset_cid: int,
    error_counter: ErrorCounter,
    timeout: tuple[float, float]
):
    limit = config.sync_page_size
    offset = offset_cid
    while True:
        url = _build_changelog_url(
            server=server,
            model=model.model_type(),
            offset_cid=offset,
            limit=limit
        )
        status_code, resp = get_request(
            client,
            url,
            error_counter=error_counter,
            timeout=timeout
        )
        if status_code != 200:
            break

        data = resp.get('_data')
        if not data:
            break

        cid = offset
        i = 0
        for i, row in enumerate(data, start=1):
            cid = row['_cid']
            yield row

        # Do not iterate further if limit was not reached
        if i < limit:
            break

        offset = cid


def _cid_offset(
    keymap: KeyMap,
    model_keymaps: list,
    reset_cid: bool
) -> int:
    sync_cid = 0
    if not reset_cid:
        initial = True
        for keymap_name in model_keymaps:
            cid = keymap.get_last_synced_id(keymap_name)
            if not cid:
                sync_cid = 0
                break

            if initial:
                sync_cid = cid
                initial = False
            else:
                sync_cid = min(sync_cid, cid)
    return sync_cid


@dataclasses.dataclass
class KeymapData:
    key: str
    value: object
    identifier: str
    data: dict


def process_keymap_data(
    keymap: KeyMap,
    model: Model,
    data: Iterable,
    primary_keys: list[Property],
    counters: dict,
    dry_run: bool
) -> Generator[KeymapData]:
    for row in data:
        cid = row['_cid']
        if row['_op'] == 'insert':
            yield from (
                KeymapData(key=key, value=value, identifier=identifier, data=row)
                for key, value, identifier in
                sync_model_insert(
                    keymap=keymap,
                    model=model,
                    row=row,
                    primary_keys=primary_keys,
                    counters=counters,
                    dry_run=dry_run
                )
            )
        elif row['_op'] == 'patch' or row['_op'] == 'update':
            yield from (
                KeymapData(key=key, value=value, identifier=identifier, data=row)
                for key, value, identifier in
                sync_model_update(
                    keymap=keymap,
                    model=model,
                    row=row,
                    counters=counters,
                    dry_run=dry_run
                )
            )


def sync_model_insert(keymap: KeyMap, model: Model, row: dict, primary_keys: List[Property], counters: dict,
                      dry_run: bool):
    values = _extract_row_data_from_keys(row, primary_keys)
    value = list(values.values())
    if len(value) == 1:
        value = value[0]

    if not dry_run:
        yield model.model_type(), value, row['_id']

    if model.model_type() in counters and model.model_type() in counters[model.model_type()]:
        counters[model.model_type()][model.model_type()].update(1)
    if '_total' in counters:
        counters['_total'].update(1)

    if row['_id'] and model.required_keymap_properties:
        for combination in model.required_keymap_properties:
            joined = '_'.join(combination)
            key = f'{model.model_type()}.{joined}'
            val = extract_values_from_row(row, model, combination)

            if not dry_run:
                yield key, val, row['_id']

            if model.model_type() in counters and key in counters[model.model_type()]:
                counters[model.model_type()][key].update(1)
            if '_total' in counters:
                counters['_total'].update(1)


def sync_model_update(keymap: KeyMap, model: Model, row: dict, counters: dict, dry_run: bool):
    for km, props in keymaps_affected_by_change(row, model).items():
        decoded = keymap.decode(km, row['_id'])
        remapped = remap_decoded_values(decoded, props)
        for key in remapped.keys():
            if key in row:
                remapped[key] = row[key]
        val = list(remapped.values())
        if len(val) == 1:
            val = val[0]

        if not dry_run:
            yield km, val, row['_id']

        if model.model_type() in counters and km in counters[model.model_type()]:
            counters[model.model_type()][km].update(1)
        if '_total' in counters:
            counters['_total'].update(1)


def sync_keymap(
    context: Context,
    keymap: KeyMap,
    client,
    server: str,
    models: List[Model],
    error_counter: ErrorCounter,
    no_progress_bar: bool,
    reset_cid: bool,
    timeout: tuple[float, float],
    dry_run: bool = False,
):
    config = context.get('config')
    counters = {}
    if not no_progress_bar:
        counters = {
            '_total': tqdm.tqdm(desc='SYNCHRONIZING KEYMAP', ascii=True)
        }
    try:
        for model in models:
            model_keymaps = [model.model_type()]
            primary_keys = model.external.pkeys
            skip_model = False
            for key in primary_keys:
                is_authorized = authorized(context, key, action=Action.SEARCH)
                if not is_authorized:
                    echo(f"SKIPPED KEYMAP '{model.model_type()}' MODEL SYNC, NO PERMISSION.")
                    skip_model = True
                    break
            if skip_model:
                continue

            for combination in model.required_keymap_properties:
                model_keymaps.append(f'{model.model_type()}.{"_".join(combination)}')

            # Get min cid (in case new model was added, or models are out of sync)
            offset_cid = _cid_offset(
                keymap=keymap,
                model_keymaps=model_keymaps,
                reset_cid=reset_cid
            )

            data = _fetch_changelog_data(
                config=config,
                model=model,
                client=client,
                server=server,
                offset_cid=offset_cid,
                error_counter=error_counter,
                timeout=timeout
            )

            if not no_progress_bar:
                model_counters = {}
                for keymap_name in model_keymaps:
                    model_counters[keymap_name] = tqdm.tqdm(desc=keymap_name, ascii=True)
                counters[model.model_type()] = model_counters

            data = process_keymap_data(
                keymap=keymap,
                model=model,
                data=data,
                primary_keys=primary_keys,
                counters=counters,
                dry_run=dry_run
            )
            data = commands.sync(context, keymap, data=data)
            try:
                for row in data:
                    offset_cid = row.data['_cid']
            finally:
                if not dry_run:
                    for keymap_name in model_keymaps:
                        keymap.update_sync_data(keymap_name, offset_cid, datetime.datetime.now())

                if model.model_type() in counters:
                    for counter in counters[model.model_type()].values():
                        counter.close()
    finally:
        if '_total' in counters:
            counters['_total'].close()


def remap_decoded_values(decoded: object, properties: List[str]):
    mapped = {}
    if not isinstance(decoded, list):
        decoded = [decoded]
    for i, prop in enumerate(properties):
        mapped[prop] = decoded[i]
    return mapped


def keymaps_affected_by_change(row: dict, model: Model) -> dict:
    keymaps = {}
    for key in model.external.pkeys:
        if key.name in row.keys():
            keymaps[model.model_type()] = list([prop.name for prop in model.external.pkeys])
            break

    for combination in model.required_keymap_properties:
        for prop in combination:
            if prop in row.keys():
                keymaps[f'{model.model_type()}.{"_".join(combination)}'] = combination
                break

    return keymaps


def _extract_row_data_from_keys(row: dict, keys: List[Property]) -> dict:
    result = {}
    for key in keys:
        if key.name in row:
            result[key.name] = row[key.name]
    return result


@commands.sync.register(Context, KeyMap)
def sync(context: Context, keymap: KeyMap, *, data: Generator[KeymapData]):
    for row in data:
        keymap.synchronize(row.key, row.value, row.identifier)
        yield row
