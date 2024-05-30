import datetime
from typer import echo
from typing import List

from spinta.auth import authorized
from spinta.cli.helpers.errors import ErrorCounter
from spinta.components import Context, Model, Action, Property
from spinta.datasets.backends.helpers import extract_values_from_row
from spinta.datasets.keymaps.components import KeyMap
import tqdm

from spinta.exceptions import UnauthorizedKeymapSync
from spinta.utils.response import get_request


def sync_keymap(
    context: Context,
    keymap: KeyMap,
    client,
    server: str,
    models: List[Model],
    error_counter: ErrorCounter,
    no_progress_bar: bool,
    reset_cid: bool
):
    counters = {}
    if not no_progress_bar:
        counters = {
            '_total': tqdm.tqdm(desc='SYNCHRONIZING KEYMAP', ascii=True)
        }
        echo()

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
        sync_cid = 0
        initial = True
        if not reset_cid:
            for keymap_name in model_keymaps:
                cid = keymap.get_sync_data(keymap_name)
                if not cid:
                    sync_cid = 0
                    break
                else:
                    if initial:
                        sync_cid = cid
                        initial = False
                    else:
                        sync_cid = min(sync_cid, cid)

        url = f'{server}/{model.model_type()}/:changes'
        if sync_cid:
            url = f'{url}/{sync_cid+1}'
        status_code, resp = get_request(
            client,
            url,
            ignore_errors=[404],
            error_counter=error_counter,
        )
        if status_code == 200:
            data = resp['_data']

            if not no_progress_bar:
                model_counters = {}
                for keymap_name in model_keymaps:
                    model_counters[keymap_name] = tqdm.tqdm(desc=keymap_name, ascii=True)
                counters[model.model_type()] = model_counters

            for row in data:
                if row['_op'] == 'insert':
                    sync_model_insert(keymap, model, row, primary_keys, counters)
                elif row['_op'] == 'patch' or row['_op'] == 'update':
                    sync_model_update(keymap, model, row, counters)
                sync_cid = row['_cid']

            for keymap_name in model_keymaps:
                keymap.update_sync_data(keymap_name, sync_cid, datetime.datetime.now())

            if model.model_type() in counters:
                for counter in counters[model.model_type()].values():
                    counter.close()

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


def sync_model_insert(keymap: KeyMap, model: Model, row: dict, primary_keys: List[Property], counters: dict):
    values = _extract_row_data_from_keys(row, primary_keys)
    value = list(values.values())
    if len(value) == 1:
        value = value[0]

    keymap.synchronize(model.model_type(), value, row['_id'])
    if model.model_type() in counters and model.model_type() in counters[model.model_type()]:
        counters[model.model_type()][model.model_type()].update(1)
    if '_total' in counters:
        counters['_total'].update(1)

    if row['_id'] and model.required_keymap_properties:
        for combination in model.required_keymap_properties:
            joined = '_'.join(combination)
            key = f'{model.model_type()}.{joined}'
            val = extract_values_from_row(row, model, combination)
            keymap.synchronize(key, val, row['_id'])
            if model.model_type() in counters and key in counters[model.model_type()]:
                counters[model.model_type()][key].update(1)
            if '_total' in counters:
                counters['_total'].update(1)


def sync_model_update(keymap: KeyMap, model: Model, row: dict, counters: dict):
    for km, props in keymaps_affected_by_change(row, model).items():
        decoded = keymap.decode(km, row['_id'])
        remapped = remap_decoded_values(decoded, props)
        for key in remapped.keys():
            if key in row:
                remapped[key] = row[key]
        val = list(remapped.values())
        if len(val) == 1:
            val = val[0]
        keymap.synchronize(km, val, row['_id'])
        if model.model_type() in counters and km in counters[model.model_type()]:
            counters[model.model_type()][km].update(1)
        if '_total' in counters:
            counters['_total'].update(1)
