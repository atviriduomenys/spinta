import pathlib
from io import StringIO
from typing import Tuple
from typing import Union

from pytest import FixtureRequest

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.cli.manifest import copy_manifest
from spinta.components import Context
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.core.config import configure_rc
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestPath
from spinta.manifests.helpers import detect_manifest_from_path
from spinta.manifests.tabular.helpers import normalizes_columns
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import TestContext
from spinta.testing.context import create_test_context


def compare_manifest(manifest: Manifest, expected: str, context: Context = None) -> Tuple[str, str]:
    expected = striptable(expected)
    if not context:
        context = Context('empty')
    if expected:
        header = expected.splitlines()[0]
        cols = normalizes_columns(header.split('|'))
        sizes = {c: len(x) - 2 for c, x in zip(cols, f' {header} '.split('|'))}
        actual = render_tabular_manifest(context, manifest, cols, sizes=sizes)
    else:
        actual = render_tabular_manifest(context, manifest)
    return actual, expected


def _create_file_path_for_type(tmp_path: pathlib.Path, file_name: str, manifest_type: str):
    if manifest_type != 'internal_sql' and tmp_path is None:
        raise Exception(f"TMP_PATH IS REQUIRED FOR {manifest_type} MANIFEST")

    if manifest_type == 'internal_sql':
        return 'sqlite:///' + str(tmp_path / f'{file_name}.sqlite')
    elif manifest_type in ['csv', 'xml', 'xlsx', 'yaml']:
        return str(tmp_path / f'{file_name}.{manifest_type}')
    elif manifest_type in ['ascii', 'tabular']:
        return str(tmp_path / f'{file_name}.txt')
    else:
        raise Exception(f"NO SUPPORT FOR {manifest_type} MANIFEST")


def load_manifest_get_context(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str] = None,
    *,
    load_internal: bool = False,
    request: FixtureRequest = None,
    manifest_type: str = '',
    tmp_path: pathlib.Path = None,
    full_load: bool = True,
    ensure_config_dir: bool = False,
    wipe_data: bool = True,
    **kwargs,
) -> TestContext:
    temp_rc = configure_rc(rc, None, **kwargs)
    context = create_test_context(temp_rc, wipe_data=wipe_data)

    if isinstance(manifest, str) and '|' in manifest:
        if manifest_type and manifest_type != 'ascii':
            ascii_file = _create_file_path_for_type(tmp_path, '_temp_ascii_manifest', 'ascii')
            output_file = _create_file_path_for_type(tmp_path, '_temp_manifest', manifest_type)
            with open(ascii_file, 'w') as f:
                f.write(manifest)
            copy_manifest(
                context,
                manifests=[ascii_file],
                output=output_file,
                output_type=manifest_type
            )
            manifests = [output_file]
        else:
            manifests = [ManifestPath(
                type='ascii',
                file=StringIO(manifest),
            )]
    elif manifest:
        if isinstance(manifest, pathlib.Path):
            manifest = str(manifest)
        manifest_ = detect_manifest_from_path(rc, manifest)
        if manifest_type and manifest_.type != manifest_type:
            output_path = _create_file_path_for_type(tmp_path, '_temp_manifest', manifest_type)
            copy_manifest(
                context,
                manifests=[manifest],
                output=output_path,
                output_type=manifest_type
            )
            manifest = output_path
        manifests = [manifest]
    else:
        manifests = manifest
    manifest_type = manifest_type if manifest_type != 'internal_sql' else 'internal'
    rc = configure_rc(rc, manifests, manifest_type='inline' if manifest_type != 'internal' else manifest_type, **kwargs)
    context = create_test_context(rc, request, wipe_data=wipe_data)
    store = load_store(context, verbose=False, ensure_config_dir=ensure_config_dir)
    commands.load(context, store.manifest, load_internal=load_internal, full_load=full_load)
    commands.link(context, store.manifest)
    return context


def load_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    **kwargs,
) -> Manifest:
    context = load_manifest_get_context(rc, manifest, **kwargs)
    store: Store = context.get('store')
    return store.manifest


def load_manifest_and_context(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str] = None,
    **kwargs,
) -> Tuple[Context, Manifest]:
    context = load_manifest_get_context(rc, manifest, **kwargs)
    store: Store = context.get('store')
    return context, store.manifest


def prepare_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    *,
    request: FixtureRequest = None,
    load_internal: bool = True,
    **kwargs,
) -> Tuple[TestContext, Manifest]:
    context = load_manifest_get_context(
        rc, manifest,
        request=request,
        load_internal=load_internal,
        **kwargs,
    )
    store: Store = context.get('store')
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    return context, store.manifest


def bootstrap_manifest(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str],
    *,
    request: FixtureRequest = None,
    load_internal: bool = True,
    full_load: bool = True,
    wipe_data: bool = True,
    **kwargs,
) -> TestContext:
    context = load_manifest_get_context(
        rc, manifest,
        request=request,
        load_internal=load_internal,
        full_load=full_load,
        wipe_data=wipe_data,
        **kwargs,
    )
    store: Store = context.get('store')
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    commands.bootstrap(context, store.manifest)
    context.loaded = True
    return context
