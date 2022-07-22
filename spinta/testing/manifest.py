import pathlib
from io import StringIO
from typing import Tuple
from typing import Union

from pytest import FixtureRequest
from py._path.local import LocalPath

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.components import Context
from spinta.components import Store
from spinta.core.config import RawConfig
from spinta.core.config import configure_rc
from spinta.manifests.components import Manifest
from spinta.manifests.components import ManifestPath
from spinta.manifests.tabular.helpers import normalizes_columns
from spinta.manifests.tabular.helpers import render_tabular_manifest
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import TestContext
from spinta.testing.context import create_test_context


def compare_manifest(manifest: Manifest, expected: str) -> Tuple[str, str]:
    expected = striptable(expected)
    if expected:
        header = expected.splitlines()[0]
        cols = normalizes_columns(header.split('|'))
        sizes = {c: len(x) - 2 for c, x in zip(cols, f' {header} '.split('|'))}
        actual = render_tabular_manifest(manifest, cols, sizes=sizes)
    else:
        actual = render_tabular_manifest(manifest)
    return actual, expected


def load_manifest_get_context(
    rc: RawConfig,
    manifest: Union[pathlib.Path, str] = None,
    *,
    load_internal: bool = False,
    request: FixtureRequest = None,
    **kwargs,
) -> TestContext:
    if isinstance(manifest, (pathlib.Path, LocalPath)):
        manifests = [str(manifest)]
    elif isinstance(manifest, str) and '|' in manifest:
        manifests = [ManifestPath(
            type='ascii',
            file=StringIO(manifest),
        )]
    elif manifest:
        manifests = [manifest]
    else:
        manifests = manifest

    rc = configure_rc(rc, manifests, **kwargs)
    context = create_test_context(rc, request)
    store = load_store(context, verbose=False, ensure_config_dir=False)
    commands.load(context, store.manifest, load_internal=load_internal)
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
    **kwargs,
) -> TestContext:
    context = load_manifest_get_context(
        rc, manifest,
        request=request,
        load_internal=load_internal,
        **kwargs,
    )
    store: Store = context.get('store')
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    commands.bootstrap(context, store.manifest)
    context.loaded = True
    return context
