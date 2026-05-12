from pathlib import Path

from sqlalchemy.engine.url import make_url

from spinta import commands
from spinta.cli.helpers.store import load_store


from spinta.core.config import RawConfig
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.context import create_test_context
from spinta.testing.pytest import MIGRATION_DATABASE
from spinta.testing.tabular import create_tabular_manifest


def _build_distribute_rc(
    rc: RawConfig,
    path: Path,
    *,
    default_distribution_strategy: str | None = None,
    default_distribution_property: str | None = None,
    model_distribution: dict | None = None,
) -> RawConfig:
    url = make_url(rc.get("backends", "default", "dsn", required=True))
    # Reusing migration database, since it gets recreated each new test
    url = url.set(database=MIGRATION_DATABASE)

    updated_dict = {
        "manifests": {
            "default": {
                "type": "tabular",
                "path": str(path / "manifest.csv"),
                "backend": "default",
                "keymap": "default",
                "mode": "internal",
            },
        },
        "backends": {
            "default": {"type": "postgresql", "dsn": url},
        },
    }
    if default_distribution_strategy is not None:
        updated_dict["default_distribution_strategy"] = default_distribution_strategy
    if default_distribution_property is not None:
        updated_dict["default_distribution_property"] = default_distribution_property
    if model_distribution is not None:
        updated_dict["models"] = model_distribution
    return rc.fork(updated_dict)


def configure_distribute(
    rc: RawConfig,
    manifest: str,
    path: Path,
    *,
    default_distribution_strategy: str | None = None,
    default_distribution_property: str | None = None,
    model_distribution: dict | None = None,
):
    rc = _build_distribute_rc(
        rc,
        path,
        default_distribution_property=default_distribution_property,
        default_distribution_strategy=default_distribution_strategy,
        model_distribution=model_distribution,
    )
    context = create_test_context(rc, name="pytest/cli")
    create_tabular_manifest(context, f"{path}/manifest.csv", striptable(manifest))
    return context, rc


def bootstrap_distribute_manifest(
    rc: RawConfig,
    manifest: str,
    path: Path,
    *,
    default_distribution_strategy: str | None = None,
    default_distribution_property: str | None = None,
    model_distribution: dict | None = None,
):
    context, rc = configure_distribute(
        rc=rc,
        path=path,
        manifest=manifest,
        default_distribution_strategy=default_distribution_strategy,
        default_distribution_property=default_distribution_property,
        model_distribution=model_distribution,
    )
    store = load_store(context, verbose=False)
    commands.load(context, store.manifest)
    commands.link(context, store.manifest)
    commands.check(context, store.manifest)
    commands.prepare(context, store.manifest)
    commands.bootstrap(context, store.manifest)
    context.loaded = True
    return context, rc
