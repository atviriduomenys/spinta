from typer import Argument
from typer import Context as TyperContext
from typer import Option
from typer import echo

from spinta.cli.helpers.enums import CommentPart
from spinta.cli.helpers.store import load_manifest
from spinta.components import Context, Property
from spinta.core.context import configure_context
from spinta.core.enums import Access
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.helpers import datasets_to_tabular
from spinta.manifests.tabular.helpers import render_tabular_manifest_rows
from spinta.manifests.tabular.helpers import write_tabular_manifest


def comment(
    ctx: TyperContext,
    part: CommentPart = Argument(None, help="Part to comment"),
    author: str | None = Option(None, "--author", help="Comment author"),
    uri: str | None = Option(
        None, "--uri", help="URI tag stored on each comment (use with uncomment --uri to restore selectively)"
    ),
    description: str | None = Option(None, "--description", help="Additional description stored on each comment"),
    output: str | None = Option(None, "-o", "--output", help="Output tabular manifest in a specified file"),
    manifests: list[str] = Argument(None, help="Source manifest files"),
) -> None:
    """Comment out specific manifest parts so the manifest can be processed further."""
    context: Context = ctx.obj
    context = configure_context(context, manifests)

    comment_manifest(
        context,
        author=author,
        uri=uri,
        description=description,
        output=output,
        manifests=manifests,
    )


def comment_manifest(
    context: Context,
    author: str | None = None,
    uri: str | None = None,
    description: str | None = None,
    output: str | None = None,
    manifests: list[str] | None = None,
) -> None:
    verbose = bool(output)
    context = configure_context(context, manifests)
    store = load_manifest(
        context,
        load_internal=False,
        verbose=verbose,
        full_load=True,
    )

    _update_systemic_comments(context, store.manifest, author=author, uri=uri, description=description)
    rows = datasets_to_tabular(context, store.manifest, external=False, access=Access.private)

    if output:
        write_tabular_manifest(context, output, rows)
    else:
        manager = context.get("error_manager")
        handler = manager.handler
        table = render_tabular_manifest_rows(rows)
        if handler.get_counts():
            handler.post_process()
        else:
            echo(table)


def _update_systemic_comments(
    context: Context,
    manifest: Manifest,
    *,
    author: str | None = None,
    uri: str | None = None,
    description: str | None = None,
) -> None:
    """Update author/uri/description on comment type properties."""
    from spinta import commands

    if all(argument is None for argument in [author, uri, description]):
        return

    models = commands.get_models(context, manifest)
    for model in models.values():
        for prop in model.properties.values():
            _patch_property_restore_comments(prop, author=author, uri=uri, description=description)


def _patch_property_restore_comments(
    prop: Property,
    *,
    author: str | None,
    uri: str | None,
    description: str | None,
) -> None:
    if not prop.comments:
        return

    for comment in prop.comments:
        if not comment.prepare or not comment.prepare.startswith("update"):
            continue

        if author is not None:
            comment.author = author
        if uri is not None:
            comment.uri = uri
        if description is not None:
            comment.comment = description
