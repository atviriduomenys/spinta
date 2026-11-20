from pathlib import Path

from spinta import commands
from spinta.cli.helpers.store import load_store
from spinta.testing.context import create_test_context


def _write(tmp_path: Path, name: str, prefix: str, model: str) -> str:
    # Each file uses its own namespace, otherwise all models would end up in
    # the same dataset.
    path = tmp_path / f"{name}.ttl"
    path.write_text(
        f"""
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix {prefix}: <http://example.com/{prefix}#> .

{prefix}:{model}Shape a sh:NodeShape ;
    sh:targetClass {prefix}:{model} ;
    sh:property [ sh:path {prefix}:title ; sh:datatype xsd:string ] .
""",
        encoding="utf-8",
    )
    return str(path)


def test_nested_sync_sources_are_all_loaded(rc, tmp_path):
    rdf1 = _write(tmp_path, "rdf1", "ns1", "A")
    rdf2 = _write(tmp_path, "rdf2", "ns2", "B")
    rdf3 = _write(tmp_path, "rdf3", "ns3", "C")
    rc = rc.fork(
        {
            "manifests.default": {"type": "rdf", "path": rdf1, "sync": ["rdf2"]},
            "manifests.rdf2": {"type": "rdf", "path": rdf2, "sync": ["rdf3"]},
            "manifests.rdf3": {"type": "rdf", "path": rdf3},
        }
    )
    context = create_test_context(rc)
    store = load_store(context, verbose=False)
    commands.load(context, store.manifest, load_internal=False)
    models = sorted(commands.get_models(context, store.manifest))
    assert models == ["ns1/A", "ns2/B", "ns3/C"]
