from spinta.core.config import RawConfig
from spinta.testing.client import create_test_client
from spinta.testing.client import get_html_tree
from spinta.testing.tabular import convert_ascii_manifest_to_csv
from spinta.testing.utils import error


def test_success(rc: RawConfig):
    app = create_test_client(rc, raise_server_exceptions=False)
    app.authmodel("", ["check"])
    csv_manifest = convert_ascii_manifest_to_csv("""
    id | d | r | b | m | property | type
       | datasets/gov/example     |
       |   |   |   | Country      |
       |   |   |   |   | name     | string
    """)
    resp = app.post(
        "/:check",
        files={
            "manifest": ("manifest.csv", csv_manifest, "text/csv"),
        },
    )
    assert resp.json() == {"status": "OK"}


def test_unknown_field(rc: RawConfig):
    app = create_test_client(rc, raise_server_exceptions=False)
    app.authmodel("", ["check"])
    csv_manifest = convert_ascii_manifest_to_csv("""
    id | d | r | b | m | property | typo    | ref
       | datasets/gov/example     |         |
       |   |   |   | Country      |         |
       |   |   |   |   | name     | string  |
    """)
    resp = app.post(
        "/:check",
        files={
            "manifest": ("manifest.csv", csv_manifest, "text/csv"),
        },
    )
    assert resp.json() == {
        "errors": [
            {
                "code": "TabularManifestError",
                "message": "manifest.csv:1: Unknown columns: typo.",
            },
        ]
    }


def test_strip_unknown_field(rc: RawConfig):
    app = create_test_client(rc, raise_server_exceptions=False)
    app.authmodel("", ["check"])
    csv_manifest = convert_ascii_manifest_to_csv("""
    id | d | r | b | m | property | typo | type
       | datasets/gov/example     |      |
       |   |   |   | Country      |      |
       |   |   |   |   | name     |      | string
    """)
    resp = app.post(
        "/:check",
        files={
            "manifest": ("manifest.csv", csv_manifest, "text/csv"),
        },
    )
    assert resp.json() == {
        "errors": [
            {
                "code": "TabularManifestError",
                "message": "manifest.csv:1: Unknown columns: typo.",
            },
        ]
    }


def test_unknown_type(rc: RawConfig):
    app = create_test_client(rc, raise_server_exceptions=False)
    app.authmodel("", ["check"])
    csv_manifest = convert_ascii_manifest_to_csv("""
    id | d | r | b | m | property | type
       | datasets/gov/example     |
       |   |   |   | Country      |
       |   |   |   |   | name     | stringz
    """)
    resp = app.post(
        "/:check",
        files={
            "manifest": ("manifest.csv", csv_manifest, "text/csv"),
        },
    )
    assert error(resp, ["component", "error"]) == {
        "context": {
            "component": "spinta.components.Property",
            "error": ("Unknown 'stringz' type of 'name' property in 'datasets/gov/example/Country' model."),
        },
    }


def test_html_ui(rc: RawConfig):
    app = create_test_client(rc)
    app.authmodel("", ["check"])
    resp = app.get("/:check", headers={"accept": "text/html"})
    html = get_html_tree(resp)
    assert len(html.forms) == 1

    form = html.forms[0]
    assert form.attrib["name"] == "check"
    assert dict(form.fields) == {
        "manifest": None,
    }
    assert form.inputs["manifest"].type == "file"
    assert list(form.xpath('.//input[@type="submit"]/@value')) == ["Tikrinti"]
