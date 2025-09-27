from operator import itemgetter

import pytest

from responses import GET

from spinta.testing.datasets import pull


@pytest.mark.skip("datasets")
def test_csv(rc, cli, app, responses):
    responses.add(
        GET,
        "http://example.com/countries.csv",
        status=200,
        content_type="text/plain; charset=utf-8",
        body=("kodas,šalis\nlt,Lietuva\nlv,Latvija\nee,Estija"),
    )

    assert len(pull(cli, rc, "csv")) == 3
    assert len(pull(cli, rc, "csv")) == 0

    app.authmodel("country", ["getall"])
    app.authmodel("country/:dataset/csv/:resource/countries", ["getall"])

    assert sorted([(x["code"], x["title"]) for x in app.get("/country").json()["_data"]]) == []
    assert sorted(
        [(x["code"], x["title"]) for x in app.get("/country/:dataset/csv/:resource/countries").json()["_data"]]
    ) == [
        ("ee", "Estija"),
        ("lt", "Lietuva"),
        ("lv", "Latvija"),
    ]


@pytest.mark.skip("datasets")
def test_denorm(rc, cli, app, responses):
    responses.add(
        GET,
        "http://example.com/orgs.csv",
        status=200,
        content_type="text/plain; charset=utf-8",
        body=("govid,org,kodas,šalis\n1,Org1,lt,Lietuva\n2,Org2,lt,Lietuva\n3,Org3,lv,Latvija"),
    )

    assert len(pull(cli, rc, "denorm")) == 5
    assert len(pull(cli, rc, "denorm")) == 0

    lt = "552c4c243ec8c98c313255ea9bf16ee286591f8c"
    lv = "b5dcb86880816fb966cdfbbacd1f3406739464f4"

    app.authmodel("org", ["getall"])
    app.authmodel("org/:dataset/denorm/:resource/orgs", ["getall"])
    app.authmodel("country", ["getall"])
    app.authmodel("country/:dataset/denorm/:resource/orgs", ["getall"])

    assert app.get("country").json()["_data"] == []
    assert sorted(
        [(x["_id"], x["title"]) for x in app.get("country/:dataset/denorm/:resource/orgs").json()["_data"]]
    ) == [
        (lt, "Lietuva"),
        (lv, "Latvija"),
    ]

    assert app.get("/org").json()["_data"] == []
    assert sorted(
        [
            (x["_id"], x["title"], x["country"]["_id"])
            for x in app.get("/org/:dataset/denorm/:resource/orgs").json()["_data"]
        ],
        key=itemgetter(1),
    ) == [
        ("23fcdb953846e7c709d2967fb549de67d975c010", "Org1", lt),
        ("6f9f652eb6dae29e4406f1737dd6043af6142090", "Org2", lt),
        ("11a0764da48b674ce0c09982e7c43002b510d5b5", "Org3", lv),
    ]
