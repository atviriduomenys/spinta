from spinta.testing.tabular import convert_ascii_manifest_to_csv


def test_convert_ascii_manifest_to_csv():
    assert convert_ascii_manifest_to_csv("""
    id | d | r | b | m | property | type
       | datasets/gov/example     |
       |           | Country      |
       |               | name     | string
    """).decode().splitlines() == [
        "id,dataset,resource,base,model,property,type",
        ",datasets/gov/example,,,,,",
        ",,Country,,,,",
        ",,name,,,,string",
    ]
