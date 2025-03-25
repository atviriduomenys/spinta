from spinta.testing.cli import SpintaCliRunner
from spinta.manifests.tabular.helpers import striptable
from spinta.testing.tabular import create_tabular_manifest


def yaml_content():
    return """
        - _type: datasets/gov/example/City
          _id: ca117a8a-fdd0-4c66-9565-093d4d493e6b
          name: Vilnius
          country:
            _id: dac60010-9a29-4201-8da9-e624023eb626
            name: Lietuva
        - _type: datasets/gov/example/Country
          _id: dac60010-9a29-4201-8da9-e624023eb626
          name: Lietuva
    """


def test_getall(context, rc, cli: SpintaCliRunner, tmp_path):
    manifest = striptable('''
     d | r | b | m  | property         | type    | ref     | source     | access
     datasets/gov/example              |         |         |            |
       | data                          | memory  |         |            |
       |   |                           |         |         |            |
       |   |   | Country               |         | id      |            | open
       |   |   |    | id               | integer |         |            |
       |   |   |    | name             | string  |         |            |
       |   |   |    |                  |         |         |            |
       |   |   | City                  |         | id      |            | open
       |   |   |    | id               | integer |         |            |
       |   |   |    | name             | string  |         |            |
       |   |   |    | country          | ref     | Country |            |
        """
    ''')
    temp_yaml_file = tmp_path / "test_config.yaml"
    temp_manifest_file = tmp_path / "manifest.csv"

    with temp_yaml_file.open("w") as file:
        file.write(yaml_content())

    create_tabular_manifest(context, temp_manifest_file, manifest)

    result = cli.invoke(
        rc, ["getall", temp_manifest_file, temp_yaml_file, "datasets/gov/example/City"]
    )

    assert (
        striptable(result.stdout)
        == "{'_data': [{'_type': 'datasets/gov/example/City', '_id': 'ca117a8a-fdd0-4c66-9565-093d4d493e6b', 'name': 'Vilnius', 'country': {'_id': 'dac60010-9a29-4201-8da9-e624023eb626', 'name': 'Lietuva'}}]}"
    )


def test_getone(context, rc, cli: SpintaCliRunner, tmp_path):
    manifest = striptable('''
     d | r | b | m  | property         | type    | ref     | source     | access
     datasets/gov/example              |         |         |            |
       | data                          | memory  |         |            |
       |   |                           |         |         |            |
       |   |   | Country               |         | id      |            | open
       |   |   |    | id               | integer |         |            |
       |   |   |    | name             | string  |         |            |
       |   |   |    |                  |         |         |            |
       |   |   | City                  |         | id      |            | open
       |   |   |    | id               | integer |         |            |
       |   |   |    | name             | string  |         |            |
       |   |   |    | country          | ref     | Country |            |
        """
    ''')
    temp_yaml_file = tmp_path / "test_config.yaml"
    temp_manifest_file = tmp_path / "manifest.csv"

    with temp_yaml_file.open("w") as file:
        file.write(yaml_content())

    create_tabular_manifest(context, temp_manifest_file, manifest)

    result = cli.invoke(
        rc,
        [
            "getone",
            temp_manifest_file,
            temp_yaml_file,
            "datasets/gov/example/City",
            "ca117a8a-fdd0-4c66-9565-093d4d493e6b",
        ],
    )

    assert (
        striptable(result.stdout)
        == "{'_type': 'datasets/gov/example/City', '_id': 'ca117a8a-fdd0-4c66-9565-093d4d493e6b', 'name': 'Vilnius', 'country': {'_id': 'dac60010-9a29-4201-8da9-e624023eb626', 'name': 'Lietuva'}}"
    )
