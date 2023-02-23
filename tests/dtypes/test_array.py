import pytest

from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.manifest import load_manifest
from spinta.testing.manifest import bootstrap_manifest

def test_prop_array(tmp_path, rc):
    table = '''
        d | r | b | m | property    | type    | ref      | access
        example                     |         |          |
                                    |         |          |
          |   |   | Language        |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | countries[] | backref | Country  | open
                                    |         |          |
          |   |   | Country         |         |          |
          |   |   |   | name        | string  |          | open
          |   |   |   | languages[] | ref     | Language | open
    '''
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == table

def test_prop_array_with_custom(rc, tmp_path):
    table = '''
        d | r | b | m | property    | type                                       | ref
        example                     |                                            |
                                    |                                            |
          |   |   | Language        |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | countries[] | backref                                    | Country
                                    |                                            |
          |   |   | Country         |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | languages   | array                                      | CountryLanguage[country, language]
          |   |   |   | languages[] | ref                                        | Language
                                    |                                            |
          |   |   | CountryLanguage |                                            |
          |   |   |   | language    | ref                                        | Language
          |   |   |   | country     | ref                                        | Country
    '''
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == table


def test_prop_array_with_custom_without_properties(rc, tmp_path):
    table = '''
        d | r | b | m | property    | type                                       | ref
        example                     |                                            |
                                    |                                            |
          |   |   | Language        |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | countries[] | backref                                    | Country
                                    |                                            |
          |   |   | Country         |                                            |
          |   |   |   | name        | string                                     |
          |   |   |   | languages   | array                                      | CountryLanguage
          |   |   |   | languages[] | ref                                        | Language
                                    |                                            |
          |   |   | CountryLanguage |                                            |
          |   |   |   | language    | ref                                        | Language
          |   |   |   | country     | ref                                        | Country
    '''
    create_tabular_manifest(tmp_path / 'manifest.csv', table)
    manifest = load_manifest(rc, tmp_path / 'manifest.csv')
    assert manifest == table
