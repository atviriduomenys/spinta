import pathlib

from responses import GET


def test_xlsx(store, responses):
    responses.add(
        GET, 'http://example.com/data.xlsx',
        status=200, content_type='application/vnd.ms-excel',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.xlsx').read_bytes(),
    )

    assert len(store.pull('xlsx')) > 0
    assert list(store.getall('rinkimai/:source/xlsx')) == [
        {
            'id': ['1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai'],
            'data': '1992-10-25T00:00:00',
            'rusis': 'Seimo rinkimai',
            'pavadinimas': '1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai',
        },
    ]
    assert list(store.getall('turas/:source/xlsx')) == [
        {
            'id': ['1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai', 1],
            'turas': 1,
        },
    ]
    assert list(store.getall('apygarda/:source/xlsx')) == [
        {
            'id': ['1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai', 1, 2],
            'numeris': 2,
            'pavadinimas': 'Senamiesčio',
        },
    ]
    assert list(store.getall('apylinke/:source/xlsx')) == [
        {
            'id': ['1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai', 1, 2, 0],
            'numeris': 0,
            'pavadinimas': 'Balsai, suskaičiuoti apygardos rinkimų komisijoje',
        },
        {
            'id': ['1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai', 1, 2, 3],
            'numeris': 3,
            'pavadinimas': 'Apylinkė Nr. 3',
        },
    ]
    assert list(store.getall('kandidatas/:source/xlsx')) == [
        {
            'id': ['1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai', 1, 2, 0, 'NIJOLĖ', 'VAITIEKŪNIENĖ', '1954-03-31T00:00:00'],
            'vardas': 'NIJOLĖ',
            'pavarde': 'VAITIEKŪNIENĖ',
            'lytis': 'Moteris',
            'tautybe': '',
            'kas_iskele_kandidata': 'Lietuvos Respublikos piliečių chartija',
            'gauti_balsai_is_anksto': 243,
            'gauti_balsai_is_viso': 243,
            'gauti_balsai_rinkimu_diena': 0,
            'gimimo_data': '1954-03-31T00:00:00',
        },
    ]
