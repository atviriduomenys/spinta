import pathlib

from responses import GET


def test_xlsx(store, responses):
    responses.add(
        GET, 'http://example.com/data.xlsx',
        status=200, content_type='application/vnd.ms-excel',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.xlsx').read_bytes(),
    )

    assert len(store.pull('xlsx')) > 0
    assert list(store.getall('rinkimai', {'source': 'xlsx'})) == [
        {
            'id': '2e12c8b9c3e6028b6712fe308876f0321578db4c',
            'data': '1992-10-25T00:00:00',
            'rusis': 'Seimo rinkimai',
            'pavadinimas': '1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai',
        },
    ]
    assert list(store.getall('rinkimai/turas', {'source': 'xlsx'})) == [
        {
            'id': '1c89290ef09005b332e84dcaba7873951c19216f',
            'turas': 1,
        },
    ]
    assert list(store.getall('rinkimai/apygarda', {'source': 'xlsx'})) == [
        {
            'id': 'cad571c819cc351a70bf3d3ab3594c70e43ac75d',
            'numeris': 2,
            'pavadinimas': 'Senamiesčio',
        },
    ]
    assert list(store.getall('rinkimai/apylinke', {'source': 'xlsx'})) == [
        {
            'id': 'b131042cce400ef19ad7fc7c66c66370fc7fedfb',
            'numeris': 0,
            'pavadinimas': 'Balsai, suskaičiuoti apygardos rinkimų komisijoje',
        },
    ]
    assert list(store.getall('rinkimai/kandidatas', {'source': 'xlsx'})) == [
        {
            'id': '8159cf47118c31114c71a75ed06aa66b0476ad7a',
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
