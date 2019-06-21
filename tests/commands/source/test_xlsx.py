import pathlib

from responses import GET


def test_xlsx(context, responses, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    responses.add(
        GET, 'http://example.com/data.xlsx',
        status=200, content_type='application/vnd.ms-excel',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.xlsx').read_bytes(),
    )

    rinkimai = '2e12c8b9c3e6028b6712fe308876f0321578db4c'
    turas = '1c89290ef09005b332e84dcaba7873951c19216f'
    apygarda = '025685077bbcf6e434a95b65b9a6f5fcef046861'
    apylinke = '629f0976c1a04dbe6cf3e71b3085ec555d3f63bf'

    assert len(context.pull('xlsx')) > 0
    assert list(context.getall('rinkimai', dataset='xlsx', resource='data')) == [
        {
            'type': 'rinkimai/:ds/xlsx/:rs/data',
            'id': rinkimai,
            'revision': 'REV',
            'data': '1992-10-25T00:00:00',
            'rusis': 'Seimo rinkimai',
            'pavadinimas': '1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai',
        },
    ]
    assert list(context.getall('rinkimai/turas', dataset='xlsx', resource='data')) == [
        {
            'type': 'rinkimai/turas/:ds/xlsx/:rs/data',
            'id': turas,
            'revision': 'REV',
            'turas': 1,
            'rinkimai': rinkimai,
        },
    ]
    assert list(context.getall('rinkimai/apygarda', dataset='xlsx', resource='data')) == [
        {
            'type': 'rinkimai/apygarda/:ds/xlsx/:rs/data',
            'id': apygarda,
            'revision': 'REV',
            'numeris': 2,
            'pavadinimas': 'Senamiesčio',
            'rinkimai': rinkimai,
            'turas': turas,
        },
    ]
    assert list(context.getall('rinkimai/apylinke', dataset='xlsx', resource='data')) == [
        {
            'type': 'rinkimai/apylinke/:ds/xlsx/:rs/data',
            'id': apylinke,
            'revision': 'REV',
            'numeris': 0,
            'pavadinimas': 'Balsai, suskaičiuoti apygardos rinkimų komisijoje',
            'rinkimai': rinkimai,
            'turas': turas,
            'apygarda': apygarda,
        },
    ]
    assert list(context.getall('rinkimai/kandidatas', dataset='xlsx', resource='data')) == [
        {
            'type': 'rinkimai/kandidatas/:ds/xlsx/:rs/data',
            'id': '8159cf47118c31114c71a75ed06aa66b0476ad7a',
            'revision': 'REV',
            'vardas': 'NIJOLĖ',
            'pavarde': 'VAITIEKŪNIENĖ',
            'lytis': 'Moteris',
            'tautybe': '',
            'kas_iskele_kandidata': 'Lietuvos Respublikos piliečių chartija',
            'gauti_balsai_is_anksto': 243,
            'gauti_balsai_is_viso': 243,
            'gauti_balsai_rinkimu_diena': 0,
            'gimimo_data': '1954-03-31T00:00:00',
            'rinkimai': rinkimai,
            'turas': turas,
            'apygarda': apygarda,
            'apylinke': apylinke,
        },
    ]
