import pathlib

from responses import GET


def test_xlsx(app, context, responses, mocker):
    responses.add(
        GET, 'http://example.com/data.xlsx',
        status=200, content_type='application/vnd.ms-excel',
        body=(pathlib.Path(__file__).parents[2] / 'data/data.xlsx').read_bytes(),
    )

    rinkimai = '2e12c8b9c3e6028b6712fe308876f0321578db4c'
    turas = '1c89290ef09005b332e84dcaba7873951c19216f'
    apygarda = '025685077bbcf6e434a95b65b9a6f5fcef046861'
    apylinke = '629f0976c1a04dbe6cf3e71b3085ec555d3f63bf'
    kandidatas = '8159cf47118c31114c71a75ed06aa66b0476ad7a'

    app.authmodel('rinkimai/:dataset/xlsx/:resource/data', ['getall'])
    app.authmodel('rinkimai/turas/:dataset/xlsx/:resource/data', ['getall'])
    app.authmodel('rinkimai/apygarda/:dataset/xlsx/:resource/data', ['getall'])
    app.authmodel('rinkimai/apylinke/:dataset/xlsx/:resource/data', ['getall'])
    app.authmodel('rinkimai/kandidatas/:dataset/xlsx/:resource/data', ['getall'])

    data = context.pull('xlsx')
    assert len(data) > 0
    data = {d['_id']: d for d in data}
    assert app.getdata('/rinkimai/:dataset/xlsx/:resource/data') == [
        {
            '_type': 'rinkimai/:dataset/xlsx/:resource/data',
            '_id': rinkimai,
            '_revision': data[rinkimai]['_revision'],
            'data': '1992-10-25T00:00:00',
            'rusis': 'Seimo rinkimai',
            'pavadinimas': '1992 m. spalio 25 d. Lietuvos Respublikos Seimo rinkimai',
        },
    ]
    assert app.getdata('/rinkimai/turas/:dataset/xlsx/:resource/data') == [
        {
            '_type': 'rinkimai/turas/:dataset/xlsx/:resource/data',
            '_id': turas,
            '_revision': data[turas]['_revision'],
            'turas': 1,
            'rinkimai': rinkimai,
        },
    ]
    assert app.getdata('/rinkimai/apygarda/:dataset/xlsx/:resource/data') == [
        {
            '_type': 'rinkimai/apygarda/:dataset/xlsx/:resource/data',
            '_id': apygarda,
            '_revision': data[apygarda]['_revision'],
            'numeris': 2,
            'pavadinimas': 'Senamiesčio',
            'rinkimai': rinkimai,
            'turas': turas,
        },
    ]
    assert app.getdata('/rinkimai/apylinke/:dataset/xlsx/:resource/data') == [
        {
            '_type': 'rinkimai/apylinke/:dataset/xlsx/:resource/data',
            '_id': apylinke,
            '_revision': data[apylinke]['_revision'],
            'numeris': 0,
            'pavadinimas': 'Balsai, suskaičiuoti apygardos rinkimų komisijoje',
            'rinkimai': rinkimai,
            'turas': turas,
            'apygarda': apygarda,
        },
    ]
    assert app.getdata('/rinkimai/kandidatas/:dataset/xlsx/:resource/data') == [
        {
            '_type': 'rinkimai/kandidatas/:dataset/xlsx/:resource/data',
            '_id': kandidatas,
            '_revision': data[kandidatas]['_revision'],
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
