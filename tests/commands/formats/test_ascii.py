import datetime

import pytest


def test_export_ascii(context, app, mocker):
    mocker.patch('spinta.backends.postgresql.dataset.utcnow', return_value=datetime.datetime(2019, 3, 6, 16, 15, 0, 816308))
    mocker.patch('spinta.backends.postgresql.dataset.get_new_id', return_value='REV')

    context.push([
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '1',
            'code': 'lt',
            'title': 'Lithuania',
        },
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '2',
            'code': 'lv',
            'title': 'LATVIA',
        },
        {
            'type': 'country/:dataset/csv/:resource/countries',
            'id': '2',
            'code': 'lv',
            'title': 'Latvia',
        },
    ])

    app.authorize([
        'spinta_country_dataset_csv_resource_countries_getall',
        'spinta_country_dataset_csv_resource_countries_search',
        'spinta_country_dataset_csv_resource_countries_changes',
    ])

    assert app.get('country/:dataset/csv/:resource/countries/:sort/code/:format/ascii').text == (
        '\n\n'
        'Table: country/:dataset/csv/:resource/countries\n'
        'id   code     title     revision\n'
        '================================\n'
        '1    lt     Lithuania   REV     \n'
        '2    lv     Latvia      REV     '
    )

    changes = context.changes('country', dataset='csv', resource='countries')
    ids = [c['change_id'] for c in changes]
    txn = [c['transaction_id'] for c in changes]
    assert app.get('country/:dataset/csv/:resource/countries/:changes/:format/ascii').text == (
        'change_id   transaction_id   id            datetime            action   change.code   change.title\n'
        '==================================================================================================\n'
        f'{ids[0]:<12}{txn[0]:<13}    1    2019-03-06T16:15:00.816308   insert   lt            Lithuania   \n'
        f'{ids[1]:<12}{txn[1]:<13}    2    2019-03-06T16:15:00.816308   insert   lv            LATVIA      \n'
        f'{ids[2]:<12}{txn[2]:<13}    2    2019-03-06T16:15:00.816308   patch    None          Latvia      '
    )


@pytest.mark.asyncio
async def test_export_multiple_types(context):
    rows = [
        {'type': 'a', 'value': 1},
        {'type': 'a', 'value': 2},
        {'type': 'a', 'value': 3},
        {'type': 'b', 'value': 1},
        {'type': 'b', 'value': 2},
        {'type': 'b', 'value': 3},
        {'type': 'c', 'value': 1},
        {'type': 'c', 'value': 2},
        {'type': 'c', 'value': 3},
    ]

    context.load()
    config = context.get('config')
    exporter = config.exporters['ascii']
    assert ''.join(exporter(rows)) == (
        '\n\n'
        'Table: a\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    \n'
        '\n'
        'Table: b\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    \n'
        '\n'
        'Table: c\n'
        'value\n'
        '=====\n'
        '1    \n'
        '2    \n'
        '3    '
    )
