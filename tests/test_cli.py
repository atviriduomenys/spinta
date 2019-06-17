from responses import GET

from spinta.cli import main


def test_pull(responses, cli, context):
    responses.add(
        GET, 'http://example.com/countries.csv',
        status=200, content_type='text/plain; charset=utf-8',
        body=(
            'kodas,šalis\n'
            'lt,Lietuva\n'
            'lv,Latvija\n'
            'ee,Estija'
        ),
    )

    result = cli.invoke(main, ['pull', 'csv'], catch_exceptions=False)
    assert result.output == (
        'http://example.com/ countries.csv {}\n'
        '\n'
        '\n'
        'Table: country/:ds/csv/:rs/countries\n'
        '                   id                      code    title \n'
        '=========================================================\n'
        '552c4c243ec8c98c313255ea9bf16ee286591f8c   lt     Lietuva\n'
        'b5dcb86880816fb966cdfbbacd1f3406739464f4   lv     Latvija\n'
        '68de1c04d49aeefabb7081a5baf81c055f235be3   ee     Estija '
    )

    assert context.getall('country', dataset='csv', resource='countries') == []

    result = cli.invoke(main, ['pull', 'csv', '--push'], catch_exceptions=False)
    assert 'csv:' in result.output

    rows = sorted(
        (row['code'], row['title'], row['type'])
        for row in context.getall('country', dataset='csv', resource='countries')
    )
    assert rows == [
        ('ee', 'Estija', 'country/:ds/csv/:rs/countries'),
        ('lt', 'Lietuva', 'country/:ds/csv/:rs/countries'),
        ('lv', 'Latvija', 'country/:ds/csv/:rs/countries'),
    ]
