from concurrent.futures import ThreadPoolExecutor, as_completed


def test_concurency(context, app):
    app.authorize([
        'spinta_country_insert',
        'spinta_country_getone',
    ])

    data = [
        {'code': 'fi', 'title': "Finland"},
        {'code': 'ee', 'title': "Estonia"},
        {'code': 'lv', 'title': "Latvia"},
        {'code': 'lt', 'title': "Lithuania"},
        {'code': 'pl', 'title': "Poland"},
    ]

    max_workers = len(data)

    with ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(app.post, '/country', json=d): d['code']
            for d in data
        }

    responses = {}
    for future in as_completed(futures):
        resp = future.result()
        code = futures[future]
        responses[code] = resp

    status_codes = {code: r.status_code for code, r in responses.items()}
    assert status_codes == {
        'fi': 201,
        'ee': 201,
        'lv': 201,
        'lt': 201,
        'pl': 201,
    }

    ids = {code: r.json()['id'] for code, r in responses.items()}
    with ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(app.get, f'/country/{id_}'): code
            for code, id_ in ids.items()
        }

    responses = {}
    for future in as_completed(futures):
        resp = future.result()
        code = futures[future]
        responses[code] = resp

    status_codes = {code: r.status_code for code, r in responses.items()}
    assert status_codes == {
        'fi': 200,
        'ee': 200,
        'lv': 200,
        'lt': 200,
        'pl': 200,
    }
