from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest


@pytest.mark.models(
    'backends/postgres/report',
    'backends/mongo/report',
)
def test_concurency(model, app):
    app.authmodel(model, ["insert", "getone"])

    data = [
        {"count": 1, "status": "first"},
        {"count": 2, "status": "second"},
        {"count": 3, "status": "third"},
        {"count": 4, "status": "fourth"},
        {"count": 5, "status": "fifth"},
    ]

    max_workers = len(data)

    with ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(app.post, f"/{model}", json=d): d["count"]
            for d in data
        }

    responses = {}
    for future in as_completed(futures):
        resp = future.result()
        count = futures[future]
        responses[count] = resp

    status_codes = {count: r.status_code for count, r in responses.items()}
    assert status_codes == {
        1: 201,
        2: 201,
        3: 201,
        4: 201,
        5: 201,
    }

    ids = {count: r.json()['_id'] for count, r in responses.items()}
    with ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(app.get, f'/{model}/{id_}'): count
            for count, id_ in ids.items()
        }

    responses = {}
    for future in as_completed(futures):
        resp = future.result()
        count = futures[future]
        responses[count] = resp

    status_codes = {count: r.status_code for count, r in responses.items()}
    assert status_codes == {
        1: 200,
        2: 200,
        3: 200,
        4: 200,
        5: 200,
    }
