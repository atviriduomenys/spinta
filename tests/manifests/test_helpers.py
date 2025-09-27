from spinta.manifests.helpers import TypeDetector


def test_type_detector_boolean():
    type_detector = TypeDetector()
    given_values = ["true", "false", "0", "1", 0, 1]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "boolean"


def test_type_detector_not_boolean():
    type_detector = TypeDetector()
    given_values = ["true", "false", "0", "1", 0, 1, "2"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "boolean"


def test_type_detector_binary():
    type_detector = TypeDetector()
    given_values = ["0", "1", "010", "110", 110, 10]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "binary"


def test_type_detector_not_binary():
    type_detector = TypeDetector()
    given_values = ["0", "1", "010", "110", 110, 10, 2]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "binary"


def test_type_detector_integer():
    type_detector = TypeDetector()
    given_values = ["0", "1", 2, 3, 4, -50]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "integer"


def test_type_detector_not_integer():
    type_detector = TypeDetector()
    given_values = ["0", "1", 2, 3, 4, -50, "1.1"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "integer"


def test_type_detector_number():
    type_detector = TypeDetector()
    given_values = ["0", "1", 2, 3, 4, -50, "1.1", -5.8]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "number"


def test_type_detector_not_number():
    type_detector = TypeDetector()
    given_values = ["0", "1", 2, 3, 4, -50, "1.1", -5.8, "12:05"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "number"


def test_type_detector_time():
    type_detector = TypeDetector()
    given_values = [
        "12:50",
        "10:05:45",
        "10:15:10.0050",
        "10:15:10.0050+0150",
        "10:15:10+0150",
        "10:15+0150",
    ]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "time"


def test_type_detector_not_time():
    type_detector = TypeDetector()
    given_values = ["12:50", "10:05:45", "10:15:10.005010:15:10.0050+015010:15:10+015010:15+0150", "2020-01-01"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "time"


def test_type_detector_date():
    type_detector = TypeDetector()
    given_values = ["2020-01-01", "2023-04-27"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "date"


def test_type_detector_not_date():
    type_detector = TypeDetector()
    given_values = ["2020-01-01", "2023-04-27", "2023-04-27T12:10:50.5455"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "date"


def test_type_detector_datetime():
    type_detector = TypeDetector()
    given_values = [
        "2023-04-27",
        "2023-04-27T12:10:50.100+01:00",
        "2023-04-27T12:10:50.132263+01:00",
    ]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "datetime"


def test_type_detector_not_datetime():
    type_detector = TypeDetector()
    given_values = [
        "2023-04-27",
        "2023-04-27T12:10:50.100+01:00",
        "2023-04-27T12:10:50.132263+01:00",
        "2023-04-27T12:10:50.132263+0100",
        "INVALID DATETIME",
    ]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "datetime"


def test_type_detector_url():
    type_detector = TypeDetector()
    given_values = ["http://example.com", "https://example.com"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "url"


def test_type_detector_not_url():
    type_detector = TypeDetector()
    given_values = ["mail://example.com", "test://example.com"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() != "url"


def test_type_detector_string():
    type_detector = TypeDetector()
    given_values = ["test"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.get_type() == "string"


def test_type_detector_required():
    type_detector = TypeDetector()
    given_values = ["test", "test1", "test2", 4]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.required is True


def test_type_detector_not_required():
    type_detector = TypeDetector()
    given_values = ["test", "test1", "test2", "", None]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.required is False


def test_type_detector_unique():
    type_detector = TypeDetector()
    given_values = ["test", "test1", "test2", 4]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.unique is True


def test_type_detector_not_unique():
    type_detector = TypeDetector()
    given_values = ["test", "test1", "test2", 4, "test"]

    for item in given_values:
        type_detector.detect(item)

    assert type_detector.unique is False
