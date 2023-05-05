from spinta.backends.postgresql.helpers.extractors import extract_error_model, extract_error_property_name, \
    extract_error_ref_id


def test_extract_error_ref_id():
    error_message = "Key (baseOne._id)=(b1da1c16-b8a6-4c60-b8cd-364efe8d7b44) is not present in table \"datasets/gov/ivpk/adp/catalog/BaseOne\"."
    result = extract_error_ref_id(error_message)
    assert result == "b1da1c16-b8a6-4c60-b8cd-364efe8d7b44"


def test_extract_error_property_name():
    error_message = "Key (baseOne._id)=(b1da1c16-b8a6-4c60-b8cd-364efe8d7b44) is not present in table \"datasets/gov/ivpk/adp/catalog/BaseOne\"."
    result = extract_error_property_name(error_message)
    assert result == "baseOne"


def test_extract_error_model():
    error_message = "Key (_id)=(36f62b50-aba2-472e-a6bc-234716923449) is still referenced from table \"datasets/gov/ivpk/adp/catalog/BaseFK\"."
    result = extract_error_model(error_message)
    assert result == "datasets/gov/ivpk/adp/catalog/BaseFK"
