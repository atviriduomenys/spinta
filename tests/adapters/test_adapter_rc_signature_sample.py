from __future__ import annotations

from pathlib import Path

import pytest

import spinta.adapters.rc_signature_adapter as adapter


def _rc_test_private_key_path() -> Path:
    here = Path(__file__).resolve().parent
    return here / "helpers" / "raktas_priv.pem"


def test_rc_simple_message_signing() -> None:
    key_path = _rc_test_private_key_path()
    if not key_path.is_file():
        pytest.skip(f"Private key file not found at {key_path}")

    message = "test"
    expected_signature = (
        "vvI1KKNyBu/dIkq4cIG+2ureX158o/J+2Av5wrcW2p7+n3FWig6Q7AqKDIWHod4oa2lzUePjYz9KW60H"
        "OZtMl1UaWp/kTyrjTzsCTP1oIG3svPADSZ0nZH3X3ll66KHIKvU0iqoeBWtZkl73BJ2S5OinD5km/5OI"
        "mJ8LMxlTmAUSa9vC0FrQUf+3ZixhpB5oJm412IMtjIVVETl4aYy0C/KSqqNUC2AANbzsITeu4EkfXceR"
        "57ZRnx1+XmnpNtsWyNyuYFEIoiy6EkABa+aONB6eLHmYS+B/H7PKGZSxxgu542UavNTFtPgYg5YFDkeU"
        "QX0rB2OMO9ES8m2iXreWug=="
    )

    actual = adapter._compute_rc_signature(message, str(key_path))
    assert actual == expected_signature


def test_rc_exact_parameters_signing() -> None:
    key_path = _rc_test_private_key_path()
    if not key_path.is_file():
        pytest.skip(f"Private key file not found at {key_path}")

    message = (
        '17TEST_VSSA_IS,vasu_id=190286<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        "<args><fmt>xml</fmt><obj_kodas>188772433</obj_kodas></args>2026-03-12 07:50:30"
    )
    expected_signature = (
        "mdLXBiu6D+vQ4h59+zMT+h0/qNL5SRFjl+eslyLPgIzicZFtnPGvJ5zppO155LQmLxyKl6M+NWRIhumEdOU78pdS"
        "snb5821w+t+wIqCu7eTq8epQJgGt3sXRQRJg6/YSBORKg5l+5j92PqDF21wHg1QIeA+njh6LKcSnFiXmCUrWvIaF"
        "kaun/YbfVLNtXaSXx4KkpGwJCOe30qYBYNXZ0UY26Nnvn4nvYOEspJx+IM/4pZTn/JEiSMsQ3NNn68g/csbEGSPG"
        "ntGPROBf5IvGWNI3FBoOISPyApt/B9GC0aBhJOEMmb8XCnmGAvOPfphqRzw+7k69rF8+0G+sxJMb9w=="
    )

    actual = adapter._compute_rc_signature(message, str(key_path))
    assert actual == expected_signature


def test_rc_signature_message_preparation() -> None:
    action_type = "17"
    caller_code = "TEST_VSSA_IS,vasu_id=190286"
    end_user_info = ""
    parameters = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        "<args><fmt>xml</fmt><obj_kodas>188772433</obj_kodas></args>"
    )
    time_value = "2026-03-12 07:50:30"

    soap_body = {
        "input": {
            "ActionType": action_type,
            "CallerCode": caller_code,
            "EndUserInfo": end_user_info,
            "Parameters": parameters,
            "Time": time_value,
        }
    }

    expected_args = f"{action_type}{caller_code}{end_user_info}{parameters}{time_value}"
    assert adapter.build_rc_poc_string_to_sign(soap_body) == expected_args
