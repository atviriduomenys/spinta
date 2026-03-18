from __future__ import annotations

from pathlib import Path

from spinta.adapters.rc_signature_adapter import _compute_rc_signature


def test_rc_simple_message_signing_matches_openssl() -> None:
    # Arrange
    project_root = Path(__file__).resolve().parents[2]
    key_path = project_root / "raktas_priv.pem"

    if not key_path.is_file():
        import pytest

        pytest.skip(f"Private key file not found at {key_path}")

    message = "test"

    # This value was produced with:
    #   printf 'test' | openssl dgst -sha256 -sign raktas_priv.pem | base64 -w0
    expected_signature = (
        "vvI1KKNyBu/dIkq4cIG+2ureX158o/J+2Av5wrcW2p7+n3FWig6Q7AqKDIWHod4oa2lzUePjYz9KW60H"
        "OZtMl1UaWp/kTyrjTzsCTP1oIG3svPADSZ0nZH3X3ll66KHIKvU0iqoeBWtZkl73BJ2S5OinD5km/5OI"
        "mJ8LMxlTmAUSa9vC0FrQUf+3ZixhpB5oJm412IMtjIVVETl4aYy0C/KSqqNUC2AANbzsITeu4EkfXceR"
        "57ZRnx1+XmnpNtsWyNyuYFEIoiy6EkABa+aONB6eLHmYS+B/H7PKGZSxxgu542UavNTFtPgYg5YFDkeU"
        "QX0rB2OMO9ES8m2iXreWug=="
    )

    # Act
    actual = _compute_rc_signature(message, str(key_path))

    # Assert
    assert actual == expected_signature


def test_rc_exact_message_signing_matches_openssl() -> None:
    # Arrange
    project_root = Path(__file__).resolve().parents[2]
    key_path = project_root / "raktas_priv.pem"

    if not key_path.is_file():
        import pytest

        pytest.skip(f"Private key file not found at {key_path}")

    message = '17TEST_VSSA_IS,vasu_id=190286<?xml version="1.0" encoding="UTF-8" standalone="yes"?><args><fmt>xml</fmt><obj_kodas>188772433</obj_kodas></args>2026-03-12 07:50:30'

    # This value was produced with:
    #   printf 'test' | openssl dgst -sha256 -sign raktas_priv.pem | base64 -w0
    expected_signature = (
        "mdLXBiu6D+vQ4h59+zMT+h0/qNL5SRFjl+eslyLPgIzicZFtnPGvJ5zppO155LQmLxyKl6M+NWRIhumEdOU78pdS"
        "snb5821w+t+wIqCu7eTq8epQJgGt3sXRQRJg6/YSBORKg5l+5j92PqDF21wHg1QIeA+njh6LKcSnFiXmCUrWvIaF"
        "kaun/YbfVLNtXaSXx4KkpGwJCOe30qYBYNXZ0UY26Nnvn4nvYOEspJx+IM/4pZTn/JEiSMsQ3NNn68g/csbEGSPG"
        "ntGPROBf5IvGWNI3FBoOISPyApt/B9GC0aBhJOEMmb8XCnmGAvOPfphqRzw+7k69rF8+0G+sxJMb9w=="
    )

    # Act
    actual = _compute_rc_signature(message, str(key_path))

    # Assert
    assert actual == expected_signature
