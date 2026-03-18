from __future__ import annotations

from pathlib import Path

from lxml import etree

import spinta.adapters.rc_signature_adapter as adapter


def test_rc_signature_string_to_sign_matches_shell_example(monkeypatch) -> None:
    # These values come from the legacy shell example the user runs.
    action_type = "17"
    caller_code = "TEST_VSSA_IS,vasu_id=190286"
    end_user_info = ""
    parameters = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><args><fmt>xml</fmt><obj_kodas>188772433</obj_kodas></args>'
    time_value = "2026-03-12 07:50:30"

    # Build soap_body in the same shape it has when the adapter sees it:
    # nested "input" dict with plain string values. CDATA wrapping happens later,
    # in the SOAP serializer, after the signature has already been computed.
    soap_body = {
        "input": {
            "ActionType": action_type,
            "CallerCode": caller_code,
            "EndUserInfo": end_user_info,
            "Parameters": parameters,
            "Time": time_value,
        }
    }

    # Expected ARGS from the shell example:
    #   ARGS="$ACTION$CALLER$USER_INFO$PARAMS$TIME"
    expected_args = (
        "17"
        "TEST_VSSA_IS,vasu_id=190286"
        ""
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><args><fmt>xml</fmt><obj_kodas>188772433</obj_kodas></args>'
        "2026-03-12 07:50:30"
    )

    # Act
    args = adapter.build_rc_poc_string_to_sign(soap_body)

    # Assert: the string-to-sign matches shell ARGS.
    assert args == expected_args


