#!/usr/bin/env python3
"""A minimal rcbroker-like REST data service.

A dependency-free stand-in for an rcbroker-like service (see
`apps/address_registry/views/rc_broker_views.py` in the `demo-saltiniai`
repo), exposing one endpoint:

    GET /v1/data/changes

with the required parameters DATA_TYPE, DATE_FROM, DATE_TO, TIME,
CLIENT_NAME and SIGNATURE — an RSA-SHA256 PKCS#1 v1.5 base64-encoded
signature over the concatenated parameter values, the string-to-sign and
key format used by the real service (see
`spinta/adapters/rc/signature_adapter.py`, `build_rc_string_to_sign()`).
If any parameter is missing — or the request is invalid in any other way,
incl. an invalid signature — an AR-style XML error document is returned.
"""
import base64
import hashlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

REQUIRED = ["DATA_TYPE", "DATE_FROM", "DATE_TO", "TIME", "CLIENT_NAME", "SIGNATURE"]

# Throwaway RSA test key pair (it is not a secret); the private half lives in
# `tests/adapters/helpers/rc_test_private_key.pem`. Here: 2048-bit modulus of
# the test key pair (cf. tests/adapters/helpers/rc_test_public_key.pem).
PUBLIC_E = 65537
PUBLIC_N = int(
    "d5b2184fd6cbac14f05ecece71d333881156415d5aee7c27df7e0fc63c68925c3"
    "ccca5ced8e892cdb823cbb6755aed580dd471bb39b0eda9bc0ccb2cdd1252ad1a"
    "54d298b3cd1807a928b80979cd1a8ebd72bf74cec513fa4eed85e24303c11080d"
    "68b27708a7effc4fb2c5f8e7303c698c36011560c9cd6d7df07c22a2d145ee47f"
    "0739d8d8cb03ee89c7d38f8e2acf5cb9a59180f5329fb41af3fc75ed83c74ca94"
    "1bd956a389523a67408e06e945ede137a4f630fc30cbdccbc3a3975705a30d9cd"
    "55854fd9019631342b95a96824faf42d9042a9ff385c06e7ed2789b120c1c66cb"
    "5efabe20d2491b207bb8d7f5019c0d30c781ffc88f6a9c570800c071f",
    16,
)

DATA = {
    "ADM": "<ADM><A><ID>1</ID><CODE>10</CODE><TIPAS>APS</TIPAS></A></ADM>",
    "CITY": "<CITY><C><ID>1</ID><NAME>Vilnius</NAME></C></CITY>",
}


def error_xml(message: str) -> bytes:
    return (
        '<RESPONSE><ERROR>' + message + '</ERROR>'
        '<PARAMETERS>nullnull000</PARAMETERS>'
        '<SIGNATURE>null</SIGNATURE></RESPONSE>'
    ).encode()


def verify_signature(params) -> bool:
    """Verify RSASSA-PKCS1-v1_5 SHA-256 signature, and raise on any mismatch.

    Verified string is the concatenation of the parameter values in the
    string-to-sign order: DATA_TYPE, DATE_FROM, DATE_TO, TIME, CLIENT_NAME —
    exactly as `build_rc_string_to_sign` does it on the real side.
    """
    k = (PUBLIC_N.bit_length() + 7) // 8
    if k != 256:
        raise ValueError(f"tikimasi 256 baitų viešojo rakto, gauta {k}")
    sig = base64.b64decode(params["SIGNATURE"], validate=True)
    if len(sig) != k:
        raise ValueError(f"tikimasi {k} baitų parašo, gauta {len(sig)}")
    s = int.from_bytes(sig, "big")
    if s >= PUBLIC_N:
        raise ValueError("parašas didesnis už modulį")
    m = pow(s, PUBLIC_E, PUBLIC_N)
    em = m.to_bytes(k, "big")
    string_to_sign = "".join(params[p] for p in REQUIRED[:-1])
    digest = hashlib.sha256(string_to_sign.encode("utf-8")).digest()
    digest_info = bytes.fromhex("3031300d060960864801650304020105000420") + digest
    pad = k - len(digest_info) - 3
    em_expected = bytes([0x00, 0x01]) + bytes([0xFF]) * pad + bytes([0x00]) + digest_info
    if em != em_expected:
        raise ValueError("parašo nepavyko patikrinti")
    return True


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlparse(self.path)
        params = {k: v[0] for k, v in parse_qs(url.query).items()}
        missing = [p for p in REQUIRED if p not in params]
        print(f"REQUEST {self.path}", flush=True)
        if missing:
            body = error_xml("Neperduoti visi reikalingi parametrai.")
        elif params["DATA_TYPE"] not in DATA:
            # Same error document as for missing parameters: the real service
            # answers with `Neperduoti visi reikalingi parametrai.` for any
            # invalid request — the DATA_TYPE check here is only for clarity.
            body = error_xml("Neperduoti visi reikalingi parametrai.")
        else:
            try:
                params["SIGNATURE"].encode("ascii")
                verify_signature(params)
            except Exception as e:
                body = error_xml(f"Netinkamas parašas: {e}")
            else:
                body = (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    + DATA[params["DATA_TYPE"]]
                ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # requests are logged explicitly above


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8014
    print(f"rcbroker-like service listening on {port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
