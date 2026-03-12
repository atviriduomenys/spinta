
# Examine WSDL response

## What happens in this step (SOAP → base64 → XML)

```bash
python3.12 -m venv .venv
source .venv/bin/activate
```

### Create `scripts/decore_base64.py`

```bash
mkdir -p scripts

cat > scripts/decode_base64.py <<'PY'
import base64

b64_payload = (
    "PGNvdW50cmllcz48Y291bnRyeURhdGE+PGlkPjI8L2lkPjx0aXRsZT5MaWV0dXZhPC90aXRsZT48"
    "Y29udGluZW50X2lkPjE8L2NvbnRpbmVudF9pZD48Y29udGluZW50Pjxjb2RlPjE8L2NvZGU+PG5h"
    "bWU+RXVyb3BlPC9uYW1lPjwvY29udGluZW50PjwvY291bnRyeURhdGE+PGNvdW50cnlEYXRhPjxp"
    "ZD4xPC9pZD48dGl0bGU+TG9yZW08L3RpdGxlPjxjb250aW5lbnRfaWQ+MTwvY29udGluZW50X2lk"
    "Pjxjb250aW5lbnQ+PGNvZGU+MTwvY29kZT48bmFtZT5FdXJvcGU8L25hbWU+PC9jb250aW5lbnQ+"
    "PC9jb3VudHJ5RGF0YT48L2NvdW50cmllcz4="
)

xml_bytes = base64.b64decode(b64_payload)
xml_text = xml_bytes.decode("utf-8")

print(xml_text)
PY
```

The legacy SOAP response does not return the data as normal structured fields. Instead, it includes a field that contains a **base64-encoded XML string**.

In this demo we:

1. Run `python scripts/decode_base64.py` to **decode** the base64 value back into the original XML text.
2. Pipe the output into a Python one-liner to **pretty-print** (indent/format) the XML so it is readable.

After decoding and prettifying, we can see that the real payload is an XML document with:

- a root element: `<countries>`
- inside it, a list/array of `<countryData>` elements
- each `<countryData>` object contains fields like `<id>`, `<title>`, `<continent_id>`, and a nested `<continent>` object

Run:

```bash
python scripts/decode_base64.py | python -c "import sys; from xml.dom import minidom; print(minidom.parseString(sys.stdin.buffer.read()).toprettyxml(indent='  '))"

```

## Spinta Installation

```bash
python3.12 -m venv .venv
source .venv/bin/activate

pip install --upgrade --pre spinta
pip install uvicorn
```

--

### Create `config.yml`

Create a `config.yml` file with the following content:

```bash
cat > config.yml <<'YAML'
# Test environment overrides
data_path: $BASEDIR/data

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$BASEDIR/data/keymap.db

accesslog:
  type: file
  file: $BASEDIR/accesslog.json

# RC Broker signature adapter configuration (required for POC)
rc_signature:
  private_key_path: $BASEDIR/raktas_priv.pem

# SOAP adapter modules (load adapter from local file)
soap_adapter_modules:
  - $BASEDIR/spinta/adapters/rc_signature_adapter.py
YAML
```

### Create `dsa.csv`

Create a `dsa.csv` file with the following content:

```bash
cat > dsa.csv <<'CSV'
id,dataset,resource,base,model,property,type,ref,source,prepare,level,status,visibility,access,uri,eli,title,description
,datasets/org/lg/is/demo/location,,,,,dataset,,,,,,,,,,,"DSA for endpoint very similar to Registrų centras broker. All parameters do nothing except action_type=""64"" will return ResponseData without base64 encoding"
,,rc_wsdl,,,,wsdl,,https://test-data.data.gov.lt/api/v1/rc/get-data/?wsdl,,,,,,,,,
,,get_data,,,,soap,,Get.GetPort.GetPort.GetData,wsdl(rc_wsdl),,,,,,,,
,,,,,,param,action_type,input/ActionType,input(),,,,,,,,
,,,,,,param,caller_code,input/CallerCode,input(),,,,,,,,
,,,,,,param,end_user_info,input/EndUserInfo,input(),,,,,,,,
,,,,,,param,parameters,input/Parameters,input(),,,,,,,,
,,,,,,param,time,input/Time,input(),,,,,,,,
,,,,,,param,signature,input/Signature,rc_signature(),,,,,,,,
,,,,,,param,caller_signature,input/CallerSignature,input(),,,,,,,,
,,,,GetData,,,,/,,,,,open,,,,
,,,,,response_code,string,,ResponseCode,,,,,,,,,
,,,,,response_data,string,,ResponseData,base64(),,,,,,,,
,,,,,decoded_parameters,string,,DecodedParameters,,,,,,,,,
,,,,,action_type,string,,,param(action_type),,,,,,,,
,,,,,end_user_info,string,,,param(end_user_info),,,,,,,,
,,,,,caller_code,string,,,param(caller_code),,,,,,,,
,,,,,parameters,string,,,param(parameters),,,,,,,,
,,,,,time,string,,,param(time),,,,,,,,
,,,,,signature,string,,,param(signature),,,,,,,,
,,,,,caller_signature,string,,,param(caller_signature),,,,,,,,
,,,,,,,,,,,,,,,,,
,,nested_read,,,,dask/xml,,,eval(param(nested_xml)),,,,,,,,
,,,,,,param,nested_xml,GetData,read().response_data,,,,,,,,
,,,,Country,,,,countries/countryData,,,,,open,,,,
,,,,,id,string,,id,,,,,,,,,
,,,,,title,string,,title,,,,,,,,,
,,,,,,,,,,,,,,,,,
CSV
```

### Start the Spinta Web Server

```bash
spinta -o config=config.yml run dsa.csv --mode external
```

---

## Transform Legacy resource to UAPI

UAPI response for "GetData" model

```bash
curl -sS --location \
  'http://127.0.0.1:8000/datasets/org/lg/is/demo/location/GetData?action_type=25&caller_code=10&parameters=%22%22&time=%221%22&signature=%221%22' \
  | python -m json.tool
```

UAPI response when GetData model is parsed to get Country model

```bash
curl -sS --location \
  'http://127.0.0.1:8000/datasets/org/lg/is/demo/location/Country/:format/json?action_type=25&caller_code=10&parameters=%22%22&time=%221%22&signature=%221%22' \
  | python -m json.tool
```

---

## RC Broker signature (POC)

This section is the **testing guide** for the RC signature POC: how to load the branch, install, configure the key, and verify that Spinta computes and sends the signature to the RC Broker test environment. It is aimed at anyone testing the flow against the test RC Broker machine.

**Fresh-install checklist (summary)**

1. Clone this repo and checkout the POC branch.
2. Create a venv and install Spinta from the repo (`pip install -e .`).
3. Generate an RSA key pair and **register the public key with RC** (test environment); only then will RC accept signed requests.
4. Configure the private key path (in `config.yml` or via env var).
5. Ensure `config.yml` and `dsa.csv` exist (repo includes examples).
6. Start Spinta and call GetData; check server logs for the signature and SOAP envelope.

---

### 1. Install Spinta from the repo

The signature POC lives in this repository. Clone it, checkout the branch with the POC, then install in editable mode so the manifest and SOAP code use the new logic:

```bash
git clone <repo-url>
cd spinta
git checkout <poc-branch-name>

python3.12 -m venv .venv
source .venv/bin/activate   # or: .venv\Scripts\activate on Windows

pip uninstall spinta
pip install -e .
pip install uvicorn
```

### 2. Create a key pair and register the public key with RC

The POC signs the request using a private key. RC verifies the signature using the **public key** that you must **register with the RC test environment** (via RC’s process / VSSA ops). Until the public key is registered, RC will reject the signed request.

```bash
# Generate a 2048-bit RSA private key (for demo/testing)
openssl genrsa -out raktas_priv.pem 2048

# Export the public key (send this to RC / VSSA for registration in the RC test environment)
openssl rsa -in raktas_priv.pem -pubout -out raktas_pub.pem
```

**Important:** Register `raktas_pub.pem` (or its contents) with the RC Broker test environment before testing; otherwise RC will not accept the signature.

### 3. Configure the private key path

The adapter looks for the private key path in this order:

1. **config.yml** (recommended for production/admin use):
   ```yaml
   rc_signature:
     private_key_path: /path/to/raktas_priv.pem
   ```

2. **Environment variable** (useful for dev/testing):
   ```bash
   export RC_POC_PRIVATE_KEY_PATH=$PWD/raktas_priv.pem
   ```

If neither is set, the adapter logs a warning and the signature will be empty (RC will reject the request).

For this demo, either add the `rc_signature` section to `config.yml` or set the env var.

### 3b. Load adapter module

The POC uses **config-driven adapter loading** via `soap_adapter_modules` in `config.yml`. This loads the adapter from a local file path at startup, without requiring a package install or entry points.

The example `config.yml` above already includes:

```yaml
soap_adapter_modules:
  - $BASEDIR/spinta/adapters/rc_signature_adapter.py
```

The module must export `get_deferred_prepare_names()` and `get_body_resolvers()` functions. This approach lets admins deploy adapter files without updating Spinta's version.

**Note:** For production releases, adapters can also be registered via Python entry points (`spinta.soap.deferred_prepares` and `spinta.soap.body_resolvers` groups in `pyproject.toml`), which Spinta loads automatically at startup.

### 4. DSA: signature param uses `rc_signature()`

In `dsa.csv`, the **signature** resource param must have **prepare** `rc_signature()` (not `input()`). The current `dsa.csv` in this repo already has:

- `param,signature,input/Signature,rc_signature()`

So the SOAP body field `Signature` is filled by the POC (computed from ActionType, CallerCode, Parameters, Time and the private key). You can omit `signature` from the URL or pass any value (e.g. `signature=ignored`); it will be overwritten.

If your RC test environment uses a different WSDL endpoint, edit the `rc_wsdl` row in `dsa.csv` (the `uri` column) and ensure the host has network access / IP allowlist to RC Broker if required.

### 5. Start Spinta and call GetData

```bash
spinta -o config=config.yml run dsa.csv --mode external
```

Then:

```bash
curl -sS 'http://127.0.0.1:8000/datasets/org/lg/is/demo/location/GetData' \
  --get \
  --data-urlencode 'action_type=25' \
  --data-urlencode 'caller_code=10' \
  --data-urlencode 'parameters=""' \
  --data-urlencode 'time="2026-03-03 10:05:24"' \
  --data-urlencode 'signature=ignored' \
  | python -m json.tool
```

In the server log you should see the computed signature and a SOAP envelope fragment with `<apps:Signature>` filled. If neither `rc_signature.private_key_path` in config.yml nor `RC_POC_PRIVATE_KEY_PATH` env var is set, the log will warn and the signature will be empty.
