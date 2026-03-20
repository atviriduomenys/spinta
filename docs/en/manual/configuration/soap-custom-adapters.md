# SOAP custom adapters (agent)

Spinta can load **optional Python modules** that plug into the SOAP request pipeline. Use this when a SOAP backend needs values that depend on the **full request body** (for example a signature over several fields) and cannot be prepared from a single manifest property alone.

## How it works

1. **Startup** — `config.yml` lists absolute paths under `soap_adapter_modules`. Each file is loaded with `importlib`; Spinta reads `get_deferred_prepare_names()` and `get_body_resolvers()` from the module.
2. **Deferred prepare names** — For SOAP resources, the manifest `prepare` column can contain ufunc expressions. Names returned by `get_deferred_prepare_names()` are treated as **deferred**: they stay as unevaluated `Expr` objects while defaults are merged and URL/query parameters are applied property-by-property (so a resolver never sees a half-built body).
3. **Body resolvers** — After that pass completes, Spinta resolves each deferred expression by calling the matching resolver from `get_body_resolvers()`. The callable receives a `SoapQueryBuilder` `env` with `env.soap_request_body`, `env.context`, etc., and must return the final value (e.g. a string for a SOAP element).

Optional hook: if the module defines `validate_soap_adapter_config(raw_config)`, it runs immediately after the module is loaded. Use it to require extra keys in `config.yml` so misconfiguration fails at startup.

## `config.yml`

```yaml
soap_adapter_modules:
  - /absolute/path/to/my_adapter.py
```

List one or more filesystem paths. Paths must exist; missing files are logged and skipped.

Adapter-specific settings live next to other top-level keys, for example the RC signature helper expects:

```yaml
rc_signature:
  private_key_path: /absolute/path/to/private.pem
```

(Only adapters that read those keys need them; your own adapter can use different keys and validate them in `validate_soap_adapter_config`.)

## DSA manifest (`prepare` column)

SOAP **resource params** map manifest properties to the SOAP body. The `prepare` column describes how each param gets its default value before the request runs.

- Typical prepares (`input()`, `cdata().input()`, …) are resolved as soon as the body is assembled.
- A prepare that calls a **deferred** ufunc (name registered via `get_deferred_prepare_names()`) stays as an `Expr` until the adapter resolver runs.

Example (pattern only; column names depend on your CSV layout):

| property   | source           | prepare        |
|------------|------------------|----------------|
| signature  | `input/Signature`| `rc_signature()` |

Here `rc_signature` must be both:

- listed in `get_deferred_prepare_names()` → `["rc_signature"]`, and  
- provided in `get_body_resolvers()` → `{"rc_signature": callable}`.

The resolver runs when other body fields are already on `env.soap_request_body`, so it can read them and compute the signature (or any derived field).

## Adapter module contract

Implement a normal Python module (any filename) with:

| Function | Required | Purpose |
|----------|----------|---------|
| `get_deferred_prepare_names()` | Yes | Return `list[str]` of ufunc names to defer. |
| `get_body_resolvers()` | Yes | Return `dict[str, callable]` mapping each name to `fn(env, expr=None)` or `fn(env)`. |
| `validate_soap_adapter_config(raw_config)` | No | Raise if `config.yml` is invalid for this adapter. |

Reference implementation in the Spinta tree: `spinta/adapters/rc_signature_adapter.py`, wired from a manifest row like `rc_signature()` in the RC broker example (`prepare` on the signature param).

## Operational notes

- Use **absolute paths** in `soap_adapter_modules` so the process cwd does not matter.
- The agent process must be able to import dependencies used by your adapter file (same environment as Spinta).
- If your resolver needs configuration, read it from `env.context.get("rc")` the same way built-in code reads `config.yml` (nested `.get("section", "key", …)` on the raw config object).
