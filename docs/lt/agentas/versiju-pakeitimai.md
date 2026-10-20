# Pakeitimai pagal versijas

Šiame puslapyje surinkti tik tie Spinta pakeitimai, kurie **keičia Agento diegimo,
konfigūravimo ar atnaujinimo instrukcijas**. Pilnas pakeitimų sąrašas —
[`CHANGES.rst`](https://github.com/atviriduomenys/spinta/blob/master/CHANGES.rst).

## Kaip naudotis

1. Sužinokite dabartinę versiją: `spinta --version` (Docker: `docker exec spinta spinta --version`).
2. Peržiūrėkite **visas eilutes nuo jūsų versijos (neimtinai) iki versijos, į kurią atnaujinate**.
   Jei peršokate per kelias versijas, galioja visų tarpinių versijų pakeitimai.
3. Atlikite veiksmus, pažymėtus **Privaloma**, prieš paleisdami naują versiją.

Stulpelis „Privaloma":

- **Taip** — be šio veiksmo Agentas po atnaujinimo neveiks arba veiks neteisingai.
- **Sąlyginai** — privaloma tik nurodytu atveju.
- **Ne** — senas būdas dar veikia, bet rekomenduojama pakeisti, nes ateityje jis gali būti pašalintas.

## Pakeitimai

| Versija  | Kas pasikeitė | Ką daryti | Privaloma |
|----------|---------------|-----------|-----------|
| 0.2dev22 | Pasikeitė `access` numatytoji reikšmė: `protected` → `open`. Modeliai su `access=protected` grąžina `ModelNotFound`. | Į `config.yml` įrašyti `access: protected`. | **Taip** |
| 0.2dev24 | Docker atvaizde nebėra Poetry. | Iš `docker-compose.yml` `command:` pašalinti `poetry run` priešdėlius. | Sąlyginai — jei naudojote `poetry run` |
| 1.0.0    | Pirmoji stabili versija. Pre-release (`0.2devN`) versijos nebeleidžiamos. | Diegti ir atnaujinti iš `spinta-latest.txt` (ne `spinta-latest-pre.txt`). | **Taip** |
| 1.1.0    | `spinta-1.1.0.txt` paketų sąraše nebėra `uvicorn`. | OS diegime patikrinti `env/bin/pip show uvicorn`, jei nėra — įdiegti. | Sąlyginai — OS diegimas |
| 1.2.0    | Žetonai tikrinami pagal `iss` ir `aud`. Atsirado privalomi `token_issuer` ir `resource_server`. | Į `config.yml` įrašyti `token_issuer` ir `resource_server` (nuo 1.3.0 — nauji pavadinimai, žr. žemiau). | **Taip** — jei Agentas išduoda ar tikrina žetonus |
| 1.2.0    | Kai nurodytas `token_validation_key` arba `token_validation_keys_download_url`, Agento `/auth/token` ir `/auth/introspect` išjungiami. | Žetonus gauti iš nutolusio AM (Gravitee), ne iš Agento. | Sąlyginai — naudojant nutolusį AM |
| 1.2.0    | Nauja introspekcija (`/auth/introspect`, leidimas `uapi:/:auth_introspect`). | Sukurti introspekcijos klientą, išsaugoti jo pavadinimą ir `secret`. Žr. [Autentifikacija](autentifikacija.md). | Sąlyginai — publikuojant paslaugas į vartus |
| 1.2.0    | Pasikeitė generuojama OAS specifikacija: OpenAPI `3.0.3`, `servers` imami iš `--udts-cfg`. | Sugeneruoti OAS failus iš naujo (`spinta udts oas`). | Sąlyginai — publikuojant paslaugas į vartus |
| 1.3.0    | Pervadinti autorizacijos parametrai: `token_issuer` → `auth_server_id` + `auth_server_url`, `resource_server` → `resource_server_id`, `server_url` → `resource_server_url`. | Pervadinti `config.yml`. | Ne — seni pavadinimai dar skaitomi |
| 1.3.0    | `credentials.cfg`: sekcija `[katalogas]` vietoj `[default]`, `server` → `auth_server_url`, `resource_server` → `resource_server_url`. | Pervadinti `credentials.cfg`. | Ne — seni pavadinimai dar skaitomi |
| 1.3.0    | Žetono `aud` tapo sąrašu (`[resource_server_id]`). | Patikrinti, ar žetonus tikrinančios sistemos `aud` nelaiko vienu tekstu. | Patikrinti |
