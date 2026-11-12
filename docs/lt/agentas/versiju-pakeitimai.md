# Pakeitimai pagal versijas

Šiame puslapyje surinkti tik tie Spinta pakeitimai, kurie **keičia Agento diegimo,
konfigūravimo ar atnaujinimo instrukcijas**. Pilnas pakeitimų sąrašas —
[`CHANGES.rst`](https://github.com/atviriduomenys/spinta/blob/master/CHANGES.rst).

Pačios dokumentacijos pataisymai surašyti puslapyje
[Dokumentacijos pakeitimai](dokumentacijos-pakeitimai.md).

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
| 1.2.0    | Žetonai tikrinami pagal `iss` ir `aud`. Atsirado privalomi `token_issuer` ir `resource_server`. | Į `config.yml` įrašyti `token_issuer` ir `resource_server` (nuo 1.3.0 — nauji pavadinimai, žr. žemiau). | **Taip** — jei Agentas išduoda ar tikrina žetonus |
| 1.2.0    | Kai nurodytas `token_validation_key` arba `token_validation_keys_download_url`, Agento `/auth/token` ir `/auth/introspect` išjungiami. | Žetonus gauti iš nutolusio AM (Gravitee), ne iš Agento. | Sąlyginai — naudojant nutolusį AM |
| 1.2.0    | Nauja introspekcija (`/auth/introspect`, leidimas `uapi:/:auth_introspect`). | Sukurti introspekcijos klientą, išsaugoti jo pavadinimą ir `secret`. Žr. [Autentifikacija](autentifikacija.md). | Sąlyginai — publikuojant paslaugas į vartus |
| 1.2.0    | Pasikeitė generuojama OAS specifikacija: OpenAPI `3.0.3`, `servers` imami iš `--udts-cfg`. | Sugeneruoti OAS failus iš naujo (`spinta udts oas`). | Sąlyginai — publikuojant paslaugas į vartus |
| 1.3.0    | Pervadinti autorizacijos parametrai: `token_issuer` → `auth_server_id` + `auth_server_url`, `resource_server` → `resource_server_id`, `server_url` → `resource_server_url`. | Pervadinti `config.yml`. | Ne — seni pavadinimai dar skaitomi |
| 1.3.0    | `credentials.cfg`: sekcija `[katalogas]` vietoj `[default]`, `server` → `auth_server_url`, `resource_server` → `resource_server_url`. | Pervadinti `credentials.cfg`. | Ne — seni pavadinimai dar skaitomi |
| 1.3.0    | Žetono `aud` tapo sąrašu (`[resource_server_id]`). | Patikrinti, ar žetonus tikrinančios sistemos `aud` nelaiko vienu tekstu. | Patikrinti |
| 1.3.1    | Perkurti `spinta-1.1.0.txt`, `spinta-1.2.0.txt`, `spinta-1.3.0.txt` ir `spinta-latest.txt` failai: į juos grąžinti `uvicorn` ir `gunicorn`. `1.3.0` Docker atvaizde `uvicorn` taip pat trūko. | OS diegime, jei diegėte iki 2026-10-22, patikrinti `env/bin/pip show uvicorn`, jei nėra — įdiegti. Docker atnaujinti bent iki `1.3.1`. | Sąlyginai — jei `uvicorn` nėra |
| 2.0.0    | Duomenų rinkinio kelyje leidžiamas skaitinis segmentas (`/1`). Senesnės versijos tokį kelią atmeta per `spinta copy` ir `spinta check --check-names` (`InvalidName`). | Atnaujinti iki `2.0.0`. | Sąlyginai — jei naudojate šias komandas |
| 2.0.0    | Konfigūracijos failuose raktai, kurie laiko vardų sąrašą (pvz. `backends`, `keymaps`, `manifests`), nebepriima vienos reikšmės. | `backends: one` pakeisti į `backends: [one]`. | Sąlyginai — jei taip nurodyta |
| 2.0.0    | Aplinkos kintamasis ar komandinės eilutės parametras, nustatantis tokį raktą, paslepia konfigūracijos faile nurodytus poraktus. Pavyzdžiui, `SPINTA_KEYMAPS=other` paslepia `keymaps.default`. | Pašalinti perrašymą arba išvardyti norimus poraktus. | Sąlyginai — jei naudojate tokius kintamuosius |
| 2.1.0    | `query()` resurso parametrai dabar siunčiami duomenų šaltiniui (`dask/csv`, `dask/json`, `dask/xml`, `dask/soap`); anksčiau jie buvo ignoruojami. Užklausoje nurodyta reikšmė parametrui, nesusietam su savybe per `param()`, grąžina `400`. | Patikrinti, ar šaltinis tikisi siunčiamų parametrų, ir išbandyti užklausas prieš atnaujinant PROD. | Sąlyginai — jei DSA naudoja `query()` |
| 2.1.0    | `query()` be numatytosios reikšmės, nesusietas su savybe per `param()`, daro manifestą netinkamą. | Susieti per `param()` arba nurodyti numatytąją reikšmę; patikrinti `spinta check`. Konfliktus tarp `source` URL ir `query()` parametrų parodo `spinta check --check-query-conflicts`. | Sąlyginai — jei DSA naudoja `query()` |
