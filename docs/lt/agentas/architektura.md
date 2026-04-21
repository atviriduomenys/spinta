# Architektūra

## UDTS duomenų paslaugos schema

```{figure} ../static/udts-paslauga.png
:width: 100%
:alt: UDTS duomenų paslaugos kūrimas naudojant SPINTA agentą
:target: ../_static/udts-paslauga.png

UDTS duomenų paslauga — nuo šaltinio iki duomenų vartotojo (spustelėkite norėdami padidinti)
```

Schemoje pavaizduoti du agentų pavyzdžiai:

- **SPINTA agentas A (Gunicorn)** — SQL šaltiniui, paleistas su Gunicorn
  (rekomenduojama didelės apkrovos SQL backend'ams)
- **SPINTA agentas B (SPINTA run)** — SOAP/WSDL šaltiniui, paleistas su
  `spinta run` (standartinis būdas ne-SQL šaltiniams)

## Infrastruktūros atsakomybės

| Komponentas | Kas valdo |
|-------------|-----------|
| Spinta agentai | Institucija |
| Redis (keymap) | Institucija |
| Reverse proxy | Institucija |
| Gravitee vartai | VSSA |
| Access Manager | VSSA |
| Katalogas (data.gov.lt) | VSSA |

## Duomenų srautai

| Srautas | Protokolas | Aprašas |
|---------|------------|---------|
| Duomenų šaltinis → Agentas | SQL/SOAP/REST | Agentas skaito duomenis iš šaltinio pagal DSA |
| Agentas → Vartai | HTTPS/UDTS | Vartai perduoda užklausas agentui |
| Vartai → Vartotojas | HTTPS/UDTS | Vartotojas gauna duomenis UDTS formatu |
| Agentas → Katalogas | HTTPS | Sinchronizacija (`spinta sync`) per viešąjį internetą (443 portas) |
| Vartotojas → AM | OAuth 2.0 | Autentifikacija prieš prisijungiant prie vartų |
