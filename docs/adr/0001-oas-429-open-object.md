# ADR-0001: OAS `429` atsakymas – atviras objektas, limitus taiko vartai

- **Būsena:** Priimtas
- **Data:** 2026-09-17
- **Susiję:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Kontekstas

`spinta udts oas` generuoja duomenų paslaugos OpenAPI aprašą, kurį API vartai
naudoja dviem tikslais: endpoint'ams importuoti ir užklausoms bei atsakymams
tikrinti (*OpenAPI Specification Validation* politika Request ir Response
fazėse).

Kiekviena operacija apraše deklaruoja `429 Too Many Requests` atsakymą. Pati
Spinta užklausų limitų neturi ir `429` niekada negrąžina – jį grąžina tai, kas
stovi priešais ją. Todėl kyla klausimas, ką aprašas turi sakyti apie `429`
atsakymo kūną:

- Iš pradžių schema `RateLimited` neturėjo `type`, kad tiktų bet koks kūnas
  (objektas, tekstas, tuščias), nes nebuvo žinoma, kas limitą taiko.
- Vartų komanda, tikrindama aprašus `vacuum` su vartų taisyklių rinkiniu, gavo
  pastabą `oas-missing-type` ir paprašė schemai nurodyti `type: object`.
- Copilot recenzija atkreipė dėmesį, kad `type: object` atmestų ne JSON `429`
  kūną, jei limitą taikytų tarpinis serveris tarp vartų ir Spintos (pvz., nginx
  pagal nutylėjimą grąžina HTML), nes toks atsakymas vartams atrodo kaip
  paslaugos atsakymas ir eina per atsakymo validaciją.

Rizika realizuojasi tik tada, kai limitą taiko kas nors **tarp vartų ir
Spintos**. Kai limitą taiko patys vartai, jie grąžina JSON objektą; kai WAF
prieš vartus – jo atsakymas vartų validacijos nepasiekia.

## Sprendimas

`429` atsakymo schema `RateLimited` yra **atviras objektas**:

```json
"RateLimited": {
  "type": "object",
  "description": "Answer of a rate limit reached. …",
  "properties": {},
  "example": {"message": "Rate limit exceeded", "http_status_code": 429}
}
```

- `type: object` – vartai į limitą atsako JSON objektu;
- jokių privalomų laukų ir `properties: {}` – laukus nusprendžia vartai, ne
  Spinta, todėl validacija neatmes `429` atsakymo dėl jo turinio;
- media tipas lieka `*/*`.

Sprendimas remiasi susitarimu su partneriais: **užklausų limitai taikomi per
vartus**. Partneriai savo limitų prieš agentą neturėtų taikyti, o jei taiko, jie
neturi būti griežtesni už vartų – tada jų limitas nesuveikia anksčiau nei vartų
ir `429` visada grąžina vartai.

## Svarstytos alternatyvos

1. **Palikti `429` kūną neaprašytą (be `type`).** Tiktų bet kokiam kūnui, bet
   vartų linteris tai laiko klaida (`oas-missing-type`), o susitarus, kad
   limitus taiko vartai, lankstumas nereikalingas. Atmesta.
2. **Aprašyti konkrečią vartų atsakymo struktūrą** (privalomi `message`,
   `http_status_code`). Pririštų aprašą prie konkrečios vartų versijos ir jos
   atsakymo formato. Atmesta.
3. **Keisti media tipą į `application/json`.** Tikslesnis, bet nieko nekeičia
   validacijai, kol kūnas yra objektas, ir būtų dar griežtesnis tarpinio
   serverio atveju. Neatlikta.

## Pasekmės

- Aprašas praeina vartų `vacuum` taisyklių rinkinį be `oas-missing-type`.
- Jei partneris vis dėlto taikys griežtesnį limitą tarpiniame serveryje tarp
  vartų ir Spintos ir šis grąžins ne JSON kūną, vartų atsakymo validacija tokį
  `429` atmes ir klientas gaus vartų klaidą vietoj `429`. Tai susitarimo
  pažeidimas, sprendžiamas su partneriu, o ne aprašu.
- Recenzento siūlymas grąžinti `429` kūną į neaprašytą nevykdomas – nurodoma
  šis ADR.
- Įgyvendinta Spinta commite `96e77953` (`spinta/manifests/open_api/openapi_config.py`,
  `COMMON_SCHEMAS["RateLimited"]`).
