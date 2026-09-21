# ADR-0004: Tuščias deklaruotas identifikatorius OAS apraše neaprašomas

- **Būsena:** Priimtas
- **Data:** 2026-09-17
- **Susiję:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Kontekstas

Modelis gali deklaruoti savo `_id`. Tada objekto identifikatorius yra ne Spintos
sugeneruotas UUID, o modelio pirminio rakto reikšmė:

- `base32` – raktas, užkoduotas Base32 be `=` užpildo (`encode_base32`);
  `kodas = "LT"` → `_id = "JRKA"`, objektas pasiekiamas `/Salis/=JRKA`;
- `string` – rakto reikšmė, kai raktas nesudėtinis pasiekiama per `=`
  (`/Salis/=LT`, žr. `is_accessible_by_equals_sign`).

OAS apraše tokiam identifikatoriui generuojamas šablonas:

| Kur | Šablonas |
|---|---|
| `base32` reikšmė atsakyme | `BASE32_VALUE_PATTERN` – taisyklingo ilgio Base32 |
| `base32` `{id}` užklausoje | `BASE32_ID_PATTERN` – `=` ir taisyklingo ilgio Base32, iki 513 simbolių |
| `string` `{id}` užklausoje | `DECLARED_ID_PATTERN` / `EQUALS_ID_PATTERN` – vienas kelio segmentas, 1–512 simbolių |

Visi jie reikalauja **bent vieno simbolio**. Copilot recenzija kelis kartus
atkreipė dėmesį, kad Spinta techniškai priima tuščią rakto reikšmę:
`encode_base32("")` grąžina `""`, `String.load("")` pavyksta, todėl objektas su
tuščiu šaltinio raktu būtų grąžinamas su `_id: ""` ir pasiekiamas `/=`, o aprašas
tokį atsakymą ir užklausą atmestų.

## Sprendimas

Tuščias deklaruotas identifikatorius **neaprašomas**: šablonai lieka reikalaujantys
bent vieno simbolio, o recenzijos siūlymas juos atlaisvinti nevykdomas.

- Tuščias pirminis raktas yra sugadinti šaltinio duomenys, o ne identifikatorius,
  kurį duomenų paslauga teikia naudotojams.
- `/=` nėra prasmingas objekto adresas, jo klientas negali sąmoningai naudoti.
- Aprašas apibrėžia paslaugos sutartį. Leidus tuščią reikšmę, vartai praleistų
  užklausas `/=`, o schema nustotų sakyti, kad identifikatorius yra reikšmė.

## Svarstytos alternatyvos

1. **Paskutinę Base32 grupę ir `string` ilgį padaryti neprivalomus, kad tiktų
   tuščia reikšmė.** Aprašas tiksliai atspindėtų tai, ką Spinta techniškai
   priima, bet įteisintų sugadintus duomenis kaip paslaugos dalį ir leistų per
   vartus užklausas `/=`. Atmesta.
2. **Drausti tuščią raktą pačioje Spintoje** (`Base32.load`, `String.load`
   deklaruotam `_id`). Tai būtų tikslesnis sprendimas duomenų lygmeniu, bet
   keičia Spintos elgseną su esamais duomenimis ir nėra OAS generavimo apimtis.
   Neatlikta; gali būti atskira užduotis.

## Pasekmės

- Jei šaltinyje atsirastų objektas su tuščiu pirminiu raktu, vartų atsakymo
  validacija jį atmestų, o per vartus jo nebūtų galima pasiekti. Tai laikoma
  duomenų kokybės problema, sprendžiama šaltinyje.
- Recenzento siūlymas atlaisvinti `BASE32_VALUE_PATTERN`, `BASE32_ID_PATTERN`,
  `DECLARED_ID_PATTERN` ar `EQUALS_ID_PATTERN` tuščiai reikšmei nevykdomas –
  nurodomas šis ADR.
