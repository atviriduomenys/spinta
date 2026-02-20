# Spinta Agentas

:::{note}
Spinta įrankis naudojamas ir atvirų duomenų srityje (duomenų katalogui, metaduomenų
valdymui — žr. [Atvirų duomenų dokumentacija](https://atviriduomenys.readthedocs.io/latest/ "Atvirų duomenų dokumentacija")).
Ši dokumentacija skirta išskirtinai **Spinta Agento** komponentui, kurio paskirtis —
implementuoti [UAPI](https://ivpk.github.io/uapi/ "UAPI specifikacija") (dar žinomą
kaip UDTS — Universalioji Duomenų Teikimo Sąsaja) duomenų teikėjų
pusėje.
:::

## Kas yra Spinta Agentas?

Spinta Agentas yra programinė įranga, kuri leidžia institucijai publikuoti duomenis
iš esamų sistemų (SOAP/WSDL paslaugų, duomenų bazių, XML/JSON šaltinių)
[UAPI](https://ivpk.github.io/uapi/ "UAPI specifikacija") formatu — be poreikio
keisti pačią sistemą. Agentas veikia kaip tarpininkas tarp jūsų duomenų šaltinio ir
duomenų vartų (Gravitee).

## Infrastruktūros schema

<!-- TODO: pakeisti į infrastruktūros diagramą (Agent A/B, Redis, Reverse proxy, Vartai) -->

<p align="center">
  <img src="../_static/docker-pavyzdys.png" alt="Spinta infrastruktūros schema">
  <br>
  <em>Spinta infrastruktūros komponentai (placeholder — bus pakeista)</em>
</p>

Pagrindiniai infrastruktūros komponentai:

| Komponentas | Paskirtis | Kas atsakingas |
|-------------|-----------|----------------|
| **Spinta Agentas** | Konvertuoja šaltinio duomenis į UDTS formatą | Institucija |
| **Redis** | Saugo raktų žemėlapį (keymap) — susiejimą tarp vidinių ir išorinių ID. Gali būti dalinamas tarp kelių agentų | Institucija |
| **Reverse proxy** (nginx) | HTTPS užtikrinimas, viešas prieigos taškas | Institucija |
| **Vartai** (Gravitee) | Duomenų paslaugų valdymas ir prieiga | VSSA |

:::{note}
Viena institucija gali turėti kelis Spinta agentus — kiekvienam šaltiniui arba
šaltinių grupei. Visi agentai gali naudoti bendrą Redis instanciją.
:::

## Kiek agentų reikia?

Vienas Spinta Agentas gali aptarnauti **vieną arba kelis duomenų šaltinius** — tai
nustatoma manifesto faile (DSA). Agentų skaičius priklauso nuo:

- Šaltinių skaičiaus ir tipo
- Apkrovos ir našumo reikalavimų
- Administravimo sudėtingumo

Dažniausiai rekomenduojama: **vienas agentas vienam šaltiniui** (šaltinis — tai
informacinė sistema arba duomenų bazė), tačiau galimas ir kelių šaltinių grupavimas
viename agente.

## Diegimo eiga

Žemiau pavaizduota rekomenduojama diegimo eiga. Kiekvienas žingsnis yra išsamesnis
aprašytas atskiruose skyriuose.

```
PARENGIAMIEJI DARBAI (prieš techninį diegimą)
│
├─► 1. Paruošiamas manifest (sDSA)
│       • Generuojamas su: spinta inspect <šaltinis>
│       • Tikslinamas ir pildomas veiklos žmonių
│       • Suderinamas su UDTS standartu
│       • ⚠️  Be patvirtinto manifesto negalima tęsti
│
└─► 2. Priimami infrastruktūros sprendimai
        • Diegimo būdas: OS arba Docker
        • Agentų skaičius
        • Serverio resursai

──────────────────────────────────────────────

TECHNINIS DIEGIMAS
│
├─► 3. Aplinkos paruošimas
│       • OS paketų diegimas (Docker, curl ir kt.)
│       • Spinta sisteminių naudotojo sukūrimas
│
├─► 4. Spinta diegimas
│       • Python virtualenv
│       • pip install iš requirements failo
│
├─► 5. Konfigūravimas
│       • config.yml paruošimas
│       • Redis paleidimas
│       • Manifest (sDSA) įkėlimas
│
├─► 6. Serviso paleidimas
│       • SystemD serviso konfigūravimas (OS diegimas)
│       • arba Docker Compose paleidimas
│       • Reverse proxy (nginx) konfigūravimas
│
├─► 7. Autentifikacija
│       • OAuth kliento sukūrimas testavimui
│
└─► 8. Lokalus testavimas
        • Token gavimas
        • Duomenų gavimo tikrinimas
        • ⚠️  Privaloma sėkmingai užbaigti prieš registraciją

──────────────────────────────────────────────

REGISTRACIJA IR PALEIDIMAS
│
├─► 9. Registracija kataloge
│
├─► 10. OAS diegimas į vartus
│        • Numatyta: vidiniai vartai (IS valstybiniame DC / SVDPT tinkle)
│        • Išimtis: išoriniai vartai (IS ne valstybiniame DC)
│        │
│        ├─► [Vidiniai vartai — numatyta] → 11. Tinklo konfigūracija (KVTC/SVDPT)
│        │       • Papildomas tinklo sujungimas per SVDPT
│        │       • KVTC forma → VSSA
│        │
│        └─► [Išoriniai vartai — išimtis] → diegimas baigtas
│                • Papildomos tinklo konfigūracijos nereikia
│
└─► ✅ Agentas veikia per Gravitee vartus
```

## Apie manifestą (sDSA)

:::{important}
Manifesto failas (sDSA — Šaltinio Duomenų Struktūros Aprašas) yra **privaloma
prielaida** prieš pradedant techninį agento diegimą. Tai CSV formato failas,
kuriame aprašyta, kokius duomenis agentas teiks ir kaip juos pasiekti.
:::

sDSA paruošimo eiga:

1. **Generavimas** — `spinta inspect` komanda automatiškai nuskaito šaltinio struktūrą
   ir sukuria pradinį sDSA
2. **Tikslinimas** — veiklos žmonės patikrina ir papildo duomenų semantiką
   (pavadinimai, tipai, ryšiai tarp modelių)
3. **Suderinimas** — sDSA suderinamas su UDTS standartu ir patvirtinamas

Daugiau informacijos apie sDSA ruošimą:
[Duomenų šaltiniai — DSA 1.1](https://ivpk.github.io/dsa/1.1/saltiniai.html "DSA 1.1 specifikacija")

## Palaikomi šaltiniai

Spinta Agentas palaiko šiuos duomenų šaltinių tipus:

- **WSDL/SOAP** — dažniausiai naudojamas valstybės institucijų paslaugoms
- **SQL** — reliacinės duomenų bazės (PostgreSQL, MySQL, MSSQL ir kt.)
- **XML** — XML formato failai arba paslaugos
- **JSON** — JSON formato failai arba REST API

Plačiau apie kiekvieno šaltinio konfigūravimą žr. skyriuje
[Šaltinių konfigūravimas](šaltinių-konfigūravimas.md).

## DSA kūrimas ir kelionė

DSA (Duomenų Struktūros Aprašas) yra pagrindinis artefaktas, kuriuo grindžiamas
visas Spinta Agento darbas. Jis kuriamas, tobulinamas, testuojamas ir galiausiai
keliamas į Katalogą — iš kurio generuojami API vartų konfigūracijos failai.

Vienas agentas gali aptarnauti kelis sDSA failus. Vėliau jie apjungiami į vieną
**manifestą** (nurodytas `config.yml` faile):

:::{note}
**sDSA vs Manifest**

- **sDSA** (Šaltinio DSA) — vieno duomenų šaltinio struktūros aprašas (CSV failas).
  Generuojamas su `spinta inspect`, tikslinamas veiklos žmonių.
- **Manifest** — apjungtas DSA failas, sudarytas iš kelių sDSA. Tai vis dar DSA
  formatu, bet apimantis kelis šaltinius. Dėl planuojamos sinchronizacijos su
  Katalogu, manifestas turi būti **vienas** (ne keli).
- **config.yml** — Spinta agento konfigūracijos failas. Jame, be kita ko, nurodomas
  kelias į manifestą.

Manifesto kūrimas aprašytas skyriuje [Šaltinių konfigūravimas](šaltinių-konfigūravimas.md).
:::

Žemiau pateikta aukšto lygio schema, kaip sDSA kuriamas ir keliauja iki API vartų:

```{figure} ../static/dsa-ciklas.png
:width: 100%
:alt: DSA gyvavimo ciklo schema
:target: ../_static/dsa-ciklas.png

DSA gyvavimo ciklas — nuo generavimo iki API vartų (spustelėkite norėdami padidinti)
```

| # | Etapas | Atlikėjas | Įrankis / komanda | Rezultatas |
|---|--------|-----------|-------------------|------------|
| 1 | **sDSA generavimas** | Administratorius | `spinta inspect` | Pradinis sDSA failas su šaltinio struktūra |
| 2 | **sDSA tikslinimas** | Veiklos žmonės + Admin | Rankinis redagavimas | Patikslintas sDSA: pavadinimai, tipai, prieinamumo lygiai |
| 3 | **Testavimas su agentu** | Administratorius | `spinta run`, `spinta copy` | Veikiantis sDSA — duomenys grąžinami teisingai |
| 4 | **Rengimas publikavimui** | Administratorius | Rankinis redagavimas | Išvalytas sDSA: pašalinti connection string'ai ir šaltinio duomenys |
| 5 | **Patikrinimas** | Atsakingas asmuo | Peržiūra | Patvirtintas sDSA — paruoštas dalinimuisi |
| 6 | **Kėlimas į Katalogą** | Administratorius | Katalogas (data.gov.lt) | sDSA Kataloge — generuojamas OAS ir Gravitee config |
| 7 | **API diegimas vartuose** | VSSA (Gravitee admin) | Gravitee | Veikianti duomenų paslauga per API vartus |

**Susijusios nuorodos:**

- [DSA 1.1 specifikacija](https://ivpk.github.io/dsa/1.1/ "Duomenų struktūros aprašas — oficiali specifikacija") — pilnas DSA formato aprašas
- [DSA šaltiniai](https://ivpk.github.io/dsa/1.1/saltiniai.html "DSA šaltinių aprašas") — kaip aprašyti duomenų šaltinius DSA faile
- [UAPI/UDTS specifikacija](https://ivpk.github.io/uapi/ "UAPI specifikacija") — duomenų teikimo sąsajos standartas
