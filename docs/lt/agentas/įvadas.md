# Spinta Agento įvadas

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

Dažniausiai rekomenduojama: **vienas agentas vienai atsakomybei** (pvz., vienas SOAP
šaltinis — vienas agentas), tačiau galimas ir kelių šaltinių grupavimas viename agente.

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
│        • Vidiniai arba išoriniai vartai
│
└─► 11. Tinklo konfigūracija (jei reikalinga)
         • KVTC/SVDPT sujungimas
         • Kreiptis į atsakingą asmenį
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
[Duomenų šaltiniai — DSA](https://ivpk.github.io/dsa/draft/saltiniai.html "DSA dokumentacija")

## Palaikomi šaltiniai

Spinta Agentas palaiko šiuos duomenų šaltinių tipus:

- **WSDL/SOAP** — dažniausiai naudojamas valstybės institucijų paslaugoms
- **SQL** — reliacinės duomenų bazės (PostgreSQL, MySQL, MSSQL ir kt.)
- **XML** — XML formato failai arba paslaugos
- **JSON** — JSON formato failai arba REST API

Plačiau apie kiekvieno šaltinio konfigūravimą žr. skyriuje
[Šaltinių konfigūravimas](šaltinių-konfigūravimas.md).
