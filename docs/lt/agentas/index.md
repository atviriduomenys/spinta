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

## Palaikomi šaltiniai

Spinta Agentas palaiko šiuos duomenų šaltinių tipus:

- **WSDL/SOAP** — dažniausiai naudojamas valstybės institucijų paslaugoms
- **SQL** — reliacinės duomenų bazės (PostgreSQL, MySQL, MSSQL ir kt.)
- **XML** — XML formato failai arba paslaugos
- **JSON** — JSON formato failai arba REST API

## UDTS duomenų paslauga

Spinta Agentas leidžia institucijai iš esamų duomenų šaltinių sukurti standartizuotą
**UDTS duomenų paslaugą** — be jokių pakeitimų šaltinio sistemoje. Duomenų vartotojas
(kita institucija ar sistema) gauna prieigą per API vartus (Gravitee) naudodamas
UDTS standartą.

```{figure} ../static/udts-paslauga.png
:width: 100%
:alt: UDTS duomenų paslaugos kūrimas naudojant SPINTA agentą
:target: ../_static/udts-paslauga.png

UDTS duomenų paslauga — nuo šaltinio iki duomenų vartotojo (spustelėkite norėdami padidinti)
```

Institucija valdo savo infrastruktūrą (Spinta agentus, Redis, Reverse proxy), o
VSSA valdo Gravitee vartus ir Access Manager. Techniniai diegimo reikalavimai
aprašyti skyriuose [Agento paruošimas](agento-paruošimas.md) ir
[Agento diegimas](diegimas/index.rst).

(kiek-agentu-reikia)=
## Kiek agentų reikia?

Kiekviena institucija turi turėti bent **du agentus** — nepriklausomai nuo IS aplinkų
skaičiaus. Agentų skaičius priklauso nuo to, kokio tipo duomenys bus skelbiami kataloge:

| Agento paskirtis | Duomenų tipas | Paskirtis / panaudos atvejis | Autorizacijos serveris | Privalomas DVMS projekto apimtyje |
|---|---|---|---|---|
| PROD agentas (duomenų teikimui) | PROD | Jungiamas su produkcine vartų aplinka<br/>ir teikia PRODUKCINIUS IS duomenis<br/>galutiniams vartotojams | Vartų PROD AM | Taip |
| TEST agentas (DSA testavimo agentas) | PROD | Institucija tikrina PRODUKCINIŲ<br/> duomenų DSA pakeitimus<br/>prieš keliant DSA pakeitimus<br/> į PRODUKCINIŲ duomenų  PROD agentą | Spinta AM \* | Taip |
| PROD agentas (duomenų teikimui) | DEMO | Jungiamas su produkcine vartų aplinka<br/>ir teikia DEMO IS duomenis<br/>galutiniams vartotojams | Vartų PROD AM | Tik jei kataloge<br/>skelbiami DEMO duomenys |
| TEST agentas (DSA testavimo agentas) | DEMO | Institucija tikrina DEMO<br/> duomenų DSA pakeitimus<br/>prieš keliant DSA pakeitimus<br/> į DEMO duomenų  PROD agentą | Spinta AM \* | Tik jei kataloge<br/>skelbiami DEMO duomenys |
| Agentas SPINTA versijos testavimui | PROD arba DEMO | Lokalus — skirtas patikrinti<br/>Spinta versijų suderinamumą<br/>prieš atnaujinant | Vidinis Spinta AM \* | Ne (rekomenduojamas) |

:::{important}
\* Vidinis Spinta AM rekomenduojamas tol, kol kataloge neužbaigtas sutarčių sudarymo ir valdymo modulis. Atsiradus galimybei sudaryti testavimo sutartį su savimi, agentus bus galima prijungti prie centralizuoto Gravitee TEST aplinkos AM.
:::

:::{warning}
**Privaloma taisyklė** (taikoma kai vienas agentas aptarnauja kelias IS): vienas agentas
negali teikti skirtingo tipo duomenų. Jei kelios IS jungiamos per vieną agentą, visos
privalo naudoti to paties tipo duomenis — PROD ir DEMO duomenys privalo būti atskirti
skirtinguose agentuose.
:::

Vienas agentas gali aptarnauti **vieną arba kelis duomenų šaltinius** — (tai nustatoma
manifesto faile sujungus DSA). Kiek agentų reikės institucijai priklauso nuo:

- **Šaltinių skaičiaus ir tipo** — kiekviena IS paprastai reikalauja atskiro agento
- **Apkrovos ir našumo reikalavimų** — intensyviai naudojamas IS geriau izoliuoti
- **Administravimo sudėtingumo** — atskiri agentai lengviau prižiūrimi

:::{note}
Rekomenduojama: **vienas agentas vienai IS**. Kelių šaltinių (IS, DB) grupavimas viename agente
galimas tik kai šaltiniai yra labai primityvūs (maža apkrova, paprasta struktūra,
administruoja ta pati komanda) — tokiu atveju tai gali supaprastinti infrastruktūrą.
:::

## Diegimo eiga

Žemiau pavaizduota rekomenduojama diegimo eiga. Kiekvienas žingsnis yra išsamesnis
aprašytas atskiruose skyriuose.

```
PARENGIAMIEJI DARBAI 
│
├─► 1. Paruošiamas manifest (sDSA) - (paprastai šis žingsnis atliekamas po spintos diegimo, tačiau įmanoma iš PC)
│       • Generuojamas su: spinta inspect <šaltinis>
│       • Tikslinamas ir pildomas veiklos žmonių
│       • Suderinamas su UDTS standartu
│       • ⚠️  Be  manifesto SPINTA agentas neveiks
│
└─► 2. Priimami infrastruktūros sprendimai
        • Diegimo būdas: OS arba Docker
        • Agentų skaičius
        • Serverio resursai

──────────────────────────────────────────────

TECHNINIS DIEGIMAS
│
├─► 3. Aplinkos paruošimas
│       • OS paketų diegimas (Docker, Redis, nginx*, curl ir kt.)
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
│       • Reverse proxy (nginx) konfigūravimas(jei nenaudojami kiti sprendimai)
│
├─► 7. Autentifikacija
│       • OAuth kliento sukūrimas testavimui arba prijungimas prie nutolusio authorizacijos serverio
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
prielaida** prieš pradedantspinta agento testavimą. Tai CSV formato failas,
kuriame aprašyta, kokius duomenis agentas teiks ir kaip juos pasiekti.
:::

sDSA paruošimo eiga:

1. **Generavimas** — `spinta inspect` komanda automatiškai nuskaito šaltinio struktūrą
   ir sukuria pradinį sDSA
2. **Tikslinimas** — veiklos žmonės patikrina ir papildo sugeneruotus failus
   (pavadinimai, tipai, ryšiai tarp modelių)
3. **Techninis tikrinimas** — sDSA konvertuojamas į manifest, atliekami lokalūs duomenų gavimo testai. 
4. **DSA Publikavimas** — įsitikinus, kad DSA suderinamas su agentu (įmanoma teikti duomenis UDTS formatu), failas teikiamas VSSA tikrinimui/tvirtinimui. Gavus patvirtinimą DSA keliamas i kataloga, generuojami OAS ir Gravitee config. 
4. **Duomenų paslaugos publikavimas** — Iš katalogo generuojami OAS ir Gravitee config. Jie perduodami VSSA administratoriams, kurie naudodamiesi failais sukonfioguruoja duomenų paslaugas vartuose.



Daugiau informacijos apie sDSA ruošimą:
[Duomenų šaltiniai — DSA 1.1](https://ivpk.github.io/dsa/1.1/saltiniai.html "DSA 1.1 specifikacija")



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
- **Manifest** — apjungtas DSA failas, sudarytas iš vieno arba kelių sDSA naudojamas SPINTA agento. Tai vis dar DSA
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
| 1 | **sDSA generavimas** | Institucijos administratorius | `spinta inspect` | Pradinis sDSA failas su šaltinio struktūra |
| 2 | **sDSA tikslinimas** | Veiklos žmonės  | Rankinis redagavimas | Patikslintas sDSA: pavadinimai, tipai, prieinamumo lygiai |
| 3 | **Testavimas su agentu** | Institucijos administratorius | `spinta run`, `spinta copy` | Veikiantis sDSA — duomenys grąžinami teisingai |
| 4 | **Rengimas publikavimui** |  Veiklos žmonės + Admin | Rankinis redagavimas | Išvalytas sDSA: pašalinti connection string'ai ir šaltinio duomenys |
| 5 | **Kėlimas į Katalogą** | Administratorius | Katalogas (data.gov.lt) | sDSA Kataloge — generuojamas OAS ir Gravitee config |
| 6 | **API diegimas vartuose** | VSSA (Gravitee admin) | Gravitee | Veikianti duomenų paslauga per API vartus |

**Susijusios nuorodos:**

- [DSA 1.1 specifikacija](https://ivpk.github.io/dsa/1.1/ "Duomenų struktūros aprašas — oficiali specifikacija") — pilnas DSA formato aprašas
- [DSA šaltiniai](https://ivpk.github.io/dsa/1.1/saltiniai.html "DSA šaltinių aprašas") — kaip aprašyti duomenų šaltinius DSA faile
- [UAPI/UDTS specifikacija](https://ivpk.github.io/uapi/ "UAPI specifikacija") — duomenų teikimo sąsajos standartas
