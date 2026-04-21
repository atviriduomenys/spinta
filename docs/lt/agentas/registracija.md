# Registracija ir diegimas į vartus

:::{note}
Šiame skyriuje aprašytas registracijos procesas kataloge ir API publikavimas vartuose.
Detalus duomenų katalogo naudojimas aprašytas
[Atvirų duomenų vadovo dokumentacijoje](https://atviriduomenys.readthedocs.io/latest/ "Atvirų duomenų dokumentacija")
— ten pateikta informacija gali neatitikti dabartinės katalogo versijos.
:::

## Kam reikalinga registracija?

Duomenų kataloge agentas atitinka vieną fizinį Spinta agentą, kuris aptarnauja
konkrečią **duomenų paslaugą**. Kataloge kiekvienas agentas turi kelias **aplinkas**,
kurios referuoja į konkrečius fizinius agento instance'us, sudiegtus vadovaujantis
šia dokumentacija:

| Aplinka kataloge | Atitinka | Paskirtis |
|------------------|----------|-----------|
| **Testavimo** | DSA testavimo agentas | DSA pakeitimų tikrinimas prieš diegiant į produkciją |
| **Gamybinė** | PROD agentas | Realių duomenų teikimas galutiniams vartotojams |
| **Vystymo** | — | Skirta `spinta inspect` komandų leidimui; šioje dokumentacijoje neaprašyta |

Registracija reikalinga dviem tikslams:

1. **Spinta sinchronizacija su katalogu** — agentas prisijungia prie katalogo ir
   patvirtina, kad jis pasiekiamas.
2. **API publikavimas vartuose** — iš katalogo generuojamas OAS failas, kuris
   perduodamas VSSA API vartų konfigūravimui.

## Prielaidos

Prieš registruojant agentą kataloge, būtina:

- Turėti **duomenų išteklių tvarkytojo** teises organizacijos lygyje kataloge
- Sėkmingai užbaigti [lokalų testavimą](duomenų-gavimo-testavimas.md)

## Agento sukūrimas kataloge

Agentai registruojami organizacijos lygyje kataloge.

<!-- TODO: pridėti agento registravimo formos screenshot (Pavadinimas + Rūšis laukeliai) -->

1. Prisijunkite prie [duomenų katalogo](https://data.gov.lt).
2. Duomenų išteklių sąraše susiraskite savo organizaciją.
3. Atverkite skirtuką **„Agentai"**.
4. Spauskite **„Pridėti agentą"** ir užpildykite:

| Laukas | Aprašas |
|--------|---------|
| **Pavadinimas** | Agento pavadinimas (pvz. `ivpk-spinta-prod`) |
| **Rūšis** | Pasirinkite **Spinta** — tai nurodo, kad bus naudojamas Spinta sprendimas |

5. Spauskite **„Redaguoti"** / **„Sukurti"**.

## Aplinkų kūrimas

Sukūrus agentą, reikia sukurti **aplinkas** — po vieną kiekvienam fiziniam
agento instance'ui. Aplinkos matomos agento kortelėje:

```{figure} ../static/Agento_aplinku_sarasas.png
:width: 100%
:alt: Agento aplinkų sąrašas kataloge
```

Naują aplinką kuriate mygtuku **„Pridėti aplinką"**:

```{figure} ../static/agento_aplinkos_redagavimo_langas.png
:width: 80%
:alt: Aplinkos kūrimo forma kataloge
```

| Laukas | Aprašas |
|--------|---------|
| **Aplinka** | `Testavimo`, `Gamybinė` arba `Vystymo` — aplinka, kurioje diegiamas fizinis agentas |
| **Agento adresas** | Agento URL (pvz. `https://spinta.institucija.lt`). Jei nurodytas vartų adresas — vidinis adresas, kurį mato API vartai. Jei vartai nenurodyti — išorinis adresas |
| **Autorizacijos serverio adresas** | Nepildome |
| **API vartų serverio adresas** | Nepildome |
| **Atviri duomenys publikuojami Saugykloje** | Nepažymime (DVMS kontekste nenaudojama) |
| **Agento aplinka įjungta** | Pažymima pagal nutylėjimą — palikti įjungtą |

## Credentials failas ir Spinta sinchronizacija

Iš karto po aplinkos sukūrimo kataloge rodomas `credentials.cfg` failas su prisijungimo
duomenimis. **Jis rodomas tik vieną kartą** — antrą kartą atidarę aplinkos žurnalą,
`secret` reikšmės nebematysite.

Failo turinys atrodo maždaug taip:

```ini
[default]
server = https://get-test.data.gov.lt
resource_server = https://test.data.gov.lt
organization = datasets/gov/ivpk/
organization_type = gov
client_id = af3fd535-9ac2-40a6-8cb1-a22f3ff17a93
client = af3fd535-9ac2-40a6-8cb1-a22f3ff17a93
secret = ******
scopes =
    uapi:/datasets/gov/vssa/dcat/Agent/:getall
    uapi:/datasets/gov/vssa/dcat/Agreement/:getall
    uapi:/datasets/gov/vssa/dcat/Agreement/:patch
    uapi:/datasets/gov/vssa/dcat/AgreementFile/:getone
    uapi:/datasets/gov/vssa/dcat/Dataset/:getall
    uapi:/datasets/gov/vssa/dcat/Dataset/:create
    uapi:/datasets/gov/vssa/dcat/Distribution/:getall
    uapi:/datasets/gov/vssa/dcat/Distribution/:create
    uapi:/datasets/gov/vssa/dcat/Dsa/:getone
    uapi:/datasets/gov/vssa/dcat/Dsa/:create
    uapi:/datasets/gov/vssa/dcat/Dsa/:patch
    uapi:/datasets/gov/vssa/dcat/Version/:getall
```

Išsaugokite šį failą Spinta agento serveryje:

```bash
mkdir -p ~/.config/spinta
nano ~/.config/spinta/credentials.cfg
```

### Spinta sync paleidimas

Perkrovę Spinta agentą, paleiskite sinchronizacijos komandą:

```bash
env/bin/spinta sync
```

Svarbiausias rezultatas — žinutė apie sėkmingą ryšį su katalogu:

```
✓ Connection with Catalog established successfully!
```

Po jos gali sekti klaida apie nerastą duomenų paslaugą — tai normalu, kol
duomenų paslauga dar nesusieta kataloge, ir netrukdo tolesniam darbui:

```
Data Service by the name `...` not found in catalog.
AgentRelatedDataServiceDoesNotExist
```

:::{important}
Registracija laikoma sėkminga, kai gausite `✓ Connection with Catalog established successfully!`
Tai patvirtina, kad agentas pasiekia katalogą ir credentials failas sukonfigūruotas teisingai.
:::

Tai taip pat bus matoma aplinkos žurnale (mygtukas **„Žurnalas"** aplinkų sąraše) —
**„Užklausų istorija"** su sėkminga užklausa:

```{figure} ../static/Agento_zurnalo_langas.png
:width: 100%
:alt: Aplinkos žurnalas — Užklausų istorija po sėkmingo spinta sync
```

## OAS diegimas į vartus

**OAS** (OpenAPI Specification) — standartinis API aprašo formatas (JSON/YAML),
kurį Gravitee vartai naudoja API valdymui (maršrutizavimui, prieigos kontrolei,
dokumentacijai).

Dabartinė OAS diegimo eiga yra **rankinė**:

| # | Žingsnis | Atlikėjas |
|---|----------|-----------|
| 1 | Manifest failas konvertuojamas atgal į DSA formatą ir įkeliamas į katalogą | Institucija |
| 2 | Kataloge generuojamas OAS failas | Katalogo sistema |
| 3 | OAS failas perduodamas VSSA | Institucija |
| 4 | VSSA rankiniu būdu užregistruoja API vartuose | VSSA |

:::{important}
**Planuojamas automatizavimas:** Dabartinis rankinis procesas yra laikinas.
Planuojama, kad Spinta automatiškai sinchronizuos DSA su katalogu, o Gravitee
nuskaitys OAS automatiškai.
:::

### Kurioje aplinkoje registruoti?

- **PROD vartai** — registruojamas **Gamybinės** aplinkos agentas
- **TEST vartai** — registruojamas **Testavimo** (DSA testavimo) aplinkos agentas

(vartu-tipai)=
### Vartų tipai

:::{important}
**Vidiniai vartai** — tik institucijoms, kurios yra **KVTC klientės** ir įtrauktos į
[SVDPT naudotojų sąrašą](https://www.e-tar.lt/portal/lt/legalAct/aea15050a53411e8acb39f2e6db7935b/asr)
pagal LR Vyriausybės nutarimą. Buvimas valstybiniame duomenų centre (VDC)
**negarantuoja** galimybės jungtis per SVDPT — institucija privalo būti atskira
KVTC klientė.

**Išoriniai vartai** — visos kitos institucijos: kurių IS yra komerciniame debesyje,
išorinio teikėjo infrastruktūroje, arba valstybiniame DC, bet nesančios SVDPT
naudotojų sąraše. Papildomos KVTC/SVDPT tinklo konfigūracijos nereikia.
:::

| Vartų tipas | Kada naudojama | Prieiga |
|-------------|----------------|---------|
| **Vidiniai vartai** | IS KVTC kliento aplinkoje, institucija SVDPT naudotojų sąraše | Tik SVDPT tinkle |
| **Išoriniai vartai** | IS ne SVDPT tinkle (VDC be SVDPT, komercinis debesis, išorinis teikėjas) | Internetas |

:::{note}
Jei nesate tikri, kurie vartai taikomi jūsų institucijai, kreipkitės į VSSA.
:::

Dėl OAS diegimo į vartus instrukcijų kreipkitės į VSSA.

Jei naudojami **vidiniai vartai** — reikalinga papildoma tinklo konfigūracija.
Žr. [Tinklo konfigūravimas (KVTC/SVDPT)](tinklo-konfigūravimas.md).
