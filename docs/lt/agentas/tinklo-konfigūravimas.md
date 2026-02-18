# Tinklo konfigūravimas ir registracija

## Tinklo reikalavimai

Institucija, kuri diegiasi Spinta, turi pasirūpinti tinklo konfigūracija:

- **Vartai → Spinta**: vieta, kur sudiegta Spinta, turi būti pasiekiama iš Gravitee vartų
- **Spinta → Katalogas**: Spinta turi turėti prieigą prie viešojo interneto (**443 portas**) užklausoms į `data.gov.lt` katalogą
- **Spinta → Šaltiniai**: Spinta turi pasiekti duomenų šaltinius (API, DB, failus)

:::{important}
Lokalus testavimas turi būti **sėkmingai užbaigtas** prieš pradedant registraciją
ir diegimą į vartus. Gravitee veikia kaip proxy, todėl lokalus testavimas yra
pakankamas funkcionalumo patikrinimui prieš jungiantis prie SVDPT.
:::

## Registracija kataloge

Kai lokalus testavimas sėkmingas, agentas registruojamas duomenų kataloge.
Registracija vykdoma organizacijos, kuriai priklauso naudotojas, puslapyje.

1. Prisijunkite prie [duomenų Katalogo](https://data.gov.lt).
2. Viršutiniame dešiniajame kampe užveskite pelę ant savo naudotojo vardo ir
   pasirinkite **„Mano organizacijos duomenų ištekliai"**.
3. Atverkite skirtuką **„Agentai"**.
4. Spauskite **„Pridėti Agentą"**.
5. Užpildykite formos laukus ir spauskite **„Sukurti"**:

| Laukas | Aprašas |
|--------|---------|
| **Pavadinimas** | Vartotojui matomas agento pavadinimas |
| **Rūšis** | Pasirinkite **Spinta** — bus sugeneruotos `credentials.cfg` ir `config.yml` konfigūracijos |
| **Duomenų paslauga** | Duomenų paslauga, kuriai agentas bus priskirtas (jei nenurodyta — sukuriama automatiškai) |
| **Aplinka** | `Testavimo` arba `Gamybinė` |
| **Agento adresas** | Vidinis agento adresas, kurį mato API vartai; jei vartai nenurodyti — išorinis adresas |
| **Autorizacijos serverio adresas** | AM serverio adresas metaduomenų sinchronizacijai |
| **API vartų serverio adresas** | Gravitee vartų adresas |

6. Atlikus registraciją, pateikiamos dvi konfigūracijos (Spinta rūšiai):
   - `credentials.cfg` — prisijungimui prie Katalogo (Spinta → Katalogas)
   - `config.yml` — agento konfigūracijos šablonas (Spinta → Duomenų šaltinis)

:::{caution}
Sukūrus agentą, pateikiamas slaptas prisijungimo raktas. **Dėl saugumo jis rodomas
tik vieną kartą — būtinai išsaugokite.**
:::

## OAS diegimas į vartus

**OAS** (OpenAPI Specification) — tai standartinis API aprašo formatas (JSON/YAML),
kuriame deklaruojami visi Spinta Agento teikiami duomenų endpoint'ai, jų parametrai
ir autorizacijos reikalavimai. Gravitee vartai naudoja šį aprašą API valdymui —
maršrutizavimui, prieigos kontrolei ir dokumentacijai.

Dabartinė OAS diegimo eiga yra rankinė:

1. **DSA → Katalogas** — institucija įkelia DSA į duomenų Katalogą
2. **Katalogas → Failas** — Katalogas iš DSA sugeneruoja OAS failą
3. **Failas → VSSA** — institucija perduoda OAS failą VSSA
4. **VSSA → Gravitee** — VSSA sukonfigūruoja Gravitee vartus pagal OAS

:::{important}
**Planuojamas automatizavimas:** Dabartinis rankinis procesas yra laikinas.
Planuojama, kad:

- Spinta automatiškai sinchronizuos DSA su Katalogu
- Katalogas automatiškai eksportuos OAS į GitHub repozitoriją
- Gravitee nuskaitys OAS iš GitHub ir susikonfigūruos automatiškai

<!-- TODO: pridėti nuorodas į GitHub issues kai bus sukurti -->
:::

Spinta Agento paslauga pasiekiama per Gravitee vartus. Institucija turi nuspręsti,
per kokius vartus bus teikiami duomenys.

### Vartų tipai

| Vartų tipas | Paskirtis | Prieiga |
|-------------|-----------|---------|
| **Vidiniai vartai** | Duomenys tik tarp valdžios institucijų | Tik SVDPT tinkle |
| **Išoriniai vartai** | Duomenys viešai arba autorizuotiems išoriniams gavėjams | Internetas |

:::{note}
Sprendimą dėl vartų tipo priima institucija kartu su VSSA, atsižvelgiant į
duomenų jautrumo lygį ir gavėjų ratą.
:::

<!-- TODO: papildyti OAS diegimo į vartus instrukcijomis -->

Dėl OAS diegimo į vartus instrukcijų kreipkitės į VSSA.

## Tinklo konfigūracija vidiniams vartams (KVTC/SVDPT)

Jei institucija teikia duomenis per **vidinius vartus**, reikalinga papildoma
tinklo konfigūracija per Saugųjį valstybės duomenų perdavimo tinklą (SVDPT).
Sujungimas reikalauja **abipusio** konfigūravimo — tiek institucijos, tiek VSSA pusėje.

### Techniniai reikalavimai

- **Duomenų perdavimas**: komunikacija tarp VSSA vartų ir institucijos VM privalo vykti **HTTPS** protokolu
- **Reverse Proxy**: institucija privalo pasirūpinti Reverse Proxy (pvz., Nginx, WAF arba F5), kuris terminuotų HTTPS srautą ir nukreiptų jį į Spinta **8000** portą
- **SSL sertifikatai**: institucija pati atsakinga už SSL sertifikatų diegimą savo pusėje (pvz., Let's Encrypt)
- **Portai**: https, ping, http — abipusiškai reikalingas praleidimas

:::{note}
**Pastaba administratoriams:** Jei Spinta agentas veikia už ugniasienės ar Load
Balancer'io, įsitikinkite, ar įrenginys atlieka pilną paketų terminavimą (**SNAT**).
Nuo to priklauso, ar išeinančių paketų šaltinio adresas VSSA pusėje bus matomas
kaip originalus VM IP, ar kaip tinklo įrenginio IP. Nesutapus adresams, užklausos
bus atmetamos.
:::

### DNS konfigūravimas (/etc/hosts)

SVDPT tinkle viešieji DNS įrašai neregistruojami, tačiau VSSA Load Balancer srautą
nukreipia tik pagal DNS vardus. Todėl Spinta VM mašinoje **privaloma** sukonfigūruoti
`/etc/hosts` failą:

```bash
sudo tee -a /etc/hosts << 'EOF'

# TEST vartai
10.246.4.181 test-apigw.gov.lt
10.246.4.181 am.test-apigw.gov.lt

# PROD vartai
10.246.4.182 apigw.gov.lt
10.246.4.182 am.apigw.gov.lt
EOF
```

| IP adresas | Hostname | Aplinka |
|------------|----------|---------|
| 10.246.4.181 | test-apigw.gov.lt | Testiniai vartai |
| 10.246.4.181 | am.test-apigw.gov.lt | Testinis Access Manager |
| 10.246.4.182 | apigw.gov.lt | Produkciniai vartai |
| 10.246.4.182 | am.apigw.gov.lt | Produkcinis Access Manager |

### KVTC sujungimo užsakymas

Sujungimas per SVDPT užsakomas pateikiant standartinę KVTC formą el. paštu:
**pagalba@kvtc.lt**

Formą galima gauti kreipiantis į VSSA. Formoje pildoma:

**Institucijos pusė (2 skyrius)** — institucija nurodo:
- Savo vidinį Spinta agento VM IP adresą (`10.10.X.X`)
- Poreikį pasiekti VSSA vartus (TEST: `10.246.4.181`, PROD: `10.246.4.182`)

**Institucijos pusė (3 skyrius)** — institucija deklaruoja savo resursą:
- Informacinio ištekliaus pavadinimas: `SPINTA AGENTAS X` arba `INSTITUCIJA LB`
- VM IP adresas institucijos tinkle

:::{note}
Užpildytą formą institucija turi persiųsti VSSA — sujungimas laikomas baigtu tik
tada, kai abi pusės (institucija ir VSSA) įgyvendina nustatymus pagal KVTC paraiškas.
:::
