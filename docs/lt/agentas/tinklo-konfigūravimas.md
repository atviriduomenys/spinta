# Tinklo konfigūravimas

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

## Tinklo konfigūracija išoriniams vartams

Išoriniai vartai taikomi institucijoms, kurių Spinta agentas yra prieinamas per
viešąjį internetą (ne SVDPT). Šiuo atveju KVTC/SVDPT tinklo konfigūracijos
nereikia, tačiau būtina užtikrinti šiuos reikalavimus.

### Techniniai reikalavimai

- **Prieiga per internetą**: Spinta agentas turi būti pasiekiamas per **HTTPS**
- **Reverse Proxy**: institucija turi pasirūpinti Reverse Proxy (pvz., Nginx, WAF arba F5), kuris terminuotų HTTPS srautą ir nukreiptų į Spinta **8000** portą
- **SSL sertifikatai**: institucija pati atsakinga už SSL sertifikatų diegimą (pvz., Let's Encrypt)

### Gravitee IP — whitelist reikalavimas

Gravitee vartai kreipiasi į Spinta agentą iš fiksuoto IP adreso. Šis adresas privalo
būti leidžiamas ugniasienėje arba WAF taisyklėse:

| Gravitee IP | Protokolas | Portas |
|-------------|------------|--------|
| `195.182.78.26` | HTTPS | 443 |

### WAF paslauga (gov cloud / VSSA infrastruktūra)

Jei institucijos Spinta agentas yra **VSSA infrastruktūroje arba gov cloud** aplinkoje,
būtina užsakyti WAF (Web Application Firewall) paslaugą. Tai daroma per **IT pagalbos
sistemą** — formą rasite IT pagalbos portale arba kreipkitės į VSSA.

WAF konfigūracijoje nurodykite **whitelist** IP adresą `195.182.78.26`.

:::{note}
Jei nesate tikri, ar jūsų institucija naudoja gov cloud / VSSA infrastruktūrą,
arba jei turite klausimų dėl WAF užsakymo, kreipkitės į **das@vssa.lt**.
:::

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

### Kelių agentų topologija

Jei institucijoje yra **keli Spinta agentai** (pvz., kiekvienam šaltiniui atskiras
agentas), jie gali dalintis vienu išoriniu SVDPT IP adresu per centrinį Reverse
Proxy (ugniasienę, WAF arba F5).

```
SVDPT ──► Firewall / LB (vienas išorinis IP)
               │
               ├──► Reverse Proxy
               │         ├──► Spinta A (spinta-a.institucija.lt → VM1:8000)
               │         ├──► Spinta B (spinta-b.institucija.lt → VM2:8000)
               │         └──► Spinta C (spinta-c.institucija.lt → VM3:8000)
```

Gravitee vartai kiekvieną Spinta agentą pasiekia pagal **DNS vardą** (ne IP
adresą). Todėl kiekvienas agentas turi turėti unikalų hostname, kurį Reverse
Proxy nukreipia į tinkamą VM.

**Ką reikia pateikti VSSA** (kreipiantis dėl DNS konfigūravimo vartuose):

| Informacija | Pavyzdys |
|-------------|---------|
| Išorinis (SVDPT) IP adresas | `<INSTITUTION_LB_IP>` |
| Kiekvieno agento DNS vardas | `spinta-a.institucija.lt` |
| Kiekvieno agento paskirtis | Šaltinis X, Šaltinis Y |

:::{note}
DNS vardus pasirenka institucija. Vardai turi atitikti Reverse Proxy konfigūraciją —
t.y. Reverse Proxy turi maršrutizuoti pagal `Host` antraštę į atitinkamą Spinta VM.
:::

### DNS konfigūravimas (/etc/hosts)

SVDPT tinkle viešieji DNS įrašai neregistruojami, tačiau VSSA Load Balancer srautą
nukreipia tik pagal DNS vardus. Todėl Spinta VM mašinoje **privaloma** sukonfigūruoti
`/etc/hosts` failą:

```bash
sudo tee -a /etc/hosts << 'EOF'

# TEST vartai
<TEST_GATEWAY_IP> test-apigw.gov.lt
<TEST_GATEWAY_IP> am.test-apigw.gov.lt

# PROD vartai
<PROD_GATEWAY_IP> apigw.gov.lt
<PROD_GATEWAY_IP> am.apigw.gov.lt
EOF
```

| IP adresas | Hostname | Aplinka |
|------------|----------|---------|
| `<TEST_GATEWAY_IP>` | test-apigw.gov.lt | Testiniai vartai |
| `<TEST_GATEWAY_IP>` | am.test-apigw.gov.lt | Testinis Access Manager |
| `<PROD_GATEWAY_IP>` | apigw.gov.lt | Produkciniai vartai |
| `<PROD_GATEWAY_IP>` | am.apigw.gov.lt | Produkcinis Access Manager |

:::{note}
Konkrečius IP adresus (`<TEST_GATEWAY_IP>`, `<PROD_GATEWAY_IP>`) pateikia VSSA.
:::

### KVTC sujungimo užsakymas

Tinklo sujungimui per SVDPT galimi du keliai:

**1 būdas — per VSSA (paprastesnis)**

Institucija pateikia VSSA paraišką dėl tinklo sujungimo SVDPT tinkle. Tai yra
sutikimas, kad VSSA veiktų institucijos vardu bendraujant su KVTC — viena paraiška
gali apimti kelis KVTC sujungimus (TEST ir PROD aplinkas). VSSA koordinuoja visą
procesą su KVTC ir atlieka DNS registraciją.

Paraiškos forma gaunama el. paštu: **das@vssa.lt**

**2 būdas — tiesiogiai per KVTC**

Institucija pati kreipiasi į KVTC ir užpildo KVTC sujungimo formas kiekvienam
sujungimui atskirai. KVTC formos prieinamos [KVTC svetainėje](https://www.kvtc.lt)
arba kreipiantis į KVTC kontaktus el. paštu **pagalba@kvtc.gov.lt**. Užpildytas
formas reikia persiųsti ir VSSA — sujungimas laikomas baigtu tik tada, kai abi
pusės (institucija ir VSSA) įgyvendina nustatymus pagal KVTC paraiškas.

Formoje pildoma:

**Institucijos pusė (2 skyrius)** — institucija nurodo:
- Savo vidinį Spinta agento VM IP adresą (`10.10.X.X`)
- Poreikį pasiekti VSSA vartus (TEST ir PROD gateway IP — gauti iš VSSA)

**Institucijos pusė (3 skyrius)** — institucija deklaruoja savo resursą:
- Informacinio ištekliaus pavadinimas: `SPINTA AGENTAS X` arba `INSTITUCIJA LB`
- VM IP adresas institucijos tinkle
