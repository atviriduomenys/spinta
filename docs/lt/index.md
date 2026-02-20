# Spinta

:::{note}
Ši dokumentacija yra rengiama (draft). Šiuo metu prioritetas — **Spinta Agento**
diegimo instrukcijos, skirtos institucijoms, pradedančioms diegiančioms SPINTA agentą UDTS duomenų paslaugų publikavimui vartuose. Kitos dalys bus papildomos vėliau.
:::

## Kas yra Spinta?

**Spinta** — metaduomenimis valdomas (*metadata-driven*) duomenų logistikos variklis,
sukurtas [VSSA](https://vssa.lrv.lt "Valstybės skaitmeninių sprendimų agentūra").
Platforma transformuoja uždaras, heterogeniškas duomenų saugyklas į modernius,
saugius ir standartizuotus API išteklius pagal
[UDTS/UAPI](https://ivpk.github.io/uapi/ "UAPI specifikacija") standartą —
nekeičiant esamų sistemų.

**Spinta esmė — manifesto (DSA) dominavimas prieš kodą.** Sistema elgiasi pagal
metaduomenų aprašą (DSA failą), o ne užkoduotą logiką. Tai leidžia valdyti
duomenų struktūrą, prieigos teises ir šaltinius be papildomo programavimo.

- **Vystytojas:** VSSA (MIT licencija, atviras kodas)
- **Statusas:** Aktyvios plėtros stadija (v0.x) — veikimas tiesiogiai priklauso nuo manifesto tikslumo
- **Projektai:** ADP (Atvirų duomenų portalas), DVMS (Duomenų valdymo modelio sistema)

## Architektūra

Spinta sudaro penki pagrindiniai loginiai blokai:

| Blokas | Aprašas |
|--------|---------|
| **Core Engine** | Centrinė dalis, interpretuojanti Manifestą; `dispatcher` komandų vykdymui, `spyna` užklausų vertimui |
| **CLI** | Administratoriaus įrankių rinkinys — konfigūravimas, atvirkštinė inžinerija, duomenų transportavimas |
| **Auth** | OAuth2/JWT sistema; prieigos teisės per *scopes* iki lauko lygmens |
| **Backends** | Abstrakcijos sluoksnis — PostgreSQL, MongoDB, MySQL, failų sistemos |
| **API/Renderer** | Duomenų pateikimas REST ar RDF formatais per ASGI serverį |

## CLI komandos

| Komanda | Paskirtis | Kada naudojama |
|---------|-----------|----------------|
| `spinta inspect` | Nuskaito šaltinį ir sugeneruoja pradinį Manifestą (DSA) | Prieš diegimą — DSA paruošimas |
| `spinta run` | Paleidžia API serverį — **Agento režimas** | Realaus laiko duomenų teikimas |
| `spinta push` | Siunčia duomenis į Spinta saugyklą | Duomenų publikavimas į ADP |
| `spinta migrate` | Pritaiko Manifesto struktūrą fizinėje DB | Schemų pokyčių valdymas |
| `spinta copy` | Perkelia duomenis tarp backend'ų | Vidinė duomenų migracija |
| `spinta client` | Generuoja OAuth2 klientus ir raktus | Prieigos valdymas |

## Pagrindiniai veikimo režimai

### Režimas A — Agentas (`spinta run`)

Spinta veikia kaip **standartizuotas fasadas** virš institucijos duomenų bazės ar
paslaugos. Partneriai ir piliečiai gauna duomenis per autorizuotą UDTS/UAPI API
tiesiogiai iš šaltinio — realiu laiku.

**Naudojama:** tarpinstitucinei duomenų integracijai per Gravitee vartus (DVMS projektas).

→ [Spinta Agento dokumentacija](agentas/index)

### Režimas B — Duomenų publikavimas (`spinta push`)

Duomenys iš izoliuotų sistemų periodiškai **Publikuojami** į centrinį DVMS/ADP mazgą.
Spinta atlieka griežtą duomenų validaciją pagal Manifestą prieš įrašydama į saugyklą.

**Naudojama:** atvirų duomenų publikavimui į ADP portalą.

*Dokumentacija ruošiama.*

```{toctree}
:maxdepth: 1
:hidden:
:caption: SPINTA Agentas

Apie SPINTA agentą <agentas/index>
Aplinkos paruošimas <agentas/agento-paruošimas>
Manifest parengimas <agentas/šaltinių-konfigūravimas>
Agento diegimas <agentas/diegimas/index>
agentas/autentifikacija
agentas/web-serverio-konfigūravimas
agentas/duomenų-gavimo-testavimas
agentas/tinklo-konfigūravimas
agentas/papildoma-informacija
```
