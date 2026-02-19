# Spinta

:::{note}
Ši dokumentacija yra rengiama (draft). Šiuo metu prioritetas — **Spinta Agento**
diegimo instrukcijos, skirtos institucijoms, kurios pradeda naudotis UDTS duomenų
teikimu per Gravitee vartus.
:::

## Kas yra Spinta?

**Spinta** — metaduomenimis valdomas duomenų logistikos variklis, sukurtas
[VSSA](https://vssa.lrv.lt "Valstybės skaitmeninių sprendimų agentūra").
Leidžia transformuoti uždaras heterogeniškas duomenų saugyklas į modernius,
saugius ir standartizuotus API išteklius pagal
[UDTS/UAPI](https://ivpk.github.io/uapi/ "UAPI specifikacija") standartą —
nekeičiant esamų sistemų.

Spinta esmė — **manifesto (DSA) dominavimas prieš kodą**: sistema elgiasi pagal
metaduomenų aprašą, o ne užkoduotą logiką. Tai leidžia keisti duomenų struktūrą,
prieigos teises ir šaltinius be programavimo.

## Veikimo režimai

| Režimas | Komanda | Paskirtis | Dokumentacija |
|---------|---------|-----------|---------------|
| **Agentas** | `spinta run` | Realaus laiko API proxy virš IS — teikia duomenis UDTS formatu | [Žr. Agentas](agentas/index) |
| **Duomenų publikavimas** | `spinta push` | Duomenų siuntimas į ADP/DVMS saugyklą | 🚧 Ruošiama |
| **Autentifikacijos serveris** | — | Standalone OAuth2/JWT serveris prieigos valdymui | 🚧 Ruošiama |
| **Inspekcija** | `spinta inspect` | DSA/manifesto generavimas iš duomenų šaltinio | 🚧 Ruošiama |

```{toctree}
:maxdepth: 2
:hidden:
:caption: Agentas

agentas/index
```
