# UDTS suderinamumas

:::{warning}
Spinta Agentas aktyvios plėtros stadijoje (v0.x). Ne visi
[UDTS/UAPI specifikacijos](https://ivpk.github.io/uapi/ "UAPI specifikacija") reikalavimai
yra pilnai įgyvendinti. Prieš planuodami integraciją, patikrinkite žemiau
esančią suderinamumo lentelę ir įsitikinkite, kad jums reikalingas funkcionalumas
palaikomas jūsų naudojamoje versijoje.
:::

Žemiau pateikta lentelė rodo, kurie UDTS specifikacijos reikalavimai yra palaikomi
kiekvienoje Spinta Agento versijų grupėje. Stulpelių antraštėje išvardytos versijos,
kurioms galioja tos pačios palaikymo reikšmės.

**Žymos:**

| Žyma | Reikšmė |
|------|---------|
| ✅ | Pilnai palaikoma |
| ⚠️ | Dalinis palaikymas arba apribojimai |
| ❌ | Nepalaikoma |
| ? | Netikrinta / nežinoma |

---

## Endpoint struktūra ir URI schema

| UDTS reikalavimas | 0.1.x | 0.2dev1–dev17 |
|-------------------|-------|---------------|
| UDTS URI schema (`/datasets/{form}/{org}/{catalog}/{dataset}/{version}/{model}`) | ✅ | ✅ |
| `GET /version` — versijos tikrinimas | ✅ | ✅ |
| `GET /health` — sveikatos patikra | ? | ? |

---

## CRUD operacijos

| UDTS reikalavimas | 0.1.x | 0.2dev1–dev17 |
|-------------------|-------|---------------|
| `GET /{model}` — sąrašo gavimas (getall) | ✅ | ✅ |
| `GET /{model}/{id}` — vieno objekto gavimas (getone) | ✅ | ✅ |
| `POST /{model}` — objekto kūrimas (insert) | ✅ | ✅ |
| `PUT /{model}/{id}` — pilnas atnaujinimas (update) | ✅ | ✅ |
| `PATCH /{model}/{id}` — dalinis atnaujinimas | ✅ | ✅ |
| `DELETE /{model}/{id}` — soft delete | ✅ | ✅ |
| `DELETE /{model}/{id}/:wipe` — permanent delete | ? | ? |
| `POST /{model}/{id}` — upsert operacija | ? | ? |

---

## Duomenų užklausimas

| UDTS reikalavimas | 0.1.x | 0.2dev1–dev17 |
|-------------------|-------|---------------|
| Lygybės filtras (`property=value`) | ✅ | ✅ |
| Lyginimo operatoriai (`_gt`, `_ge`, `_lt`, `_le`) | ✅ | ✅ |
| Teksto operatoriai (`_sw`, `_ew`, `_co`) | ✅ | ✅ |
| Loginis OR (`_or.property=value`) | ✅ | ✅ |
| Loginis AND (`_and.property=value`) | ✅ | ✅ |
| Puslapiavimas (`_limit`, `_page`) | ✅ | ✅ |
| Rūšiavimas (`_sort`, `_sort=-property`) | ✅ | ✅ |
| Keitimų sekimas (`GET /{model}/:changes/{cid}`) | ? | ? |

---

## Formatai ir antraštės

| UDTS reikalavimas | 0.1.x | 0.2dev1–dev17 |
|-------------------|-------|---------------|
| JSON formatas (`application/json`) | ✅ | ✅ |
| CSV formatas (`text/csv`) | ✅ | ✅ |
| ETag antraštė (versijų žymėjimas) | ? | ? |
| `traceparent` / `tracestate` antraštės | ? | ? |
| `Accept-Language` (daugiakalbystė) | ? | ? |

---

## Autentifikacija ir autorizacija

| UDTS reikalavimas | 0.1.x | 0.2dev1–dev17 |
|-------------------|-------|---------------|
| OAuth2 Client Credentials srautas | ✅ | ✅ |
| OAuth2 Authorization Code srautas | ? | ? |
| Scope valdymas — modelio lygis (`uapi:/datasets/.../Model/:action`) | ✅ | ✅ |
| Scope valdymas — eilučių lygis (`uapi:/datasets/.../Model/@filtras/:action`) | ❌ | ❌ |

:::{note}
**Eilučių lygio scope** (row-level security) — tai UDTS mechanizmas, leidžiantis
apriboti prieigą ne tik prie modelio, bet ir prie konkrečių duomenų eilučių pagal
filtro sąlygą. Pavyzdžiui, `uapi:/datasets/gov/rc/ar/ws/Gyventojas/@registruotas/:getall`
leistų klientui matyti tik eilutes, kur `registruotas=true`.

Šiuo metu Spinta Agentas **nepalaiko** šio funkcionalumo — scope tikrinamas tik
modelio lygiu. Jei jūsų integracijos scenarijus reikalauja eilučių lygio prieigos
kontrolės, kreipkitės į VSSA.
:::

---

## Savybių ir paslaugų operacijos

| UDTS reikalavimas | 0.1.x | 0.2dev1–dev17 |
|-------------------|-------|---------------|
| Savybių operacijos (`GET /{model}/{id}/{property}`) | ? | ? |
| Failo tipo savybės (`_content_type`, `_size`) | ? | ? |
| `GET /services/...` — paslaugų endpoint'ai | ❌ | ❌ |

---

:::{note}
Jei pastebėjote netikslumą arba norite papildyti lentelę naujos versijos duomenimis,
kreipkitės į [VSSA](https://vssa.lrv.lt "Valstybės skaitmeninių sprendimų agentūra").
:::
