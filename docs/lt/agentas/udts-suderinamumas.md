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
| UDTS URI schema (`{domain}/{datasets}/{form}/{org}/{catalog}/{service}/{version}/{dataset}/{model}/{id}`) | ✅ | ✅ |
| `GET /version` — versijos tikrinimas | ✅ | ✅ |
| `GET /health` — sveikatos patikra | ? | ? |

:::{note}

Spinta agentas turi `domain/version` (be dvitaškio prefikso). Kas tiesiogiai nesuderinama su UDTS.
UDTS specifikacija numato, kad versijos endpoint turi būti pasiekiamas `{domain}/{datasets}/{form}/{org}/{catalog}/{service}/:version`. 
Publikuojant paslaugą iš Katalogo į vartus, sukuriamos taisyklės kurios realizuoja šį reikalavimą ir nukreipia į SPINTA agento endpoint.


Kai paslauga publikuojama per **Gravitee vartus**, vartai per dinaminį maršrutizavimą
atlieka atitikties užtikrinimą — `/:version` užklausa per vartus sėkmingai nukreipiama
į Spinta agento `/version`. Taigi UDTS atitiktis egzistuoja **publikuotai paslaugai per vartus**,
tačiau tiesioginis Spinta agento endpoint `/:version` (su dvitaškiu) nepalaikomas.
:::

---

## CRUD operacijos

| UDTS reikalavimas | Palaiko |
|-------------------|-------|
| `GET /{model}` — sąrašo gavimas (getall) | ✅ |
| `GET /{model}/{id}` — vieno objekto gavimas (getone) | ✅ |
| `POST /{model}` — objekto kūrimas (insert) | ❌ |
| `PUT /{model}/{id}` — pilnas atnaujinimas (update) | ❌ |
| `PATCH /{model}/{id}` — dalinis atnaujinimas | ❌ |
| `DELETE /{model}/{id}` — soft delete | ❌ |
| `DELETE /{model}/{id}/:wipe` — permanent delete | ❌ |
| `POST /{model}/{id}` — upsert operacija | ❌ |

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
| Scope valdymas — dataset lygis (`uapi:/datasets/.../:action`) | ✅ | ✅ |
| Scope valdymas — modelio lygis (`uapi:/datasets/.../Model/:action`) | ❌ | ❌ |
| Scope valdymas — eilučių lygis (`uapi:/datasets/.../Model/@filtras/:action`) | ❌ | ❌ |

:::{note}
**Scope valdymas**

Spinta Agentas palaiko scope tikrinimą tik **dataset lygiu**
(`uapi:/datasets/{form}/{org}/{catalog}/{service}/:action`).
Modelio lygio scopų (`uapi:/datasets/.../Model/:action`) Spinta Agentas
**neatpažįsta ir nepalaiko** — tokie scopai bus ignoruojami arba sukels klaidą.
Eilučių lygio scopai taip pat nepalaikomi.
:::

:::{note}
**Eilučių lygio scope** (row-level security) — tai UDTS mechanizmas, leidžiantis
apriboti prieigą prie konkrečių duomenų eilučių pagal filtro sąlygą. Šiuo metu
Spinta Agentas **nepalaiko** šio funkcionalumo. Jei jūsų integracijos scenarijus
reikalauja eilučių lygio prieigos kontrolės, kreipkitės į VSSA.
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
