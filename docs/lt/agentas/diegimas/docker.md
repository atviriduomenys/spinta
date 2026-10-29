# Spinta diegimas naudojant konteinerius (Docker)

Norint naudoti Spintos Agentą kaip konteinerį galima konteinerį kurti patiems arba naudoti oficialų VSSA palaikomą
docker image. Tačiau verta atkreipti dėmesį, kad naudojant VSSA docker image apima tik Spinta dalį, tačiau Redis ir
Reverse proxy (SSL užtikrinimui) diegimas ir konfigūravimas lieka institucijos atsakomybėje.

<p align="center">
  <img src="../../_static/docker-pavyzdys.png" alt="Docker pavyzdys">
  <br>
  <em>Docker pavyzdys</em>
</p>

VSSA palaikomą oficialų Docker image galima parsisiųsti iš Docker
hub: [Spinta Docker image](https://hub.docker.com/r/vssadevops/spinta/tags "Spinta Docker image")

## Spinta Agento konfigūracijos failų paruošimas

Visų pirma pasikeiskite aktyvų naudotoją ir katalogą:

```bash
sudo -Hsu spinta
cd
```

Spinta yra konfigūruojama konfigūracijos failo pagalba, kurio, pagal nutylėjimą ieškoma aktyviame kataloge. Kur Spinta
ieško konfigūracijos failo, galima patikrinti taip:

:::{note}
PASTABA: Ši komanda neveiks, kol nebus paleistas Spinta konteineris
:::

```bash
docker exec spinta spinta config config
```

Kadangi Spintos konfigūracijos failas neegzistuoja, reikia jį sukurti:

```bash
cat > config.yml << 'EOF'
config_path: /app/spinta_config/config
env: production
access: protected

resource_server_id: https://agentas.example.lt
auth_server_id: https://agentas.example.lt
auth_server_url: https://agentas.example.lt

keymaps:
  default:
    type: redis
    dsn: redis://redis-keymap:6379/1

accesslog:
  type: file
  file: /app/spinta_config/logs/access.log
EOF
```

### Failo aprašas

| Kintamasis         | Privalomas ar neprivalomas  | Aprašas                                                                                                                                                                                                                                 |
|--------------------|-----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| config_path        | Privalomas                  | Nurodo kelią į direktoriją, kurioje yra įvairūs konfigūracijos duomenys                                                                                                                                                                 |
| env                |                             | Nurodo, ar aplinka testinė, ar vystymo, ar produkcinė. Rekomenduojama diegti su `production`, nes `dev` arba `test` įrašo papildomų konfigūracijų.                                                                                      |
| access             | **Privalomas nuo 0.2dev22** | Nurodo numatytąjį prieigos lygį. Rekomenduojama nurodyti `protected`. Nuo versijos 0.2dev22 numatytoji reikšmė pasikeitė iš `protected` į `open` — neaprašius šio lauko, modeliai su `access=protected` grąžins `ModelNotFound` klaidą. |
| resource_server_id | Privalomas                  | Šio agento (resursų serverio) identifikatorius. Spinta jį įrašo į išduodamų žetonų `aud` ir priima tik žetonus, kurių `aud` sutampa.                                                                                                    |
| auth_server_id     | Privalomas                  | Autorizacijos serverio identifikatorius. Spinta jį įrašo į žetono `iss` ir priima tik žetonus, kurių `iss` sutampa. Naudojant lokalų AM — agento adresas.                                                                               |
| auth_server_url    | Privalomas                  | Autorizacijos serverio adresas, pagal kurį sudaromi `/auth/token`, `/auth/introspect` ir JWKS adresai. Naudojant lokalų AM — agento adresas.                                                                                            |
| keymaps            | Privalomas                  | Aprašyta žemiau                                                                                                                                                                                                                         |
| .default           |                             | Nurodo numatytąjį                                                                                                                                                                                                                       |
| ..type             |                             | Tipas -                                                                                                                                                                                                                                 
 `redis`            |
| ..dsn              |                             | Nurodo, kaip pasiekti servisą                                                                                                                                                                                                           |
| accesslog          |                             | Nurodo, kur bus saugomi prieigos žurnalai                                                                                                                                                                                               |
| .type              |                             | Tipas - ar saugoma faile, ar kitaip                                                                                                                                                                                                     |
| .file              |                             | Jei saugoma faile, nurodo, kuriame faile                                                                                                                                                                                                |

```{versionchanged} 1.3.0
`resource_server_id`, `auth_server_id` ir `auth_server_url` atsirado 1.3.0 versijoje. 1.2.0 versijoje
tie patys nustatymai vadinasi `resource_server` ir `token_issuer` (seni pavadinimai vis dar skaitomi).
Iki 1.2.0 šių parametrų nebuvo. Žr. [Pakeitimai pagal versijas](../versiju-pakeitimai.md).
```


Prieš testuojant ar konfigūracija veikia, sukuriame reikalingus katalogus:

```bash
mkdir /opt/spinta/logs
mkdir /opt/spinta/config
mkdir /opt/spinta/.spinta_logs
```

### Keymap DB

<p align="center">
  <img src="../../_static/keymap-db-docker.png" alt="KeymapDB">
  <br>
  <em>Keymap DB</em>
</p>

:::{warning}
Pagal numatytuosius nustatymus naudojama SQLite, tačiau būtina pakeisti į Redis persistent.
:::

Keymap naudojamas susieti išorinius identifikatorius su vidiniais identifikatoriais. Gali būti konfigūruojama.

Spintos Agentui nurodome kokį keymap komponentą naudoti ir kaip jį pasiektį per config.yml failą. Pateikiame config.yml
failo fragmentą:

```
keymaps:
  default:
    type: redis
    dsn: redis://redis-address:6379/1
```

:::{tip}
Jei neturite `Redis` serviso, rekomenduojame diegti Docker konteineryje.
:::

## Docker konteinerio paleidimas

Sukurkite docker-compose.yml konfigūraciją:

:::{note}
Aplinkos parametrus patartina apsirašyti atskirame aplinkai skirtame faile.
:::

```bash
cat > docker-compose.yml << 'EOF'
services:
  redis-keymap:  # sqlite faster alternative
    image: valkey/valkey:9
    restart: always
    command: ["redis-server", "--appendonly", "yes", "--appendfsync", "everysec"] # šie nustatymai užtikrina pilną apsaugojimą nuo duomenų praradimo
    ports:
      - "6379:6379"
    volumes:
      - redis_keymap_data:/data
  spinta:
    image: vssadevops/spinta:latest
    container_name: spinta
    restart: always
    ports:
      - "8000:8000"
    environment:
      HOME: /app/spinta_config
      SPINTA_CONFIG: /app/spinta_config/config.yml
    volumes:
      - /opt/spinta:/app/spinta_config
    depends_on:
      - redis-keymap
volumes:
  redis_keymap_data:
EOF
```

:::{caution} **SVARBU! Redis būtinai turi būti leidžiamas persistent režimu (appendonly:yes ir appendfsync:always
parametrai)**

Yra keli persistent režimai (žr. Redis/Valkey dokumentaciją). Numatytasis režimas (appendonly:yes ir appendfsync:always)
užtikrina didžiausią duomenų nepraradimo patikimumą, tačiau turi mažiausią greitį naujo rakto kūrimo metu, lyginant su
kitais režimais.
:::

Paleiskite Docker konteinerius:

(docker-paleidimas)=
### Kaip paleidžiamas Agentas konteineryje

Pavyzdyje `command:` nenurodytas – tada konteineris paleidžiamas atvaizde esančiu skriptu
`/app/startup.sh`. Jis:

1. patikrina manifestą (`spinta check`);
2. jei nustatytas aplinkos kintamasis `SPINTA_AUTO_UPGRADE=true`, paleidžia `spinta upgrade`;
3. paleidžia Agentą (`uvicorn spinta.asgi:app --host 0.0.0.0`, prievadas 8000).

Tai **rekomenduojamas būdas**: atnaujinant atvaizdą kartu atnaujinamas ir paleidimo skriptas.
Norėdami, kad atnaujinimo skriptai būtų paleidžiami automatiškai, `environment:` dalyje
pridėkite:

```yaml
      SPINTA_AUTO_UPGRADE: "true"
```

:::{note}
**Alternatyva – savas paleidimo skriptas.** Kai reikia kitų serverio parametrų (pvz.,
`--workers`, `--proxy-headers`), papildomų patikrų prieš paleidimą ar kitokios atnaujinimo
tvarkos, galima nurodyti savą skriptą, pvz.:

```yaml
    command: bash /app/spinta_config/startup.sh
```

Tuomet už skripto turinį ir jo suderinamumą su naujomis versijomis atsako institucija.
Pagrindu imkite atvaizde esantį `/app/startup.sh` (`docker exec spinta cat /app/startup.sh`).
:::

:::{caution}
**Kur dažniausiai suklystama naudojant savą skriptą**

- Skripto nėra nurodytoje vietoje arba jis nevykdomas – konteineris nepasileidžia su
  `No such file or directory`. Skriptas turi būti prijungtame kataloge (`/opt/spinta` →
  `/app/spinta_config`), su Unix eilučių pabaigomis (LF, ne Windows CRLF).
- Skripte paliktas `poetry run` – nuo `0.2dev24` atvaizde Poetry nebėra, klaida
  `poetry: command not found`.
- Skriptas nukopijuotas iš senos versijos ir neatnaujinamas kartu su atvaizdu.
- `1.3.0` atvaizde trūko `uvicorn` (ištaisyta `1.3.1`) – atnaujinkite bent iki `1.3.1`.
:::

### Paleidimas

```bash
docker compose up -d
```

Patikrinkite Docker būseną:

```bash
docker ps -a
```

Patikrinkite Docker žurnalą:

```bash
docker logs spinta
```

## Konfigūracijos tikrinimas

```bash
docker exec spinta spinta config config backends manifests accesslog
```

:::{note}
Jei jūsų duomenų šaltinis - reliacinė duomenų bazė, patikriname ar Spinta gali prisijungti prie duomenų bazės.

```bash
docker exec spinta spinta wait 1
```

:::

