# Spinta diegimas naudojant konteinerius (Docker)

Norint naudoti Spinta Agentą kaip konteinerį galima konteinerį kurtis patiems arba naudoti oficialų VSSA palaikomą docker image. Tačiau verta atkreipti dėmesį, kad naudojant VSSA image apima tik Spinta dalį, tačiau Redis ir Reverse proxy (SSL užtikrinimui) diegimas ir konfigūravimas lieka institucijos atsakomybėje.

<p align="center">
  <img src="../../_static/docker-pavyzdys.png" alt="Docker pavyzdys">
  <br>
  <em>Docker pavyzdys</em>
</p>

VSSA palaikomą oficialų Docker image galima parsisiųsti iš Docker hub: [Spinta Docker image](https://hub.docker.com/r/vssadevops/spinta/tags "Spinta Docker image")

## Spinta Agento konfigūracijos failų paruošimas

Visų pirma pasikeiskite aktyvų naudotoją ir katalogą:

```bash
sudo -Hsu spinta
cd
```

Spinta yra konfigūruojama konfigūracijos failo pagalba, kurio, pagal nutylėjimą ieškoma aktyviame kataloge. Kur Spinta ieško konfigūracijos failo, galima patikrinti taip:

:::{note}
PASTABA: Ši komanda neveiks, kol nebus paleistas Spinta konteineris
:::

```bash
docker exec spinta poetry run spinta config config
```

Kadangi Spintos konfigūracijos failas neegzistuoja, reikia jį sukurti:

```bash
cat > config.yml << 'EOF'
config_path: /app/spinta_config/config
env: production

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

| Kintamasis  | Privalomas ar neprivalomas | Aprašas                                                                                                                                            |
| ----------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| config_path | Privalomas                 | Nurodo kelią į direktoriją, kurioje yra įvairūs konfigūracijos duomenys                                                                            |
| env         |                            | Nurodo, ar aplinka testinė, ar vystymo, ar produkcinė. Rekomenduojama diegti su `production`, nes `dev` arba `test` įrašo papildomų konfigūracijų. |
| keymaps     | Privalomas                 | Aprašyta žemiau                                                                                                                                    |
| .default    |                            | Nurodo numatytąjį                                                                                                                                  |
| ..type      |                            | Tipas - `redis`                                                                                                                                    |
| ..dsn       |                            | Nurodo, kaip pasiekti servisą                                                                                                                      |
| accesslog   |                            | Nurodo, kur bus saugomi prieigos žurnalai                                                                                                          |
| .type       |                            | Tipas - ar saugoma faile, ar kitaip                                                                                                                |
| .file       |                            | Jei saugoma faile, nurodo, kuriame faile                                                                                                           |

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

Spintos Agentui nurodome kokį keymap komponentą naudoti ir kaip jį pasiektį per config.yml failą. Pateikiame config.yml failo fragmentą:

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

Norint paleisti Spintos docker konteinerį, reikia sukurti paleidimo scriptą. Tai galite padaryti taip:

```bash
cat > startup.sh << 'EOF'
#!/bin/bash

CONFIG_DIR="/app/spinta_config/config"

if [ -z "$(ls -A $CONFIG_DIR 2>/dev/null)" ]; then
    poetry run spinta check
fi

exec poetry run spinta -o config=/app/spinta_config/config.yml run /app/spinta_config/manifest.csv --mode external --host 0.0.0.0 --port 8000
EOF
```

Patikrinkite spinta vartotojo UID ir GID:

```bash
id spinta
```

Rezultato pavyzdys

```
uid=997(spinta) gid=33(www-data) groups=33(www-data),124(docker)
```

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
    user: "997:33" # įrašykite spinta vartotojo UID:GID
    environment:
      HOME: /app/spinta_config
      SPINTA_CONFIG: /app/spinta_config/config.yml
    volumes:
      - /opt/spinta:/app/spinta_config
    depends_on:
      - redis-keymap
    command: bash /app/spinta_config/startup.sh
volumes:
  redis_keymap_data:
EOF
```

:::{caution}
**SVARBU! Redis būtinai turi būti leidžiamas persistent režimu (appendonly:yes ir appendfsync:always parametrai)**

Yra keli persistent režimai (žr. Redis/Valkey dokumentaciją). Numatytasis režimas (appendonly:yes ir appendfsync:always) užtikrina didžiausią duomenų nepraradimo patikimumą, tačiau turi mažiausią greitį naujo rakto kūrimo metu, lyginant su kitais režimais.
:::

Paleiskite Docker konteinerius:

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
docker exec spinta poetry run spinta config config backends manifests accesslog
```

:::{note}
Jei jūsų duomenų šaltinis - reliacinė duomenų bazė, patikriname ar Spinta gali prisijungti prie duomenų bazės.

```bash
docker exec spinta poetry run spinta wait 1
```

:::

## Kliento sukūrimas

Norint ištestuoti duomenų gavimą, reikia sukonfigūruoti klientus ir jų leidimus.

Daugiau informacijos apie leidimus galit rasti UDTS specifikacijoje: [UAPI](https://ivpk.github.io/uapi/#section/Authorization/Scope "UAPI")

Klientus ir jų leidimus galite pridėti šia komanda:

```bash
OUTPUT=$(docker exec -i spinta poetry run spinta client add -n test --scope - << 'EOF'
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
EOF
) && \
FILE_PATH=$(echo "$OUTPUT" | grep -A2 "saved to:" | tail -1 | xargs) && \
SECRET=$(echo "$OUTPUT" | grep -A2 "Client secret:" | tail -1 | xargs)
```

Pasitikrinti pilną gautą atsakymą galite taip:

```bash
echo "$OUTPUT"
```

Atsakymas turėtų atrodyti taip:

```
New client created and saved to:

    /opt/spinta/config/clients/id/f4/0e/76b7-f3f4-4a4c-b2e9-c03a147d65f9.yml

Client secret:

    wjhl5sKB0YkE994yXue8rX0E-dQadKcF

Remember this client secret, because only a secure hash of
client secret will be stored in the config file.
```

:::{caution}
Būtinai išsisaugokit gautą Secret, nes daugiau jo pamatyti nebegalėsite.
:::

```bash
echo $SECRET
```

Jei norite susieti šį klientą su šaltinio klientu, turite pakoreguoti šį sukurtą kliento failą, ir pridėti backends dalį, kurioje turi būti aprašyti jungimuisi prie šaltinio reikalingi duomenys:

```
backends:
  backends:
  get_data:
    sub: MTAwMQ==
```

Kliento failą galite patikrinti taip:

```bash
docker exec spinta cat $FILE_PATH
```

Susieti klientą su šaltiniu galite naudodami šią komandą:

```bash
docker exec -i spinta bash -c "sed -i 's/backends: {}/backends:\n  get_data:\n    sub: MTAwMQ==/' $FILE_PATH"
```

Pilnas kliento failas turėtų atrodyti panašiai į šį:

```
client_id: ae571e79-a826-4e10-b792-cc1a4d1e94ca
client_name: test
client_secret_hash:
  pbkdf2$sha256$952803$anM6ka5E5CuKoDMaj90f4Q$NNymDRKxL_JOwHL8z9GA6uumeIjEk6aO6>
scopes:
  - uapi:/:getone
  - uapi:/:getall
  - uapi:/:search
  - uapi:/:changes
backends:
  get_data:
    sub: MTAwMQ==
```
