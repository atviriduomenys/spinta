# Web serverio konfigūravimas

## Nginx

:::{important}
Spinta Agentas veikia tik SSL ryšiu, tačiau jo užtikrinimu turi rūpintis institucijos sistemų administratoriai. Žemiau pateikiame tik bendrojo pobūdžio aprašą.

Nginx siūlomas tik kai vienas iš variantų, galima turėti ir kitą priemonę, pavyzdžiui WAF.
:::

Įdiekite pasirinktą Web serverio paketą, šiuo atveju pavyzdys pateiktas [Nginx](https://nginx.org/en/ "Nginx"):

```bash
sudo apt install nginx
```

Sukurkite pasirinkto Web serverio, šiuo atveju Nginx, konfigūracijos failą (pakeiskite example.com į jūsų domeno pavadinimą):

```bash
cat << 'EOF' | sudo tee /etc/nginx/sites-available/example.com
server {
    listen 80;
    server_name example.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
EOF
```

Aktyvuokite konfigūracijos failą:

```bash
sudo ln -s /etc/nginx/sites-available/example.com /etc/nginx/sites-enabled/
```

Patikrinkite ar konfigūracija veikia:

```bash
sudo nginx -t
```

Perkraukite Nginx:

```bash
sudo systemctl restart nginx
```

Patikrinkite ar servisas veikia:

```bash
sudo systemctl status nginx
```

## SSL konfigūracija

Detalesnės instrukcijos apie tai, kaip konfigūruoti SSL sertifikatus ir kitus [Gunicorn](https://docs.gunicorn.org/en/stable/settings.html#ssl "Gunicorn SSL") ar [Nginx](https://nginx.org/en/docs/http/configuring_https_servers.html "Nginx SSL") parametrus rasite minėtų projektų dokumentacijoje.

Jei naudojate Let’s Encrypt sertifikatus, jų diegimą galima daryti certbot pagalba:

```bash
sudo snap install --classic certbot
```

```bash
sudo ln -s /snap/bin/certbot /usr/bin/certbot
```

```bash
sudo certbot --nginx
```
