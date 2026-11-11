FROM citusdata/citus:postgres_17

# Since PostGIS 3.7 the tiger geocoder is a separate package (#2090).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-17-postgis-3 \
      postgresql-17-postgis-3-scripts \
      postgresql-17-postgis-tiger-geocoder \
 && rm -rf /var/lib/apt/lists/*
