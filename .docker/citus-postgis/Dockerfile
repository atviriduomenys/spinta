# Pinned: images published under this tag since 2026-11-04 refuse connections in CI (#2090).
FROM citusdata/citus:postgres_17@sha256:c6899ea094a97ae9e20fee10ef2cba514b76724255119998ffbb7ba2f5dee126

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-17-postgis-3 \
      postgresql-17-postgis-3-scripts \
 && rm -rf /var/lib/apt/lists/*
