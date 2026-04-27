# Data Portal

This directory contains a draft self-hosted data distribution portal for
`data.realgoodresearch.com`.

## Services

- `minio`: Private S3-compatible object storage
- `postgres`: Catalog and token-grant database
- `broker-api`: FastAPI service that lists datasets and returns policy-aware
  download decisions
- `nginx`: TLS gateway for the frontend and reverse proxy for multiple subdomains
- `certbot`: Automated Let's Encrypt renewal sidecar

## Quick Start

1. Copy `.env.example` to `.env` and replace the MinIO credentials.
   Set `MINIO_DATA_PATH` to the host path where MinIO should store data, ideally
   your RAID-backed mount such as `/data/raid/minio`.
   Also set Postgres credentials, `POSTGRES_DATA_PATH`, `POSTGRES_BIND_ADDRESS`,
   `MINIO_BIND_ADDRESS`, and `MINIO_PUBLIC_ENDPOINT`.
2. Start the stack:

```bash
docker compose up -d --build
```

3. Request the first certificate for `data.realgoodresearch.com`:

```bash
./scripts/request-certificate.sh data.realgoodresearch.com
```

4. Reload Nginx once the certificate is issued:

```bash
docker compose restart nginx
```

The `certbot` container will renew existing certificates automatically. The Nginx
container also watches the certificate directory and reloads itself after renewals.

## Storage Location

MinIO stores object data on the host path defined by `MINIO_DATA_PATH` in `.env`.
That path is mounted into the container as `/data`.

Example:

```env
MINIO_DATA_PATH=/data/raid/minio
MINIO_BIND_ADDRESS=10.8.0.5
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
MINIO_PUBLIC_ENDPOINT=https://data.realgoodresearch.com/minio
POSTGRES_DATA_PATH=/data/raid/postgres
POSTGRES_BIND_ADDRESS=10.8.0.5
POSTGRES_PORT=5432
```

`MINIO_PUBLIC_ENDPOINT` is the public base URL used when the broker generates
presigned URLs. The Nginx config proxies `/minio/` to the internal MinIO S3
service so external clients can use those URLs.

`MINIO_BIND_ADDRESS` controls which host interface exposes the MinIO S3 API and
console. Set it to a VPN or LAN IP if you want to use the MinIO web console
without an SSH tunnel.

`POSTGRES_BIND_ADDRESS` controls which host interface exposes PostgreSQL. Use a
VPN or LAN IP if you want DBeaver access from that network only. Keep it at
`127.0.0.1` if you only want local host access.

Example DBeaver connection settings:

- Host: the server's LAN or VPN IP that matches `POSTGRES_BIND_ADDRESS`
- Port: `POSTGRES_PORT`
- Database: `POSTGRES_DB`
- Username: `POSTGRES_USER`
- Password: `POSTGRES_PASSWORD`

## Multi-Subdomain Routing

Add one file per public hostname under `nginx/conf.d/`. For example, a dashboard
site can live at `dashboard.realgoodresearch.com` and proxy to a separate Docker
service while the data portal continues serving `data.realgoodresearch.com`.

See `nginx/conf.d/dashboard.conf.example` for a template.

Recommended order for a new subdomain:

1. Point the new DNS record at this server.
2. Run `./scripts/request-certificate.sh dashboard.realgoodresearch.com`.
3. Copy the example config into `nginx/conf.d/` and adjust the upstream service.
4. Run `docker compose restart nginx`.

## Catalog Model

Postgres now stores collections, dataset records, tags, access tokens, and token
grants. The broker treats MinIO bucket layout as an implementation detail.

Classification rules:

- `public`: listed and downloadable without a token
- `restricted`: listed for everyone, downloadable only with a token grant
- `confidential`: listed for everyone, never downloadable via the public API

Token grant rules:

- Each `token_grants` row is evaluated conjunctively.
- If a row specifies both `bucket` and `classification`, both must match.
- If a row specifies `bucket`, `classification`, and `key_prefix`, all three must match.
- `dataset_id` can still be used for one-off exact dataset grants.

Collections are editorial containers only. Classification stays on each dataset,
so a single collection can mix public, restricted, and confidential files.

The database bootstrap files live in [postgres/initdb](/home/doug/git/realgoodresearch/sysadmin/data-portal/postgres/initdb:1). On a fresh Postgres data directory they create:

- `collections`
- `datasets`
- `dataset_tags`
- `access_tokens`
- `token_grants`

Dataset timestamps:

- `created_at`: auto-filled on insert
- `updated_at`: auto-updated on each row change
- `published_at`: now defaults to insert time unless you set it explicitly

If your Postgres data directory already existed before the collection changes,
run [postgres/migrations/001_add_collections.sql](/home/doug/git/realgoodresearch/sysadmin/data-portal/postgres/migrations/001_add_collections.sql:1)
once against the live database.

The seed file inserts one example collection, three example datasets, and two
example tokens. Sample plaintext tokens for a fresh database:

- `partner-alpha-2026-rotate-me`
- `partner-beta-2026-rotate-me`

## API

`GET /api/v1/collections`

- No token required
- Optional `X-Access-Token` header
- Returns one list of collection summaries with counts for public, restricted,
  and confidential files

Example:

```bash
curl https://data.realgoodresearch.com/api/v1/collections \
  -k
```

`GET /api/v1/collections/{slug}`

- Returns one collection with its README URL and the current access state of each
  dataset inside it

`GET /api/v1/datasets/{slug}`

- Returns one dataset with its current access decision for the caller

`POST /api/v1/download-url`

- Accepts a `dataset_id`
- Returns `allowed: false` for restricted items without a matching token and for
  all confidential items

Example:

```bash
curl -X POST https://data.realgoodresearch.com/api/v1/download-url \
  -H 'Content-Type: application/json' \
  -H 'X-Access-Token: partner-alpha-2026-rotate-me' \
  -d '{
    "dataset_id": "22222222-2222-2222-2222-222222222222",
    "download_filename": "briefing-apr-2026.xlsx"
  }'
```

JSON Schemas for the collection, catalog, and download endpoints live in
[broker-api/schemas](/home/doug/git/realgoodresearch/sysadmin/data-portal/broker-api/schemas:1).

## Quarto Frontend

The generated `frontend/` directory is now treated as a build artifact and does
not need to be committed. The Quarto source of truth lives under
[site](/home/doug/git/realgoodresearch/sysadmin/data-portal/site:1).

The hand-served files in `frontend/` are produced by the Quarto-built site
defined under [site](/home/doug/git/realgoodresearch/sysadmin/data-portal/site:1).
The Quarto source mirrors the typography and navigation style used in the main
Real Good Research docs site, while the browser-side collection logic stays in:

- `site/assets/catalog.js` for the collection search page
- `site/assets/collection-detail.js` for the collection detail page

The Quarto project is configured to render directly into `frontend/`:

```bash
cd data-portal/site
quarto render
```

Render directly in place. Do not rename or replace the `frontend/` directory
while Nginx is running, or the bind mount can point at a stale empty directory
and return `403`.

Safe workflow:

```bash
cd data-portal/site
quarto render
docker compose -f ../docker-compose.yml up -d --force-recreate nginx
```

This environment does not have `quarto` installed, so I could not run the render
here.
