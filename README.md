# Data Portal

This directory contains a draft self-hosted data distribution portal for
`data.realgoodresearch.com`.

This repository is deployment-specific as written. Before reusing it for another
organization or public demo, replace the example domain names, network ranges,
and sample credentials references with values appropriate for that environment.

## Services

The default [docker-compose.yml](/home/doug/git/realgoodresearch/realgooddata/docker-compose.yml:1)
is the production cloud stack:

- `postgres`: Catalog and token-grant database, running on the cloud host
- `broker-api`: FastAPI service that lists datasets and returns policy-aware
  download decisions
- `nginx`: TLS gateway for the frontend and reverse proxy for multiple subdomains
- `certbot`: Automated Let's Encrypt renewal sidecar

The local development stack in
[docker-compose.local.yml](/home/doug/git/realgoodresearch/realgooddata/docker-compose.local.yml:1)
keeps all services together and adds:

- `minio`: Private S3-compatible object storage

## Production Quick Start

1. On a fresh Ubuntu cloud host, install the system dependencies:

```bash
sudo ./scripts/install-ubuntu-dependencies.sh --configure-firewall
```

The script sets the VM hostname to `realgooddata` by default. Use
`--hostname NAME` or `--skip-hostname` if you need different behavior.

2. Copy `.env.example` to `.env`.
3. Set Postgres credentials, admin credentials, and the cloud Postgres data path.
4. Set `MINIO_ENDPOINT` to the local MinIO API origin that the cloud broker can
   reach, usually `https://your-minio-origin.example.com:9000`.
5. Create a dedicated MinIO broker user using the policy in
   [minio/policies/broker-readonly.json](/home/doug/git/realgoodresearch/realgooddata/minio/policies/broker-readonly.json:1),
   then set `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` to that user's credentials.
6. On the local storage server, run MinIO and allow inbound `9000` only from the
   cloud host's static IP address.
7. Start the cloud stack:

```bash
docker compose up -d --build
```

8. Request the first certificate for `data.realgoodresearch.com`:

```bash
./scripts/request-certificate.sh data.realgoodresearch.com
```

9. Reload Nginx once the certificate is issued:

```bash
docker compose restart nginx
```

The `certbot` container will renew existing certificates automatically. The Nginx
container also watches the certificate directory and reloads itself after renewals.

## Local Development Quick Start

Use the local compose file when you want Postgres, MinIO, broker, Nginx, and
certbot on one machine:

```bash
cp .env.local.example .env
docker compose -f docker-compose.local.yml up -d --build
```

The local broker uses the local MinIO root credentials for convenience. Production
should use the dedicated `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` variables in
`.env.example`.

## Admin Panel

The broker now serves a minimal admin panel at `/admin`.

The nginx config currently restricts `/admin` to:

- `127.0.0.1`
- `::1`
- `10.6.0.0/24`
- `192.168.50.0/24`

Anything outside that VPN/local range receives `403 Forbidden` before the login
page is reached.

Set these additional env vars in `.env` before rebuilding the broker:

```env
ADMIN_USERNAME=admin
ADMIN_PASSWORD=replace-with-a-long-random-password
ADMIN_SESSION_SECRET=replace-with-a-separate-long-random-secret
ADMIN_SESSION_TTL_SECONDS=43200
```

Then rebuild the broker and restart Nginx:

```bash
docker compose up -d --build broker-api nginx
```

For the local development stack, add `-f docker-compose.local.yml` to the command.

The first admin release supports:

- env-based login at `/admin/login`
- token creation and revocation
- collection create/edit
- dataset create/edit
- bulk import from a MinIO bucket/prefix into a collection

Bulk import behavior:

- target collection is required
- imported rows use the selected `classification` and `visibility`
- `storage_bucket`, `storage_key`, and `file_size_bytes` are populated from MinIO
- title and slug are auto-generated from the object filename unless a close `storage_key` match is found, in which case title and summary are copied from the existing dataset
- existing catalog rows for the same bucket/object key are skipped

## MinIO Origin Storage

In production, MinIO runs on the local/internal storage server, not on the cloud
host. The broker reaches it through `MINIO_ENDPOINT`.

On the local storage server, MinIO stores object data on the host path defined by
`MINIO_DATA_PATH`. That path is mounted into the container as `/data`.

Example:

```env
MINIO_DATA_PATH=/data/raid/minio
MINIO_BIND_ADDRESS=0.0.0.0
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

Forward or expose only the MinIO API port, `9000`, and firewall it so the only
allowed source is the cloud host's static IP address. Keep the MinIO console,
`9001`, local-only or reachable through an SSH tunnel.

If you want to run just the production MinIO origin from this repo on the local
storage server, use the local compose file and start only `minio`:

```bash
cp .env.local.example .env
docker compose -f docker-compose.local.yml up -d minio
```

For local development, keep MinIO bound to loopback:

```env
MINIO_BIND_ADDRESS=127.0.0.1
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

`MINIO_BIND_ADDRESS` controls which host interface exposes the MinIO S3 API and
console in the local stack.

## MinIO Broker Credentials

Do not use the MinIO root credentials in the cloud broker `.env`. Create a
dedicated broker user with a read-only policy instead.

On the local storage server, configure `mc` with the MinIO root credentials from
that server's `.env`:

```bash
mc alias set local http://127.0.0.1:9000 'your-minio-root-user' 'your-minio-root-password'
```

Create or update the broker read-only policy from this repo:

```bash
mc admin policy create local broker-readonly minio/policies/broker-readonly.json
```

Create the broker user, attach the policy, and print the cloud `.env` values:

```bash
BROKER_SECRET="$(openssl rand -base64 36)"

mc admin user add local broker-service-account "$BROKER_SECRET"
mc admin policy attach local broker-readonly --user broker-service-account

printf 'MINIO_ACCESS_KEY=%s\n' 'broker-service-account'
printf 'MINIO_SECRET_KEY=%s\n' "$BROKER_SECRET"
```

Then set those values on the cloud host:

```env
MINIO_ACCESS_KEY=broker-service-account
MINIO_SECRET_KEY=replace-with-the-generated-secret
```

The default broker policy can list buckets and read objects across the MinIO
deployment. If the portal should only serve specific buckets, replace the
wildcard bucket resources in the policy with explicit bucket ARNs before creating
or updating it.

## Postgres Storage

In production, Postgres runs on the cloud host. Its data directory is controlled
by `POSTGRES_DATA_PATH` in `.env`.

Example:

```env
POSTGRES_DATA_PATH=/data/raid/postgres
POSTGRES_BIND_ADDRESS=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_SERVICE_PORT=5432
```

`POSTGRES_BIND_ADDRESS` controls which host interface exposes PostgreSQL on the
cloud host. Keep it at `127.0.0.1` unless you have a specific reason to expose
Postgres beyond the host.

Example DBeaver connection settings:

- Host: `127.0.0.1` through an SSH tunnel to the cloud host, or the host/interface
  that matches `POSTGRES_BIND_ADDRESS`
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

The database bootstrap files live in [postgres/initdb](/home/doug/git/realgoodresearch/sysadmin/data-portal/postgres/initdb:1). On a fresh Postgres data directory they already include the current schema, including collection tags and dataset roles (`data`, `documentation`, `visuals`, `GIS`). A clean initialization creates:

- `collections`
- `collection_tags`
- `datasets`
- `dataset_tags`
- `access_tokens`
- `token_grants`

Dataset timestamps:

- `created_at`: auto-filled on insert
- `updated_at`: auto-updated on each row change
- `published_at`: now defaults to insert time unless you set it explicitly

Schema policy:

- [postgres/initdb](/home/doug/git/realgoodresearch/sysadmin/data-portal/postgres/initdb:1) is the canonical baseline for fresh databases.
- Before go-live, it is acceptable to rebuild the Postgres data directory and rely on `initdb/`.
- After go-live, every schema change should ship in two places:
  - a new forward-only SQL file under `postgres/migrations/`
  - the updated canonical schema in `postgres/initdb/001_schema.sql`
- Fresh installs should initialize from `initdb/`. Existing live databases should apply only the migration files created after they were initialized.

If `postgres/migrations/` is currently empty, that is fine. Add new migration files there only for future post-launch schema changes.

The seed file inserts one example collection, four example datasets, and two
example tokens for local testing only. These plaintext token values are public
demo fixtures and must never be used for any real deployment:

- `partner-alpha-2026-rotate-me`
- `partner-beta-2026-rotate-me`

## Rebuild Postgres From Scratch

If you have not loaded real data yet, the cleanest way to pick up the latest schema is to rebuild the Postgres data directory from scratch.

1. Stop the Postgres service:

```bash
docker compose stop postgres
```

2. Remove the existing Postgres container:

```bash
docker compose rm -f postgres
```

3. Delete the contents of the host directory referenced by `POSTGRES_DATA_PATH` in your `.env`.

Example pattern:

```bash
rm -rf /path/from/POSTGRES_DATA_PATH/*
```

Only do this if you are sure the database contains no real data you need to keep.

4. Start Postgres again:

```bash
docker compose up -d postgres
```

5. Confirm initialization succeeded:

```bash
docker compose logs postgres
```

On first startup, Postgres should run the SQL files in `postgres/initdb/`.

6. Rebuild the broker after the database is back:

```bash
docker compose up -d --build broker-api
```

For the local development stack, add `-f docker-compose.local.yml` to each
`docker compose` command in this section.

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
