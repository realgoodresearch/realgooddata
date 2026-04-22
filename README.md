# Data Portal

This directory contains a draft self-hosted data distribution portal for
`data.realgoodresearch.com`.

## Services

- `minio`: Private S3-compatible object storage
- `broker-api`: FastAPI service that validates partner access tokens and
  returns short-lived presigned download URLs
- `nginx`: TLS gateway for the frontend and reverse proxy for multiple subdomains
- `certbot`: Automated Let's Encrypt renewal sidecar

## Quick Start

1. Copy `.env.example` to `.env` and replace the MinIO credentials.
   Set `MINIO_DATA_PATH` to the host path where MinIO should store data, ideally
   your RAID-backed mount such as `/data/raid/minio`.
2. Copy `broker-api/config/tokens.example.json` to
   `broker-api/config/tokens.json` and replace the sample tokens.
3. Start the stack:

```bash
docker compose up -d --build
```

4. Request the first certificate for `data.realgoodresearch.com`:

```bash
./scripts/request-certificate.sh data.realgoodresearch.com
```

5. Reload Nginx once the certificate is issued:

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
```

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

## Token Model

Each token maps to one or more `(bucket, prefix)` access rules. A token can only
request presigned downloads for objects under its allowed prefixes.

Example request:

```bash
curl -X POST https://data.realgoodresearch.com/api/v1/download-url \
  -H 'Content-Type: application/json' \
  -H 'X-Access-Token: partner-alpha-2026-rotate-me' \
  -d '{
    "bucket": "population-estimates",
    "object_key": "partners/alpha/region-a.csv",
    "download_filename": "region-a.csv"
  }'
```
