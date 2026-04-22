#!/bin/sh
set -eu

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 domain [domain ...]" >&2
  exit 1
fi

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

if [ ! -f "${PROJECT_DIR}/.env" ]; then
  echo "Missing ${PROJECT_DIR}/.env" >&2
  exit 1
fi

LETSENCRYPT_EMAIL="$(grep '^LETSENCRYPT_EMAIL=' "${PROJECT_DIR}/.env" | cut -d '=' -f 2-)"
if [ -z "${LETSENCRYPT_EMAIL}" ]; then
  echo "LETSENCRYPT_EMAIL is missing in ${PROJECT_DIR}/.env" >&2
  exit 1
fi

DOMAIN_ARGS=""
for domain in "$@"; do
  DOMAIN_ARGS="${DOMAIN_ARGS} -d ${domain}"
done

cd "${PROJECT_DIR}"
docker compose run --rm certbot certonly \
  --webroot \
  -w /var/www/certbot \
  --email "${LETSENCRYPT_EMAIL}" \
  --agree-tos \
  --no-eff-email \
  ${DOMAIN_ARGS}
