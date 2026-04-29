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
PRIMARY_DOMAIN=""
for domain in "$@"; do
  if [ -z "${PRIMARY_DOMAIN}" ]; then
    PRIMARY_DOMAIN="${domain}"
  fi
  DOMAIN_ARGS="${DOMAIN_ARGS} -d ${domain}"
done

cd "${PROJECT_DIR}"

LIVE_DIR="${PROJECT_DIR}/certbot/conf/live/${PRIMARY_DOMAIN}"
ARCHIVE_DIR="${PROJECT_DIR}/certbot/conf/archive/${PRIMARY_DOMAIN}"
RENEWAL_CONF="${PROJECT_DIR}/certbot/conf/renewal/${PRIMARY_DOMAIN}.conf"

# If nginx created a temporary self-signed cert, clear that placeholder state
# so certbot can issue the real certificate.
if [ -d "${LIVE_DIR}" ] && [ ! -s "${RENEWAL_CONF}" ]; then
  rm -rf "${LIVE_DIR}" "${ARCHIVE_DIR}"
  rm -f "${RENEWAL_CONF}"
fi

docker compose run --rm --entrypoint certbot certbot certonly \
  --webroot \
  -w /var/www/certbot \
  --email "${LETSENCRYPT_EMAIL}" \
  --agree-tos \
  --no-eff-email \
  ${DOMAIN_ARGS}
