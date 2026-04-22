#!/bin/sh
set -eu

CERT_DOMAIN="data.realgoodresearch.com"
CERT_DIR="/etc/letsencrypt/live/${CERT_DOMAIN}"
FULLCHAIN="${CERT_DIR}/fullchain.pem"
PRIVKEY="${CERT_DIR}/privkey.pem"

mkdir -p "${CERT_DIR}" /var/www/certbot

if [ ! -f "${FULLCHAIN}" ] || [ ! -f "${PRIVKEY}" ]; then
  openssl req -x509 -nodes -days 3 -newkey rsa:2048 \
    -keyout "${PRIVKEY}" \
    -out "${FULLCHAIN}" \
    -subj "/CN=${CERT_DOMAIN}" >/dev/null 2>&1
fi

(
  LAST_STATE=""
  while :; do
    CURRENT_STATE="$(find /etc/letsencrypt/live -type f \( -name '*.pem' -o -name '*.conf' \) -exec sha256sum {} + 2>/dev/null | sha256sum | awk '{print $1}')"
    if [ -n "${LAST_STATE}" ] && [ "${CURRENT_STATE}" != "${LAST_STATE}" ]; then
      nginx -s reload >/dev/null 2>&1 || true
    fi
    LAST_STATE="${CURRENT_STATE}"
    sleep 6h
  done
) &

exec nginx -g "daemon off;"
