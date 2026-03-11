#!/bin/bash
# ============================================================
# Let's Encrypt SSL 초기 발급 스크립트
#
# 사용법: sudo bash docker/init-ssl.sh
#
# 1) 자체 서명 더미 인증서 생성 → Nginx 시작 가능하게
# 2) docker compose up -d nginx
# 3) certbot으로 실제 인증서 발급
# 4) Nginx reload
# ============================================================

set -e

# .env 파일에서 변수 로드
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

DOMAIN="${DOMAIN:?ERROR: DOMAIN not set in .env}"
EMAIL="${SSL_EMAIL:-admin@${DOMAIN}}"

DATA_PATH="./docker/certbot"
CERT_PATH="/etc/letsencrypt/live/${DOMAIN}"

echo "═══════════════════════════════════════════"
echo "  Let's Encrypt SSL Setup"
echo "  Domain: ${DOMAIN}"
echo "  Email:  ${EMAIL}"
echo "═══════════════════════════════════════════"

# 1. 더미 인증서 생성 (Nginx가 시작할 수 있도록)
echo ""
echo "── Step 1: Creating dummy certificate ──"
docker compose run --rm --entrypoint "\
  mkdir -p ${CERT_PATH} && \
  openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
    -keyout '${CERT_PATH}/privkey.pem' \
    -out '${CERT_PATH}/fullchain.pem' \
    -subj '/CN=localhost'" certbot

echo "  Dummy certificate created"

# 2. Nginx 시작
echo ""
echo "── Step 2: Starting Nginx ──"
docker compose up -d nginx
sleep 3

# 3. 더미 인증서 삭제
echo ""
echo "── Step 3: Removing dummy certificate ──"
docker compose run --rm --entrypoint "\
  rm -rf /etc/letsencrypt/live/${DOMAIN} && \
  rm -rf /etc/letsencrypt/archive/${DOMAIN} && \
  rm -rf /etc/letsencrypt/renewal/${DOMAIN}.conf" certbot

# 4. 실제 인증서 발급
echo ""
echo "── Step 4: Requesting Let's Encrypt certificate ──"
docker compose run --rm --entrypoint "\
  certbot certonly --webroot \
    -w /var/www/certbot \
    --email ${EMAIL} \
    --agree-tos \
    --no-eff-email \
    -d ${DOMAIN}" certbot

# 5. Nginx reload
echo ""
echo "── Step 5: Reloading Nginx ──"
docker compose exec nginx nginx -s reload

echo ""
echo "═══════════════════════════════════════════"
echo "  SSL Setup Complete!"
echo "  https://${DOMAIN} is ready"
echo "═══════════════════════════════════════════"
