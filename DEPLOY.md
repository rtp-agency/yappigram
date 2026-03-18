# Deployment Guide — crm.metra-ai.org

## Prerequisites
- Server with Docker & Docker Compose
- Domain `crm.metra-ai.org` pointing to server IP
- Ports 80 and 443 open

## 1. Clone & Configure

```bash
git clone <repo-url> /opt/yappigram
cd /opt/yappigram

# Copy and fill in secrets
cp .env.example .env
nano .env  # Set all passwords, tokens, etc.
```

Generate secrets:
```bash
# JWT_SECRET
openssl rand -hex 32

# POSTGRES_PASSWORD
openssl rand -hex 16

# REDIS_PASSWORD
openssl rand -hex 16
```

## 2. Obtain SSL Certificate

```bash
# Start nginx temporarily for ACME challenge
docker compose -f docker-compose.prod.yml up -d nginx

# Get certificate
docker run --rm \
  -v yappigram_certs:/etc/letsencrypt \
  -v yappigram_certbot-www:/var/www/certbot \
  certbot/certbot certonly \
  --webroot -w /var/www/certbot \
  -d crm.metra-ai.org \
  --email admin@metra-ai.org \
  --agree-tos --no-eff-email

docker compose -f docker-compose.prod.yml down
```

## 3. Deploy

```bash
docker compose -f docker-compose.prod.yml up -d --build
```

## 4. Set Telegram Bot WebApp URL

In BotFather, set the Mini App URL to: `https://crm.metra-ai.org`

## 5. Verify

```bash
curl https://crm.metra-ai.org/api/health
# Should return: {"status":"ok"}
```

## Updates

```bash
cd /opt/yappigram
git pull
docker compose -f docker-compose.prod.yml up -d --build backend frontend
```

## SSL Renewal

Certbot container auto-renews. To force:
```bash
docker compose -f docker-compose.prod.yml exec certbot certbot renew
docker compose -f docker-compose.prod.yml exec nginx nginx -s reload
```
