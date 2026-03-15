# Email Validator Service

A layered email validation service similar to Debounce, built with Python + FastAPI.

## Validation Pipeline

| Layer | What it checks | Infra needed |
|-------|---------------|--------------|
| 1. Syntax | Format validity | None |
| 2. Domain + MX | Domain exists, accepts mail | None |
| 3. Disposable | Temp/throwaway domains | None |
| 4. Typo detection | gmial.com → gmail.com | None |
| 5. SMTP probe | Mailbox existence | 1 clean IP |
| 6. Catch-all detection | 3-probe algorithm | 1 clean IP |
| 7. Risk scoring | Combined 0–100 score | None |
| 8a. Retry queue | Greylisting + bulk jobs | Redis + Celery |
| 8b. IP rotation | Multiple exit IPs | Proxy pool |
| 9. API + persistence | REST API, result cache | FastAPI + Postgres |

## Project Structure

```
emailvalidator/
├── app/
│   ├── validators/
│   │   ├── syntax.py        # Layer 1
│   │   ├── domain.py        # Layer 2
│   │   ├── disposable.py    # Layer 3
│   │   ├── typo.py          # Layer 4
│   │   ├── smtp.py          # Layer 5
│   │   ├── catchall.py      # Layer 6
│   │   ├── scorer.py        # Layer 7
│   │   ├── proxy.py         # Layer 8b — IP rotation
│   │   └── types.py
│   ├── api/
│   │   ├── routes.py        # /validate, /bulk, /result
│   │   ├── schemas.py       # Pydantic request/response models
│   │   └── deps.py          # FastAPI dependencies
│   ├── workers/
│   │   ├── tasks.py         # Celery tasks (Layer 8a)
│   │   └── proxy_pool.py    # Per-worker proxy singleton
│   ├── db/
│   │   ├── models.py        # SQLAlchemy models
│   │   └── session.py       # Async DB session
│   └── main.py
├── tests/
├── alembic/
├── docker/
│   └── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Running locally

```bash
docker-compose up --build
```

API: `http://localhost:8000`  |  Docs: `http://localhost:8000/docs`

## Running tests

```bash
docker-compose run --rm api pytest tests/ -v
```

---

## IP Infrastructure & Proxy Scaling

SMTP probing (layers 5–6) requires a clean exit IP not on major blocklists.
**Standard residential, datacenter, and mobile proxies all block port 25** and
cannot be used. Only specialist SMTP proxies or VPS IPs with port 25 open work.

### Option A — VPS IPs (recommended for MVP)

Own the IP, configure PTR/rDNS yourself, no per-verification cost.

| Provider | Port 25 | Cost | Notes |
|----------|---------|------|-------|
| **Hetzner** (EU) | Open on request | €4–6/mo | Best price/performance, fast ticket |
| **Contabo** (EU/US) | Open on Medium+ plans, KYC required | €5–8/mo | Cheapest, KYC takes ~1 day |
| **OVH** (EU/US/CA) | Open by default on VPS SSD | €6–10/mo | Large ASN, decent reputation |
| **AvaHosting** | Open by default | €5–8/mo | Flexible, no restrictions |
| **Vultr** | Closed since ~2022 | — | Not recommended |
| **DigitalOcean / AWS / GCP** | Blocked permanently | — | Do not use |

**Setup checklist per VPS IP:**
1. Set PTR (rDNS) to match your HELO hostname — e.g. `mail.yourdomain.com`
2. Add SPF: `v=spf1 ip4:<vps-ip> ~all`
3. Set `PROXY_N_HELO` env var to the PTR hostname
4. Set `PROXY_N_FROM` to a real address on your domain
5. Verify port 25: `telnet alt1.gmail-smtp-in.l.google.com 25`

### Option B — Specialist SMTP Proxies (recommended at scale)

When VPS IPs hit their limits or you need geographic diversity.

| Provider | Type | Cost | Capacity | Notes |
|----------|------|------|----------|-------|
| **proxy4smtp.com** | SOCKS5, port 25 explicit | $49/proxy/mo | ~10k verif/day | Only well-known public provider for this use case. Used by Reacher.email |

**Why not residential proxies?**
All mainstream providers (Bright Data, Oxylabs, Smartproxy, IPRoyal) block port 25
to protect their IP pools. Connections silently time out. Do not attempt.

**Why not mobile proxies?**
Port 25 is blocked at the carrier level on all mobile networks worldwide.

**Why not datacenter proxies?**
Datacenter ASNs (AWS, Hetzner shared ranges, etc.) are on blocklists used by Gmail,
Outlook, and Yahoo. Connections are refused before EHLO.

### Scaling guide

| Daily volume | Setup | Approx cost |
|-------------|-------|-------------|
| 0–10k/day | 1 Hetzner VPS | €5/mo |
| 10k–50k/day | 3–5 Hetzner VPS IPs | €15–25/mo |
| 50k–200k/day | 5 VPS + 2 proxy4smtp proxies | €120–170/mo |
| 200k–500k/day | 10 proxy4smtp proxies | ~$490/mo |
| 500k+/day | proxy4smtp volume discount | custom |

### Proxy configuration

```yaml
# docker-compose.yml environment section
environment:
  # Direct VPS connection (no SOCKS5 — uses server's own IP)
  PROXY_1_HELO: mail.yourdomain.com
  PROXY_1_FROM: probe@yourdomain.com
  PROXY_1_DAILY_LIMIT: 8000

  # proxy4smtp SOCKS5 proxy
  PROXY_2_HOST: socks.proxy4smtp.com
  PROXY_2_PORT: 1080
  PROXY_2_USER: your_username
  PROXY_2_PASS: your_password
  PROXY_2_HELO: mail.yourdomain.com
  PROXY_2_FROM: probe@yourdomain.com
  PROXY_2_DAILY_LIMIT: 8000

  # Rotation: round_robin | least_used | random
  PROXY_ROTATION_STRATEGY: least_used
```

Proxy stats: `GET /admin/proxy-stats`

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/validate` | Single email, sync, layers 1–4 (fast, no SMTP) |
| POST | `/validate/full` | Single email, async with SMTP, returns job ID |
| POST | `/bulk` | Up to 10,000 emails, background processing |
| GET | `/result/{job_id}` | Poll async result |
| GET | `/health` | Health check |
| GET | `/admin/proxy-stats` | Proxy pool usage |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL async connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis (results + domain cache) |
| `CELERY_BROKER_URL` | `redis://localhost:6379/1` | Celery broker |
| `SMTP_HELO_HOSTNAME` | `mail.validator.example.com` | Must match PTR record |
| `SMTP_FROM_ADDRESS` | `probe@validator.example.com` | MAIL FROM address |
| `PROXY_N_HOST` | — | SOCKS5 host for proxy N (1–9), empty = direct |
| `PROXY_N_PORT` | `1080` | SOCKS5 port |
| `PROXY_N_USER` | — | SOCKS5 username (optional) |
| `PROXY_N_PASS` | — | SOCKS5 password (optional) |
| `PROXY_N_HELO` | — | HELO hostname for this proxy |
| `PROXY_N_FROM` | — | MAIL FROM for this proxy |
| `PROXY_N_DAILY_LIMIT` | `8000` | Max verifications/day |
| `PROXY_ROTATION_STRATEGY` | `round_robin` | `round_robin` \| `least_used` \| `random` |
| `DEBUG` | `false` | Enable debug logging |
