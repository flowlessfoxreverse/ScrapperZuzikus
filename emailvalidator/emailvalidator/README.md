# Email Validator Service

Layered email validation service for the ScrapperZuzikus platform.

This service should sit after contact discovery in the main scraper pipeline:

1. `overpass` / later `google_maps` discovers companies
2. main app crawls websites and extracts emails
3. main app sends emails to `emailvalidator`
4. `emailvalidator` validates and scores them
5. validation result is written back to the **main app database**

This service is not the lead system of record. The main app remains the canonical owner of:
- companies
- emails
- phones
- contact channels
- runs
- recipe/category performance

`emailvalidator` is a specialized validation microservice with its own storage and job lifecycle.

## Recommended integration pattern

Use **separate PostgreSQL databases** per service.

Recommended deployment:
- main scraper app DB: business entities and pipeline state
- emailvalidator DB: validation cache, async jobs, SMTP/proxy telemetry

Why this is the right scaling pattern:
- validation traffic can grow independently of scraping traffic
- validator schema can evolve without coupling to the main app DB
- failures in validator jobs should not lock or migrate the main lead DB
- the main app can be rebuilt or replaced without losing validator cache strategy

The main app should call this service over HTTP, not by sharing tables.

## Main app write-back contract

The validator should write back to the main project through a narrow callback/update API.

Recommended future main-app endpoint:

```text
POST /api/email-validation-results
```

Suggested request payload:

```json
{
  "email_id": 12345,
  "email": "info@example.com",
  "status": "valid",
  "score": 92,
  "reasons": ["smtp_accept", "mx_found"],
  "detail": {
    "syntax_valid": true,
    "domain_exists": true,
    "mx_found": true,
    "is_disposable": false,
    "is_role_based": false,
    "typo_suggestion": null,
    "smtp_verdict": "accept",
    "primary_mx": "mx1.example.com"
  },
  "validated_at": "2026-03-16T12:34:56Z",
  "provider_job_id": "uuid-or-batch-id"
}
```

Main app update behavior:
- update `emails.validation_status`
- update `emails.technical_metadata`
- optionally update `emails.suppression_status`
- keep `emails` table as the canonical user-facing state

Suggested status mapping:

| emailvalidator | main app `ValidationStatus` |
|---|---|
| `valid` | `valid` |
| `invalid` | `invalid` |
| `risky` | `risky` |
| `unknown` | `unknown` |

The main app should also persist validator details into `technical_metadata`, for example:

```json
{
  "validator": {
    "service": "emailvalidator",
    "job_id": "uuid",
    "score": 92,
    "reasons": ["smtp_accept", "mx_found"],
    "detail": {
      "smtp_verdict": "accept",
      "primary_mx": "mx1.example.com"
    },
    "validated_at": "2026-03-16T12:34:56Z"
  }
}
```

## Recommended pipeline contract

### Synchronous path

Use for fast UI checks or low-volume verification:

```text
POST /validate
```

Main app can call this when:
- validating a single email on demand
- rechecking one changed email
- admin/manual review flow

### Asynchronous path

Use for production scraping batches:

```text
POST /validate/full
POST /bulk
GET /result/{job_id}
```

Recommended main-app usage:
- once crawl extracts new emails, enqueue them in batches
- include `email_id` as external reference in your orchestration layer
- when result completes:
  - callback to main app endpoint, or
  - main app polls and writes back

Preferred pattern for production:
- emailvalidator owns validation execution
- main app owns final status persistence

## Ownership boundaries

### emailvalidator owns
- syntax/domain/MX/disposable/typo/SMTP/catch-all logic
- validation job queue
- proxy rotation for SMTP probing
- validator-local result cache
- validator-local telemetry

### main app owns
- all companies and contacts
- dedupe/canonical company merge
- scrape runs and recipe/category linkage
- user-visible validation state in `emails`
- downstream suppression/export logic

## Required identifiers between services

To connect safely, keep these IDs in the orchestration payload:
- `email_id`
- `company_id` optional but useful
- `run_id` optional
- `region_id` optional
- `category_id` optional

Minimum required identifier is:
- `email_id`

That allows validator results to write back deterministically even if the same email string exists more than once across companies or runs.

## Integration notes for future developers

- Do not make `emailvalidator` directly update the scraper DB by shared ORM models.
- Do not let the main app query validator tables directly.
- Do not duplicate company/contact ownership inside validator storage.
- Do treat validator DB as disposable operational storage plus cache.
- Do keep main app DB as the source of truth for lead records.
- Do prefer callback or small polling workers over cross-service joins.

## Validation pipeline

| Layer | What it checks | Infra needed |
|-------|---------------|--------------|
| 1. Syntax | Format validity | None |
| 2. Domain + MX | Domain exists, accepts mail | None |
| 3. Disposable | Temp/throwaway domains | None |
| 4. Typo detection | gmial.com -> gmail.com | None |
| 5. SMTP probe | Mailbox existence | 1 clean IP |
| 6. Catch-all detection | 3-probe algorithm | 1 clean IP |
| 7. Risk scoring | Combined 0-100 score | None |
| 8a. Retry queue | Greylisting + bulk jobs | Redis + Celery |
| 8b. IP rotation | Multiple exit IPs | Proxy pool |
| 9. API + persistence | REST API, result cache | FastAPI + Postgres |

## Project structure

```text
emailvalidator/
├── app/
│   ├── validators/
│   │   ├── syntax.py
│   │   ├── domain.py
│   │   ├── disposable.py
│   │   ├── typo.py
│   │   ├── smtp.py
│   │   ├── catchall.py
│   │   ├── scorer.py
│   │   ├── proxy.py
│   │   └── types.py
│   ├── api/
│   │   ├── routes.py
│   │   ├── schemas.py
│   │   └── deps.py
│   ├── workers/
│   │   ├── tasks.py
│   │   └── proxy_pool.py
│   ├── db/
│   │   ├── models.py
│   │   └── session.py
│   └── main.py
├── tests/
├── alembic/
├── docker/
│   └── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## Running locally

```bash
docker-compose up --build
```

API: `http://localhost:8000`  
Docs: `http://localhost:8000/docs`

## Running tests

```bash
docker-compose run --rm api pytest tests/ -v
```

## IP infrastructure and proxy scaling

SMTP probing (layers 5-6) requires a clean exit IP not on major blocklists.
Standard residential, datacenter, and mobile proxies usually block port 25 and
cannot be used. Only specialist SMTP proxies or VPS IPs with port 25 open work.

### Option A - VPS IPs

Best for MVP and controlled infrastructure.

| Provider | Port 25 | Cost | Notes |
|----------|---------|------|-------|
| Hetzner | Open on request | EUR4-EUR6/mo | Best price/performance |
| Contabo | Open on Medium+ plans, KYC required | EUR5-EUR8/mo | Cheapest |
| OVH | Open by default on some VPS plans | EUR6-EUR10/mo | Large ASN |
| AvaHosting | Open by default | EUR5-EUR8/mo | Flexible |

Setup checklist:
1. Set PTR/rDNS to match HELO host, e.g. `mail.yourdomain.com`
2. Add SPF for the probe IP
3. Set `PROXY_N_HELO`
4. Set `PROXY_N_FROM`
5. Verify port 25 connectivity

### Option B - Specialist SMTP proxies

For scale or geographic spread.

| Provider | Type | Cost | Notes |
|----------|------|------|-------|
| proxy4smtp.com | SOCKS5, port 25 explicit | ~$49/proxy/mo | Niche provider for SMTP traffic |

### Scaling guide

| Daily volume | Setup | Approx cost |
|-------------|-------|-------------|
| 0-10k/day | 1 VPS IP | low |
| 10k-50k/day | 3-5 VPS IPs | low-medium |
| 50k-200k/day | VPS + specialist SMTP proxies | medium |
| 200k+/day | specialist SMTP proxy pool | high |

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/validate` | Single email, sync, layers 1-4 |
| POST | `/validate/full` | Single email, async with SMTP |
| POST | `/bulk` | Up to 10,000 emails, background processing |
| GET | `/result/{job_id}` | Poll async result |
| GET | `/health` | Health check |
| GET | `/admin/proxy-stats` | Proxy pool usage |

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | - | PostgreSQL async connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis |
| `CELERY_BROKER_URL` | `redis://localhost:6379/1` | Celery broker |
| `SMTP_HELO_HOSTNAME` | `mail.validator.example.com` | Must match PTR |
| `SMTP_FROM_ADDRESS` | `probe@validator.example.com` | MAIL FROM |
| `PROXY_N_HOST` | - | SOCKS5 host for proxy N |
| `PROXY_N_PORT` | `1080` | SOCKS5 port |
| `PROXY_N_USER` | - | SOCKS5 username |
| `PROXY_N_PASS` | - | SOCKS5 password |
| `PROXY_N_HELO` | - | HELO hostname for this proxy |
| `PROXY_N_FROM` | - | MAIL FROM for this proxy |
| `PROXY_N_DAILY_LIMIT` | `8000` | Max verifications/day |
| `PROXY_ROTATION_STRATEGY` | `round_robin` | Rotation strategy |
| `DEBUG` | `false` | Debug logging |
