# WA Verifier — Self-Hosted WhatsApp Number Checker

A production-ready, scalable system to verify whether phone numbers have active WhatsApp accounts.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Your Network                             │
│                                                                 │
│  ┌───────────┐    ┌─────────────┐    ┌──────────────────────┐  │
│  │  Client   │───▶│  FastAPI    │───▶│  Redis Queue         │  │
│  │ (curl /   │    │  :8000      │    │  (BullMQ-style FIFO) │  │
│  │  your UI) │    └─────────────┘    └──────────┬───────────┘  │
│  └───────────┘           │                      │              │
│                          │                      ▼              │
│                          │           ┌──────────────────────┐  │
│                          │           │  Python Worker Pool  │  │
│                          │           │  (N concurrent)      │  │
│                          │           └──────────┬───────────┘  │
│                          │                      │              │
│                          ▼                      ▼              │
│                   ┌────────────┐    ┌──────────────────────┐  │
│                   │ PostgreSQL │◀───│  Baileys Service     │  │
│                   │ (results)  │    │  Node.js :3001       │  │
│                   └────────────┘    │  (WA Web client)     │  │
│                                     └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

**Components:**
- **FastAPI** — REST API for job management (create, status, results, export)
- **Redis** — Job queue (FIFO, supports backpressure)
- **Python Worker** — Pulls jobs, sends batches to Baileys, writes results to Postgres
- **Baileys Service** — Node.js microservice that maintains a real WhatsApp Web session and exposes `/check` and `/check/bulk` endpoints
- **PostgreSQL** — Persistent storage for jobs and per-number results

---

## Quick Start

### 1. Clone and configure

```bash
git clone <your-repo>
cd wa-verifier
cp .env.example .env
# Edit .env — at minimum change POSTGRES_PASSWORD and API_KEY
```

### 2. Start all services

```bash
docker compose up -d
```

### 3. Authenticate WhatsApp (one-time setup)

The Baileys service needs to be linked to a real WhatsApp account.

```bash
# Watch the Baileys logs for the QR code
docker compose logs -f baileys
```

Scan the QR code with WhatsApp on your phone:
**WhatsApp → Settings → Linked Devices → Link a Device**

> ⚠️ **Use a dedicated WhatsApp account**, not your personal one.  
> The session is persisted in the `baileys_auth` Docker volume — back it up.

Once connected you'll see: `✅ WhatsApp connected successfully`

Verify via API:
```bash
curl http://localhost:8000/api/health
```

---

## API Usage

### Create a job (JSON)

```bash
curl -X POST http://localhost:8000/api/jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My first batch",
    "phones": ["+905301234567", "+905307654321", "+12125551234"]
  }'
```

Response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "My first batch",
  "status": "pending",
  "total_numbers": 3,
  "processed_count": 0,
  "active_count": 0,
  "inactive_count": 0,
  "error_count": 0,
  "progress_pct": 0.0,
  "created_at": "2024-01-15T10:00:00Z",
  "updated_at": "2024-01-15T10:00:00Z"
}
```

### Create a job (CSV upload)

Prepare a CSV (one number per row, or with a `phone` column header):
```
+905301234567
+905307654321
+12125551234
```

```bash
curl -X POST "http://localhost:8000/api/jobs/upload?name=MyBatch" \
  -F "file=@numbers.csv"
```

### Check job status

```bash
curl http://localhost:8000/api/jobs/{job_id}
```

### Get results

```bash
# All results (paginated)
curl "http://localhost:8000/api/jobs/{job_id}/results?limit=100&skip=0"

# Only active numbers (on WhatsApp)
curl "http://localhost:8000/api/jobs/{job_id}/results?status=active"

# Only inactive
curl "http://localhost:8000/api/jobs/{job_id}/results?status=inactive"
```

### Export as CSV

```bash
# Export all active numbers to CSV
curl "http://localhost:8000/api/jobs/{job_id}/export?status=active" \
  -o active_numbers.csv
```

### List all jobs

```bash
curl "http://localhost:8000/api/jobs/?skip=0&limit=20"

# Filter by status
curl "http://localhost:8000/api/jobs/?status=completed"
```

### Search a number across all jobs

```bash
curl "http://localhost:8000/api/numbers/search?phone=+90530"
```

### Get WhatsApp QR (if session expired)

```bash
curl http://localhost:8000/api/whatsapp/qr
# Returns base64 QR string — render it with any QR library
```

---

## Scaling

### Scale workers horizontally

```bash
# Run 5 parallel worker processes
docker compose up --scale worker=5 -d
```

### Scale Baileys sessions (for higher throughput)

Run multiple Baileys instances with a load balancer in front:

```bash
# Add to docker-compose.yml:
# baileys2, baileys3 with different AUTH_STATE_PATH values
# Each needs its own WhatsApp account linked
```

Then modify `BaileysClient` in Python to round-robin across instances.

### Throughput estimates

| Config | Numbers/hour |
|--------|-------------|
| 1 worker, 1 Baileys, 500ms delay | ~1,800 |
| 3 workers, 1 Baileys, 500ms delay | ~3,600 |
| 3 workers, 3 Baileys, 200ms delay | ~16,000 |

---

## Tuning for Safety (avoid bans)

The single most important factor in avoiding WhatsApp bans is **pacing**.

| Setting | Conservative | Moderate | Aggressive |
|---------|-------------|----------|------------|
| `CHECK_DELAY_MS` | 1000ms | 500ms | 200ms |
| `BATCH_SIZE` | 10 | 25 | 50 |
| `RATE_LIMIT_POINTS` | 5/s | 10/s | 20/s |
| Account age | 6+ months | 3+ months | Any |

Additional tips:
- Use accounts with an **established history** (real contacts, chat history)
- Don't run checks **24/7** — simulate human usage patterns
- Rotate between **multiple accounts** for large lists
- Keep numbers **per session per day** under 5,000

---

## Monitoring

### View logs

```bash
docker compose logs -f worker      # Watch job processing
docker compose logs -f baileys     # Watch WA connection
docker compose logs -f api         # Watch API requests
```

### Check queue depth

```bash
docker compose exec redis redis-cli llen wa_verifier:jobs:queue
```

### Direct Postgres query

```bash
docker compose exec postgres psql -U waverifier -d waverifier -c "
  SELECT status, COUNT(*), AVG(active_count::float/NULLIF(total_numbers,0))*100 as active_pct
  FROM jobs GROUP BY status;
"
```

---

## Backup & Recovery

### Backup WhatsApp session (critical!)

```bash
docker run --rm \
  -v wa-verifier_baileys_auth:/data \
  -v $(pwd)/backups:/backup \
  alpine tar czf /backup/baileys_auth_$(date +%Y%m%d).tar.gz /data
```

### Backup Postgres

```bash
docker compose exec postgres pg_dump -U waverifier waverifier \
  | gzip > backups/db_$(date +%Y%m%d).sql.gz
```

---

## Project Structure

```
wa-verifier/
├── docker-compose.yml
├── .env.example
│
├── baileys-service/          # Node.js WhatsApp checker
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── checker.js        # Express API + Baileys session
│
└── python-api/               # Python orchestration layer
    ├── Dockerfile
    ├── requirements.txt
    └── app/
        ├── main.py           # FastAPI app entry point
        ├── api/
        │   ├── health.py     # Health + QR endpoints
        │   ├── jobs.py       # Job CRUD + CSV upload + export
        │   └── numbers.py    # Number search
        ├── core/
        │   ├── config.py     # Settings (env-based)
        │   └── database.py   # Async SQLAlchemy engine
        ├── models/
        │   └── models.py     # Job + PhoneNumber ORM models
        ├── services/
        │   └── baileys_client.py  # HTTP client for Baileys
        └── workers/
            └── job_worker.py      # Async queue worker
```

---

## Legal & Ethical Notice

- Only verify numbers **you have a legitimate right to contact**
- Comply with **GDPR / KVKK** (Turkey) — store only what you need, delete on request
- This uses WhatsApp Web via reverse engineering — it is **against WhatsApp's ToS**
- Use at your own risk; for high-volume production use consider the **Meta Business API**
