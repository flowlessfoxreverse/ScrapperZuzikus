.PHONY: up down logs restart scale-workers shell-api shell-db qr status clean

# ── Lifecycle ───────────────────────────────────────────────────
up:
	docker compose up -d --build
	@echo "✅ All services started"
	@echo "   API:     http://localhost:8000"
	@echo "   API docs: http://localhost:8000/docs"
	@echo "   Baileys: http://localhost:3001/health"

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

logs-worker:
	docker compose logs -f worker

logs-baileys:
	docker compose logs -f baileys

# ── WhatsApp ────────────────────────────────────────────────────
qr:
	@echo "Fetching WhatsApp QR code..."
	@curl -s http://localhost:8000/api/whatsapp/qr | python3 -c \
		"import sys,json; d=json.load(sys.stdin); print('Status:', d.get('status','?')); \
		qr=d.get('qr'); \
		print('QR data available, install qrcode-terminal: npx qrcode-terminal \"'+qr+'\"') if qr else print('No QR - already connected or starting up')"

wa-status:
	@curl -s http://localhost:8000/api/health | python3 -m json.tool

# ── Scaling ─────────────────────────────────────────────────────
scale-workers:
	@read -p "Number of workers [3]: " n; n=$${n:-3}; \
	docker compose up --scale worker=$$n -d
	@echo "Scaled workers"

# ── Database ────────────────────────────────────────────────────
migrate:
	docker compose exec api alembic upgrade head

shell-db:
	docker compose exec postgres psql -U waverifier -d waverifier

stats:
	@docker compose exec postgres psql -U waverifier -d waverifier -c \
		"SELECT status, COUNT(*) as jobs, SUM(total_numbers) as total_nums, \
		SUM(active_count) as active, SUM(inactive_count) as inactive \
		FROM jobs GROUP BY status ORDER BY status;"

queue-depth:
	@docker compose exec redis redis-cli llen wa_verifier:jobs:queue

# ── Dev shells ──────────────────────────────────────────────────
shell-api:
	docker compose exec api bash

shell-worker:
	docker compose exec worker bash

# ── Backup ──────────────────────────────────────────────────────
backup:
	@mkdir -p backups
	@docker compose exec postgres pg_dump -U waverifier waverifier \
		| gzip > backups/db_$$(date +%Y%m%d_%H%M%S).sql.gz
	@docker run --rm \
		-v wa-verifier_baileys_auth:/data \
		-v $$(pwd)/backups:/backup \
		alpine tar czf /backup/baileys_auth_$$(date +%Y%m%d_%H%M%S).tar.gz /data
	@echo "✅ Backed up DB and Baileys session to ./backups/"

# ── Cleanup ─────────────────────────────────────────────────────
clean:
	docker compose down -v
	@echo "⚠️  All volumes deleted (including WA session and DB)"
