"""
Tests for Layer 9 — FastAPI routes

Uses FastAPI's dependency_overrides to inject mocks for DB and Redis,
avoiding any real Postgres/Redis connection in the test environment.

Covers:
  POST /validate         — fast pipeline, cache hit/miss, field contracts
  POST /validate/full    — job creation, 202 response shape
  POST /bulk             — deduplication, estimate, 422 validation
  GET  /result/{job_id}  — pending / success / failure states
  GET  /health           — fields present, disposable count
  GET  /admin/proxy-stats — utilization calculation
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.api import deps
from app.api.schemas import EmailStatus


# ── Dependency mock factories ─────────────────────────────────────────────────

def make_db():
    session = AsyncMock()
    session.get = AsyncMock(return_value=None)
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.execute = AsyncMock(
        return_value=MagicMock(scalar_one_or_none=MagicMock(return_value=None))
    )
    return session


def make_redis():
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)   # cache miss by default
    r.setex = AsyncMock(return_value=True)
    r.ping = AsyncMock(return_value=True)
    return r


# FastAPI dependency overrides — inject mocks at the app level
async def _override_get_db():
    yield make_db()

async def _override_get_redis():
    yield make_redis()


# ── Client fixture with dependency overrides ──────────────────────────────────

@pytest.fixture
def client():
    app.dependency_overrides[deps.get_db]    = _override_get_db
    app.dependency_overrides[deps.get_redis] = _override_get_redis
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def client_with_cache_hit():
    """Client where Redis always returns a pre-built cached result."""
    cached = _build_cached_payload()

    async def _redis_with_hit():
        r = make_redis()
        r.get = AsyncMock(return_value=json.dumps(cached))
        yield r

    app.dependency_overrides[deps.get_db]    = _override_get_db
    app.dependency_overrides[deps.get_redis] = _redis_with_hit
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _good_validation_response():
    from app.api.schemas import ValidationResponse, ValidationDetail
    return ValidationResponse(
        email="user@gmail.com",
        normalized="user@gmail.com",
        status=EmailStatus.RISKY,
        score=70,
        reasons=[],
        detail=ValidationDetail(
            syntax_valid=True, domain_exists=True, mx_found=True,
            is_disposable=False, is_role_based=False,
            typo_suggestion=None, smtp_verdict=None,
            primary_mx="gmail-smtp-in.l.google.com",
        ),
        validated_at=datetime.now(timezone.utc),
    )


def _build_cached_payload() -> dict:
    r = _good_validation_response()
    d = r.model_dump()
    d["validated_at"] = d["validated_at"].isoformat()
    d["detail"] = {
        "syntax_valid": True, "domain_exists": True, "mx_found": True,
        "is_disposable": False, "is_role_based": False,
        "typo_suggestion": None, "smtp_verdict": None, "primary_mx": None,
    }
    return d


# ── POST /validate ────────────────────────────────────────────────────────────

class TestValidateEndpoint:
    def test_returns_200(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            resp = client.post("/validate", json={"email": "user@gmail.com"})
        assert resp.status_code == 200

    def test_response_shape(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            data = client.post("/validate", json={"email": "user@gmail.com"}).json()
        for field in ("email", "status", "score", "reasons", "detail", "validated_at"):
            assert field in data, f"Missing field: {field}"

    def test_detail_has_layer_signals(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            detail = client.post("/validate", json={"email": "user@gmail.com"}).json()["detail"]
        assert "syntax_valid" in detail
        assert "mx_found" in detail
        assert "is_disposable" in detail

    def test_score_in_range(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            score = client.post("/validate", json={"email": "user@gmail.com"}).json()["score"]
        assert 0 <= score <= 100

    def test_status_is_valid_enum(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            status = client.post("/validate", json={"email": "user@gmail.com"}).json()["status"]
        assert status in ("valid", "risky", "invalid", "unknown")

    def test_cache_hit_returns_cached_true(self, client_with_cache_hit):
        resp = client_with_cache_hit.post("/validate", json={"email": "user@gmail.com"})
        assert resp.status_code == 200
        assert resp.json()["cached"] is True

    def test_cache_hit_skips_pipeline(self, client_with_cache_hit):
        with patch("app.api.routes._run_fast_pipeline") as mock_pipe:
            client_with_cache_hit.post("/validate", json={"email": "user@gmail.com"})
        mock_pipe.assert_not_called()

    def test_whitespace_stripped(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()) as mock_pipe:
            client.post("/validate", json={"email": "  user@gmail.com  "})
        called_email = mock_pipe.call_args[0][0]
        assert called_email == "user@gmail.com"

    def test_missing_email_returns_422(self, client):
        assert client.post("/validate", json={}).status_code == 422

    def test_request_id_echoed_in_response(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            resp = client.post(
                "/validate",
                json={"email": "user@gmail.com"},
                headers={"X-Request-ID": "my-trace-id"},
            )
        assert resp.headers.get("X-Request-ID") == "my-trace-id"

    def test_response_time_header_present(self, client):
        with patch("app.api.routes._run_fast_pipeline",
                   return_value=_good_validation_response()):
            resp = client.post("/validate", json={"email": "user@gmail.com"})
        assert "X-Response-Time" in resp.headers


# ── POST /validate/full ───────────────────────────────────────────────────────

class TestValidateFullEndpoint:
    def test_returns_202(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock(id="task-1")
            resp = client.post("/validate/full", json={"email": "user@gmail.com"})
        assert resp.status_code == 202

    def test_response_has_job_id(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock(id="task-1")
            data = client.post("/validate/full", json={"email": "user@gmail.com"}).json()
        assert "job_id" in data
        assert len(data["job_id"]) > 0

    def test_status_is_pending(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            data = client.post("/validate/full", json={"email": "user@gmail.com"}).json()
        assert data["status"] == "pending"

    def test_poll_url_contains_job_id(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            data = client.post("/validate/full", json={"email": "user@gmail.com"}).json()
        assert data["job_id"] in data["poll_url"]

    def test_queued_at_present(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            data = client.post("/validate/full", json={"email": "user@gmail.com"}).json()
        assert "queued_at" in data

    def test_task_is_dispatched(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            client.post("/validate/full", json={"email": "user@gmail.com"})
        mock_celery.send_task.assert_called_once()


# ── POST /bulk ────────────────────────────────────────────────────────────────

class TestBulkEndpoint:
    def test_returns_202(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            resp = client.post("/bulk", json={"emails": ["a@x.com", "b@x.com"]})
        assert resp.status_code == 202

    def test_total_matches_input(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            data = client.post("/bulk", json={"emails": ["a@x.com", "b@x.com", "c@x.com"]}).json()
        assert data["total"] == 3

    def test_deduplicates_emails(self, client):
        emails = ["dup@x.com", "dup@x.com", "other@x.com"]
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            data = client.post("/bulk", json={"emails": emails}).json()
        assert data["total"] == 2

    def test_empty_list_returns_422(self, client):
        assert client.post("/bulk", json={"emails": []}).status_code == 422

    def test_over_10000_returns_422(self, client):
        emails = [f"u{i}@x.com" for i in range(10_001)]
        assert client.post("/bulk", json={"emails": emails}).status_code == 422

    def test_skip_smtp_faster_estimate(self, client):
        emails = ["a@x.com"] * 100
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            full = client.post("/bulk", json={"emails": emails, "skip_smtp": False}).json()
            fast = client.post("/bulk", json={"emails": emails, "skip_smtp": True}).json()
        assert full["estimated_seconds"] > fast["estimated_seconds"]

    def test_batch_id_in_poll_url(self, client):
        with patch("app.api.routes.celery_app") as mock_celery:
            mock_celery.send_task.return_value = MagicMock()
            data = client.post("/bulk", json={"emails": ["a@x.com"]}).json()
        assert data["batch_id"] in data["poll_url"]


# ── GET /result/{job_id} ──────────────────────────────────────────────────────

class TestResultEndpoint:
    def _mock_celery(self, state: str, result=None):
        mock = MagicMock()
        mock_task = MagicMock()
        mock_task.state = state
        mock_task.result = result
        mock.AsyncResult.return_value = mock_task
        return mock

    def test_pending_returns_pending(self, client):
        with patch("app.api.routes.celery_app", self._mock_celery("PENDING")):
            data = client.get("/result/job-123").json()
        assert data["status"] == "pending"

    def test_processing_returns_processing(self, client):
        with patch("app.api.routes.celery_app", self._mock_celery("STARTED")):
            data = client.get("/result/job-123").json()
        assert data["status"] == "processing"

    def test_failed_returns_failed_with_error(self, client):
        with patch("app.api.routes.celery_app",
                   self._mock_celery("FAILURE", Exception("probe failed"))):
            data = client.get("/result/job-123").json()
        assert data["status"] == "failed"
        assert data["error"] is not None

    def test_success_returns_completed_with_result(self, client):
        task_result = {
            "email": "user@gmail.com", "normalized": "user@gmail.com",
            "status": "valid", "score": 95, "reasons": [],
            "syntax_valid": True, "domain_exists": True, "mx_found": True,
            "is_disposable": False, "is_role_based": False,
            "typo_suggestion": None, "smtp_verdict": "valid",
            "primary_mx": "gmail-smtp-in.l.google.com",
        }
        with patch("app.api.routes.celery_app",
                   self._mock_celery("SUCCESS", task_result)):
            data = client.get("/result/job-123").json()
        assert data["status"] == "completed"
        assert data["result"]["email"] == "user@gmail.com"
        assert data["result"]["score"] == 95

    def test_success_result_has_detail(self, client):
        task_result = {
            "email": "user@gmail.com", "normalized": "user@gmail.com",
            "status": "valid", "score": 95, "reasons": [],
            "syntax_valid": True, "domain_exists": True, "mx_found": True,
            "is_disposable": False, "is_role_based": False,
            "typo_suggestion": None, "smtp_verdict": "valid",
            "primary_mx": "gmail-smtp-in.l.google.com",
        }
        with patch("app.api.routes.celery_app",
                   self._mock_celery("SUCCESS", task_result)):
            data = client.get("/result/job-123").json()
        assert "detail" in data["result"]
        assert data["result"]["detail"]["smtp_verdict"] == "valid"

    def test_job_id_echoed(self, client):
        with patch("app.api.routes.celery_app", self._mock_celery("PENDING")):
            data = client.get("/result/my-specific-job").json()
        assert data["job_id"] == "my-specific-job"


# ── GET /health ───────────────────────────────────────────────────────────────

class TestHealthEndpoint:
    def test_returns_200(self, client):
        with patch("app.api.routes.celery_app"):
            assert client.get("/health").status_code == 200

    def test_has_required_fields(self, client):
        with patch("app.api.routes.celery_app"):
            data = client.get("/health").json()
        for field in ("status", "db", "redis", "workers", "disposable_domains", "version"):
            assert field in data, f"Missing field: {field}"

    def test_disposable_domains_positive(self, client):
        with patch("app.api.routes.celery_app"):
            data = client.get("/health").json()
        assert data["disposable_domains"] > 0

    def test_version_present(self, client):
        with patch("app.api.routes.celery_app"):
            data = client.get("/health").json()
        assert data["version"] == "0.1.0"

    def test_db_error_shows_degraded(self, client):
        """When DB is unavailable the health check degrades gracefully."""
        async def _bad_db():
            session = AsyncMock()
            session.execute = AsyncMock(side_effect=Exception("connection refused"))
            session.commit = AsyncMock()
            session.rollback = AsyncMock()
            yield session

        app.dependency_overrides[deps.get_db] = _bad_db
        with patch("app.api.routes.celery_app"):
            data = client.get("/health").json()
        app.dependency_overrides[deps.get_db] = _override_get_db
        assert data["status"] == "degraded"
        assert "error" in data["db"].lower()


# ── GET /admin/proxy-stats ────────────────────────────────────────────────────

class TestProxyStatsEndpoint:
    def _mock_pool(self, entries):
        pool = MagicMock()
        pool.size = len(entries)
        pool.available = [MagicMock() for e in entries if not e["is_exhausted"]]
        pool.stats.return_value = entries
        return pool

    def test_returns_200(self, client):
        entries = [{"name": "p1", "uses_today": 100, "daily_limit": 8000,
                    "is_exhausted": False, "is_direct": False, "host": "p.test.com"}]
        with patch("app.api.routes.get_pool", return_value=self._mock_pool(entries)):
            assert client.get("/admin/proxy-stats").status_code == 200

    def test_total_proxies_count(self, client):
        entries = [
            {"name": "p1", "uses_today": 100, "daily_limit": 8000,
             "is_exhausted": False, "is_direct": False, "host": "p1.test.com"},
            {"name": "p2", "uses_today": 50, "daily_limit": 8000,
             "is_exhausted": False, "is_direct": True, "host": None},
        ]
        with patch("app.api.routes.get_pool", return_value=self._mock_pool(entries)):
            data = client.get("/admin/proxy-stats").json()
        assert data["total_proxies"] == 2

    def test_utilization_calculated_correctly(self, client):
        entries = [{"name": "p1", "uses_today": 4000, "daily_limit": 8000,
                    "is_exhausted": False, "is_direct": False, "host": "p.test.com"}]
        with patch("app.api.routes.get_pool", return_value=self._mock_pool(entries)):
            data = client.get("/admin/proxy-stats").json()
        assert data["proxies"][0]["utilization_pct"] == 50.0

    def test_exhausted_proxy_flagged(self, client):
        entries = [{"name": "p1", "uses_today": 8000, "daily_limit": 8000,
                    "is_exhausted": True, "is_direct": False, "host": "p.test.com"}]
        with patch("app.api.routes.get_pool", return_value=self._mock_pool(entries)):
            data = client.get("/admin/proxy-stats").json()
        assert data["proxies"][0]["is_exhausted"] is True
        assert data["available_proxies"] == 0

    def test_direct_proxy_flagged(self, client):
        entries = [{"name": "direct", "uses_today": 10, "daily_limit": 8000,
                    "is_exhausted": False, "is_direct": True, "host": None}]
        with patch("app.api.routes.get_pool", return_value=self._mock_pool(entries)):
            data = client.get("/admin/proxy-stats").json()
        assert data["proxies"][0]["is_direct"] is True
        assert data["proxies"][0]["host"] is None
