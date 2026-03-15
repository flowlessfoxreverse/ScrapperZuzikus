"""
Tests for Phase 3 — 8a: Retry Queue

Tests the greylist retry scheduling, delay calculation,
bulk fan-out, and pipeline result shape. Celery tasks are called
eagerly (CELERY_TASK_ALWAYS_EAGER=True) so no broker is needed.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from app.workers.tasks import (
    _greylist_delay,
    _run_pipeline,
    validate_email_task,
    validate_bulk_task,
)


# ── Greylist delay schedule ───────────────────────────────────────────────────

class TestGreylistDelay:
    def test_attempt_0_is_5_min(self):
        assert _greylist_delay(0) == 300

    def test_attempt_1_is_15_min(self):
        assert _greylist_delay(1) == 900

    def test_attempt_2_is_1_hour(self):
        assert _greylist_delay(2) == 3_600

    def test_attempt_3_is_4_hours(self):
        assert _greylist_delay(3) == 14_400

    def test_beyond_schedule_returns_last(self):
        assert _greylist_delay(99) == 14_400


# ── _run_pipeline result shape ────────────────────────────────────────────────

class TestRunPipeline:
    def test_invalid_syntax_returns_immediately(self):
        result = _run_pipeline("notanemail", skip_smtp=True)
        assert result["status"] == "invalid"
        assert result["syntax_valid"] is False

    def test_valid_email_skip_smtp_returns_dict(self):
        mock_domain_result = MagicMock(
            domain_exists=True, mx_found=True, primary_mx="mx.gmail.com"
        )
        mock_loop = MagicMock()
        mock_loop.run_until_complete.return_value = mock_domain_result

        with patch("app.validators.domain.validate_domain"), \
             patch("asyncio.new_event_loop", return_value=mock_loop):
            result = _run_pipeline("user@gmail.com", skip_smtp=True)

        assert "email" in result
        assert "status" in result
        assert "score" in result

    def test_result_contains_required_keys(self):
        result = _run_pipeline("bad@@@", skip_smtp=True)
        required = {"email", "status", "score"}
        assert required.issubset(result.keys())


# ── Task execution ────────────────────────────────────────────────────────────

class TestValidateEmailTask:
    def test_task_runs_pipeline(self):
        mock_result = {
            "email": "user@example.com",
            "status": "valid",
            "score": 95,
            "smtp_verdict": None,
        }
        with patch("app.workers.tasks._run_pipeline", return_value=mock_result):
            result = validate_email_task.apply(
                kwargs={"email": "user@example.com"},
            ).get()

        assert result["status"] == "valid"
        assert result["attempts"] == 1
        assert "completed_at" in result

    def test_task_attaches_request_id(self):
        mock_result = {
            "email": "user@example.com",
            "status": "valid",
            "score": 90,
            "smtp_verdict": None,
        }
        with patch("app.workers.tasks._run_pipeline", return_value=mock_result):
            result = validate_email_task.apply(
                kwargs={
                    "email": "user@example.com",
                    "request_id": "req-abc-123",
                },
            ).get()

        assert result["request_id"] == "req-abc-123"

    def test_greylisted_result_triggers_retry(self):
        """When smtp_verdict=greylisted, task should request a retry."""
        mock_result = {
            "email": "user@example.com",
            "status": "unknown",
            "score": 50,
            "smtp_verdict": "greylisted",
        }

        task = validate_email_task.s(email="user@example.com")

        retry_called = []

        def fake_retry(*args, **kwargs):
            retry_called.append(kwargs)
            raise validate_email_task.retry.func.__class__  # stop execution

        with patch("app.workers.tasks._run_pipeline", return_value=mock_result):
            with patch.object(validate_email_task, "retry") as mock_retry:
                mock_retry.side_effect = Exception("retry")
                with pytest.raises(Exception, match="retry"):
                    validate_email_task.apply(
                        kwargs={"email": "user@example.com"},
                    ).get()

                mock_retry.assert_called_once()
                call_kwargs = mock_retry.call_args.kwargs
                assert call_kwargs.get("queue") == "retry"
                assert isinstance(call_kwargs.get("countdown"), int)


# ── Bulk task ─────────────────────────────────────────────────────────────────

class TestValidateBulkTask:
    def test_fans_out_to_individual_tasks(self):
        emails = ["a@example.com", "b@example.com", "c@example.com"]

        with patch("app.workers.tasks.validate_email_task.apply_async") as mock_apply:
            mock_apply.return_value = MagicMock(id="task-id-123")
            result = validate_bulk_task.apply(
                kwargs={"emails": emails, "skip_smtp": True},
            ).get()

        assert result["total"] == 3
        assert len(result["task_ids"]) == 3
        assert mock_apply.call_count == 3

    def test_includes_batch_id(self):
        with patch("app.workers.tasks.validate_email_task.apply_async") as mock_apply:
            mock_apply.return_value = MagicMock(id="task-id")
            result = validate_bulk_task.apply(
                kwargs={
                    "emails": ["x@example.com"],
                    "batch_id": "batch-xyz",
                    "skip_smtp": True,
                },
            ).get()
        assert result["batch_id"] == "batch-xyz"

    def test_empty_list_returns_zero_total(self):
        result = validate_bulk_task.apply(
            kwargs={"emails": [], "skip_smtp": True},
        ).get()
        assert result["total"] == 0
        assert result["task_ids"] == []
