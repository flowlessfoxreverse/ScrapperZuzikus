"""
HTTP client for the emailvalidator microservice.

Submits emails for async bulk validation via POST /bulk.
All network calls are best-effort: failures are logged and the
caller decides whether to revert the pending-status mark.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def submit_bulk_validation(
    emails: list[str],
    *,
    validator_url: str,
    webhook_url: str | None = None,
    skip_smtp: bool = False,
) -> str | None:
    """
    POST a list of email addresses to the emailvalidator /bulk endpoint.

    Returns the batch_id string on success, or None if the call failed.
    The caller is responsible for marking emails as pending before calling
    this function and reverting if None is returned.
    """
    url = validator_url.rstrip("/") + "/bulk"
    payload: dict = {"emails": emails, "skip_smtp": skip_smtp}
    if webhook_url:
        payload["webhook_url"] = webhook_url

    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            batch_id = result.get("batch_id")
            logger.info(
                "emailvalidator bulk submitted: %d emails, batch_id=%s",
                len(emails),
                batch_id,
            )
            return batch_id
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:500]
        logger.warning("emailvalidator /bulk HTTP %s: %s", exc.code, body)
        return None
    except Exception as exc:
        logger.warning("emailvalidator /bulk failed: %s", exc)
        return None
