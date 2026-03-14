from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def ensure_scrape_run_control_columns(engine: Engine) -> None:
    inspector = inspect(engine)
    try:
        columns = {column["name"] for column in inspector.get_columns("scrape_runs")}
    except Exception:
        return

    statements: list[str] = []
    dialect = engine.dialect.name
    if "cancel_requested" not in columns:
        default_false = "FALSE" if dialect == "postgresql" else "0"
        statements.append(f"ALTER TABLE scrape_runs ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT {default_false}")
    if "cancel_requested_at" not in columns:
        statements.append("ALTER TABLE scrape_runs ADD COLUMN cancel_requested_at TIMESTAMP NULL")
    if "cancel_reason" not in columns:
        statements.append("ALTER TABLE scrape_runs ADD COLUMN cancel_reason TEXT NULL")

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def ensure_proxy_pool_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    try:
        columns = {column["name"] for column in inspector.get_columns("proxy_endpoints")}
    except Exception:
        return

    statements: list[str] = []
    dialect = engine.dialect.name
    default_true = "TRUE" if dialect == "postgresql" else "1"
    default_http_capacity = "8"
    default_browser_capacity = "1"
    if "supports_http" not in columns:
        statements.append(f"ALTER TABLE proxy_endpoints ADD COLUMN supports_http BOOLEAN NOT NULL DEFAULT {default_true}")
    if "supports_browser" not in columns:
        statements.append(f"ALTER TABLE proxy_endpoints ADD COLUMN supports_browser BOOLEAN NOT NULL DEFAULT {default_true}")
    if "max_http_leases" not in columns:
        statements.append(f"ALTER TABLE proxy_endpoints ADD COLUMN max_http_leases INTEGER NOT NULL DEFAULT {default_http_capacity}")
    if "max_browser_leases" not in columns:
        statements.append(f"ALTER TABLE proxy_endpoints ADD COLUMN max_browser_leases INTEGER NOT NULL DEFAULT {default_browser_capacity}")

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        if "kind" in columns:
            connection.execute(
                text(
                    "UPDATE proxy_endpoints "
                    "SET supports_http = CASE WHEN kind = 'crawler' THEN TRUE ELSE supports_http END, "
                    "supports_browser = CASE WHEN kind = 'browser' THEN TRUE ELSE supports_browser END"
                )
            )
