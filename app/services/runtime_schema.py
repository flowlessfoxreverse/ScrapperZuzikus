from __future__ import annotations

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine


def _table_names(engine: Engine) -> set[str]:
    inspector = inspect(engine)
    try:
        return set(inspector.get_table_names())
    except Exception:
        return set()


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
    default_active = "TRUE" if dialect == "postgresql" else "1"
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
    if "last_success_at" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN last_success_at TIMESTAMP NULL")
    if "last_failure_at" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN last_failure_at TIMESTAMP NULL")
    if "cooldown_until" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN cooldown_until TIMESTAMP NULL")
    if "auto_disabled_at" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN auto_disabled_at TIMESTAMP NULL")
    if "success_count" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN success_count INTEGER NOT NULL DEFAULT 0")
    if "consecutive_failures" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0")
    if "health_score" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN health_score INTEGER NOT NULL DEFAULT 100")
    if "failure_count" not in columns:
        statements.append("ALTER TABLE proxy_endpoints ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0")
    if "is_active" not in columns:
        statements.append(f"ALTER TABLE proxy_endpoints ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT {default_active}")

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        if "kind" in columns:
            connection.execute(
                text(
                    "UPDATE proxy_endpoints "
                    "SET supports_http = CASE WHEN kind::text = 'CRAWLER' THEN TRUE ELSE supports_http END, "
                    "supports_browser = CASE WHEN kind::text = 'BROWSER' THEN TRUE ELSE supports_browser END"
                )
            )


def ensure_contact_channel_schema(engine: Engine) -> None:
    tables = _table_names(engine)
    if "companies" not in tables:
        return
    if "contact_channels" in tables:
        return

    dialect = engine.dialect.name
    if dialect == "postgresql":
        channel_type_sql = "VARCHAR(32)"
    else:
        channel_type_sql = "VARCHAR(32)"

    statements = [
        (
            "CREATE TABLE contact_channels ("
            "id INTEGER PRIMARY KEY, "
            "company_id INTEGER NOT NULL REFERENCES companies(id), "
            f"channel_type {channel_type_sql} NOT NULL, "
            "channel_value VARCHAR(255) NOT NULL, "
            "normalized_value VARCHAR(255) NOT NULL, "
            "source_type VARCHAR(32) NOT NULL DEFAULT 'link', "
            "source_page_url VARCHAR(500) NULL, "
            "technical_metadata JSON NOT NULL DEFAULT '{}' , "
            "first_seen_at TIMESTAMP NULL, "
            "last_seen_at TIMESTAMP NULL"
            ")"
        ),
        "CREATE INDEX ix_contact_channels_company_id ON contact_channels(company_id)",
        "CREATE INDEX ix_contact_channels_channel_type ON contact_channels(channel_type)",
        "CREATE INDEX ix_contact_channels_normalized_value ON contact_channels(normalized_value)",
        (
            "CREATE UNIQUE INDEX uq_company_contact_channel "
            "ON contact_channels(company_id, channel_type, normalized_value)"
        ),
    ]
    if dialect == "postgresql":
        statements[0] = (
            "CREATE TABLE contact_channels ("
            "id SERIAL PRIMARY KEY, "
            "company_id INTEGER NOT NULL REFERENCES companies(id), "
            f"channel_type {channel_type_sql} NOT NULL, "
            "channel_value VARCHAR(255) NOT NULL, "
            "normalized_value VARCHAR(255) NOT NULL, "
            "source_type VARCHAR(32) NOT NULL DEFAULT 'link', "
            "source_page_url VARCHAR(500) NULL, "
            "technical_metadata JSONB NOT NULL DEFAULT '{}'::jsonb, "
            "first_seen_at TIMESTAMP WITH TIME ZONE NULL, "
            "last_seen_at TIMESTAMP WITH TIME ZONE NULL"
            ")"
        )

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def ensure_phone_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    try:
        columns = {column["name"]: column for column in inspector.get_columns("phones")}
    except Exception:
        return

    phone_column = columns.get("phone_number")
    if phone_column is None:
        return

    current_length = getattr(phone_column.get("type"), "length", None)
    if current_length is not None and current_length >= 255:
        return

    dialect = engine.dialect.name
    if dialect != "postgresql":
        return

    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE phones ALTER COLUMN phone_number TYPE VARCHAR(255)"))


def ensure_run_company_retry_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    try:
        columns = {column["name"] for column in inspector.get_columns("run_companies")}
    except Exception:
        return

    if "retry_count" in columns:
        return

    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE run_companies ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0"))


def ensure_request_metric_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    try:
        columns = {column["name"] for column in inspector.get_columns("request_metrics")}
    except Exception:
        return

    default_false = "FALSE" if engine.dialect.name == "postgresql" else "0"
    statements: list[str] = []
    if "used_proxy" not in columns:
        statements.append(
            f"ALTER TABLE request_metrics ADD COLUMN used_proxy BOOLEAN NOT NULL DEFAULT {default_false}"
        )
    if "proxy_id" not in columns:
        statements.append("ALTER TABLE request_metrics ADD COLUMN proxy_id INTEGER NULL")
    if "proxy_label" not in columns:
        statements.append("ALTER TABLE request_metrics ADD COLUMN proxy_label VARCHAR(128) NULL")
    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
