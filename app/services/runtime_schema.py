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


def ensure_recipe_schema(engine: Engine) -> None:
    tables = _table_names(engine)
    dialect = engine.dialect.name
    inspector = inspect(engine)
    columns_by_table: dict[str, set[str]] = {}
    for table_name in tables:
        try:
            columns_by_table[table_name] = {column["name"] for column in inspector.get_columns(table_name)}
        except Exception:
            columns_by_table[table_name] = set()

    statements: list[str] = []

    if "taxonomy_verticals" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS taxonomy_verticals ("
                "id SERIAL PRIMARY KEY, "
                "slug VARCHAR(64) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "description TEXT NULL, "
                "sort_order INTEGER NOT NULL DEFAULT 0, "
                "is_active BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS taxonomy_verticals ("
                "id INTEGER PRIMARY KEY, "
                "slug VARCHAR(64) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "description TEXT NULL, "
                "sort_order INTEGER NOT NULL DEFAULT 0, "
                "is_active BOOLEAN NOT NULL DEFAULT 1, "
                "created_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append("CREATE INDEX IF NOT EXISTS ix_taxonomy_verticals_slug ON taxonomy_verticals(slug)")

    if "niche_clusters" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS niche_clusters ("
                "id SERIAL PRIMARY KEY, "
                "slug VARCHAR(64) NOT NULL UNIQUE, "
                "vertical_slug VARCHAR(64) NOT NULL REFERENCES taxonomy_verticals(slug), "
                "label VARCHAR(128) NOT NULL, "
                "description TEXT NULL, "
                "sort_order INTEGER NOT NULL DEFAULT 0, "
                "is_active BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS niche_clusters ("
                "id INTEGER PRIMARY KEY, "
                "slug VARCHAR(64) NOT NULL UNIQUE, "
                "vertical_slug VARCHAR(64) NOT NULL REFERENCES taxonomy_verticals(slug), "
                "label VARCHAR(128) NOT NULL, "
                "description TEXT NULL, "
                "sort_order INTEGER NOT NULL DEFAULT 0, "
                "is_active BOOLEAN NOT NULL DEFAULT 1, "
                "created_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append("CREATE INDEX IF NOT EXISTS ix_niche_clusters_slug ON niche_clusters(slug)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_niche_clusters_vertical_slug ON niche_clusters(vertical_slug)")

    try:
        category_columns = {column["name"] for column in inspector.get_columns("categories")}
    except Exception:
        category_columns = set()

    if "seeded_recipe_id" not in category_columns and "categories" in tables:
        statements.append("ALTER TABLE categories ADD COLUMN seeded_recipe_id INTEGER NULL")
    if "cluster_slug" not in category_columns and "categories" in tables:
        statements.append("ALTER TABLE categories ADD COLUMN cluster_slug VARCHAR(64) NULL")

    if "query_recipes" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipes ("
                "id SERIAL PRIMARY KEY, "
                "slug VARCHAR(96) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "description TEXT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "source_variant_id INTEGER NULL, "
                "status VARCHAR(10) NOT NULL DEFAULT 'draft', "
                "is_platform_template BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipes ("
                "id INTEGER PRIMARY KEY, "
                "slug VARCHAR(96) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "description TEXT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "source_variant_id INTEGER NULL, "
                "status VARCHAR(10) NOT NULL DEFAULT 'draft', "
                "is_platform_template BOOLEAN NOT NULL DEFAULT 1, "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipes_slug ON query_recipes(slug)")
    else:
        try:
            recipe_columns = {column["name"]: column for column in inspector.get_columns("query_recipes")}
        except Exception:
            recipe_columns = {}
        if "cluster_slug" not in recipe_columns:
            statements.append("ALTER TABLE query_recipes ADD COLUMN cluster_slug VARCHAR(64) NULL")
        if "source_variant_id" not in recipe_columns:
            statements.append("ALTER TABLE query_recipes ADD COLUMN source_variant_id INTEGER NULL")
        recipe_vertical = recipe_columns.get("vertical")
        recipe_vertical_length = getattr(recipe_vertical.get("type"), "length", None) if recipe_vertical else None
        if dialect == "postgresql" and recipe_vertical_length is not None and recipe_vertical_length < 64:
            statements.append("ALTER TABLE query_recipes ALTER COLUMN vertical TYPE VARCHAR(64)")

    category_vertical = None
    if "vertical" in category_columns:
        try:
            category_vertical = next(column for column in inspector.get_columns("categories") if column["name"] == "vertical")
        except StopIteration:
            category_vertical = None
    category_vertical_length = getattr(category_vertical.get("type"), "length", None) if category_vertical else None
    if dialect == "postgresql" and category_vertical_length is not None and category_vertical_length < 64:
        statements.append("ALTER TABLE categories ALTER COLUMN vertical TYPE VARCHAR(64)")

    if "query_recipe_versions" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_versions ("
                "id SERIAL PRIMARY KEY, "
                "recipe_id INTEGER NOT NULL REFERENCES query_recipes(id), "
                "version_number INTEGER NOT NULL DEFAULT 1, "
                "status VARCHAR(10) NOT NULL DEFAULT 'draft', "
                "adapter VARCHAR(16) NOT NULL DEFAULT 'overpass_public', "
                "osm_tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "exclude_tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "search_terms JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "website_keywords JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "language_hints JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "notes TEXT NULL, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_versions ("
                "id INTEGER PRIMARY KEY, "
                "recipe_id INTEGER NOT NULL REFERENCES query_recipes(id), "
                "version_number INTEGER NOT NULL DEFAULT 1, "
                "status VARCHAR(10) NOT NULL DEFAULT 'draft', "
                "adapter VARCHAR(16) NOT NULL DEFAULT 'overpass_public', "
                "osm_tags JSON NOT NULL DEFAULT '[]', "
                "exclude_tags JSON NOT NULL DEFAULT '[]', "
                "search_terms JSON NOT NULL DEFAULT '[]', "
                "website_keywords JSON NOT NULL DEFAULT '[]', "
                "language_hints JSON NOT NULL DEFAULT '[]', "
                "notes TEXT NULL, "
                "created_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_recipe_version_number ON query_recipe_versions(recipe_id, version_number)"
        )

    if "query_recipe_validations" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_validations ("
                "id SERIAL PRIMARY KEY, "
                "recipe_version_id INTEGER NOT NULL REFERENCES query_recipe_versions(id), "
                "status VARCHAR(10) NOT NULL DEFAULT 'draft', "
                "provider VARCHAR(32) NOT NULL DEFAULT 'overpass_public', "
                "sample_regions JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "score INTEGER NULL, "
                "metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "cache_key VARCHAR(255) NULL, "
                "expires_at TIMESTAMP WITH TIME ZONE NULL, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_validations ("
                "id INTEGER PRIMARY KEY, "
                "recipe_version_id INTEGER NOT NULL REFERENCES query_recipe_versions(id), "
                "status VARCHAR(10) NOT NULL DEFAULT 'draft', "
                "provider VARCHAR(32) NOT NULL DEFAULT 'overpass_public', "
                "sample_regions JSON NOT NULL DEFAULT '[]', "
                "score INTEGER NULL, "
                "metrics_json JSON NOT NULL DEFAULT '{}', "
                "cache_key VARCHAR(255) NULL, "
                "expires_at TIMESTAMP NULL, "
                "created_at TIMESTAMP NOT NULL"
                ")"
            )

    if "query_recipe_variants" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_variants ("
                "id SERIAL PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "variant_key VARCHAR(96) NOT NULL, "
                "slug VARCHAR(96) NOT NULL, "
                "label VARCHAR(128) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "provenance VARCHAR(32) NOT NULL DEFAULT 'curated_prompt', "
                "template_score INTEGER NOT NULL DEFAULT 0, "
                "prompt_match_score INTEGER NOT NULL DEFAULT 0, "
                "rank_score INTEGER NOT NULL DEFAULT 0, "
                "validation_count INTEGER NOT NULL DEFAULT 0, "
                "observed_validation_score INTEGER NOT NULL DEFAULT 0, "
                "latest_validation_score INTEGER NULL, "
                "latest_validation_status VARCHAR(32) NULL, "
                "latest_total_results INTEGER NULL, "
                "latest_website_rate DOUBLE PRECISION NULL, "
                "last_validated_at TIMESTAMP WITH TIME ZONE NULL, "
                "fit_reasons JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "rationale JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "osm_tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "exclude_tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "search_terms JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "website_keywords JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "language_hints JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_variants ("
                "id INTEGER PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "variant_key VARCHAR(96) NOT NULL, "
                "slug VARCHAR(96) NOT NULL, "
                "label VARCHAR(128) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "provenance VARCHAR(32) NOT NULL DEFAULT 'curated_prompt', "
                "template_score INTEGER NOT NULL DEFAULT 0, "
                "prompt_match_score INTEGER NOT NULL DEFAULT 0, "
                "rank_score INTEGER NOT NULL DEFAULT 0, "
                "validation_count INTEGER NOT NULL DEFAULT 0, "
                "observed_validation_score INTEGER NOT NULL DEFAULT 0, "
                "latest_validation_score INTEGER NULL, "
                "latest_validation_status VARCHAR(32) NULL, "
                "latest_total_results INTEGER NULL, "
                "latest_website_rate FLOAT NULL, "
                "last_validated_at TIMESTAMP NULL, "
                "fit_reasons JSON NOT NULL DEFAULT '[]', "
                "rationale JSON NOT NULL DEFAULT '[]', "
                "osm_tags JSON NOT NULL DEFAULT '[]', "
                "exclude_tags JSON NOT NULL DEFAULT '[]', "
                "search_terms JSON NOT NULL DEFAULT '[]', "
                "website_keywords JSON NOT NULL DEFAULT '[]', "
                "language_hints JSON NOT NULL DEFAULT '[]', "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_recipe_variant_prompt_key ON query_recipe_variants(prompt_fingerprint, variant_key)"
        )
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variants_prompt_fingerprint ON query_recipe_variants(prompt_fingerprint)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variants_slug ON query_recipe_variants(slug)")
    else:
        variant_columns = columns_by_table.get("query_recipe_variants", set())
        variant_additions = {
            "validation_count": "INTEGER NOT NULL DEFAULT 0",
            "observed_validation_score": "INTEGER NOT NULL DEFAULT 0",
            "latest_validation_score": "INTEGER NULL",
            "latest_validation_status": "VARCHAR(32) NULL",
            "latest_total_results": "INTEGER NULL",
            "latest_website_rate": "DOUBLE PRECISION NULL" if dialect == "postgresql" else "FLOAT NULL",
            "last_validated_at": "TIMESTAMP WITH TIME ZONE NULL" if dialect == "postgresql" else "TIMESTAMP NULL",
        }
        for column_name, column_def in variant_additions.items():
            if column_name not in variant_columns:
                statements.append(f"ALTER TABLE query_recipe_variants ADD COLUMN {column_name} {column_def}")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variants_slug ON query_recipe_variants(slug)")

    if "query_prompt_cluster_decisions" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_prompt_cluster_decisions ("
                "id SERIAL PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NOT NULL, "
                "match_score INTEGER NOT NULL DEFAULT 0, "
                "matched_aliases JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "rationale JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "times_seen INTEGER NOT NULL DEFAULT 0, "
                "times_selected INTEGER NOT NULL DEFAULT 0, "
                "ambiguity_count INTEGER NOT NULL DEFAULT 0, "
                "last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_prompt_cluster_decisions ("
                "id INTEGER PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NOT NULL, "
                "match_score INTEGER NOT NULL DEFAULT 0, "
                "matched_aliases JSON NOT NULL DEFAULT '[]', "
                "rationale JSON NOT NULL DEFAULT '[]', "
                "times_seen INTEGER NOT NULL DEFAULT 0, "
                "times_selected INTEGER NOT NULL DEFAULT 0, "
                "ambiguity_count INTEGER NOT NULL DEFAULT 0, "
                "last_seen_at TIMESTAMP NOT NULL, "
                "created_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_prompt_cluster_decision "
            "ON query_prompt_cluster_decisions(prompt_fingerprint, cluster_slug)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_prompt_cluster_decisions_prompt_fingerprint "
            "ON query_prompt_cluster_decisions(prompt_fingerprint)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_prompt_cluster_decisions_cluster_slug "
            "ON query_prompt_cluster_decisions(cluster_slug)"
        )

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
