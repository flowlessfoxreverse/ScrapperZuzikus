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
                "source_plan_id INTEGER NULL, "
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
                "source_plan_id INTEGER NULL, "
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
        if "source_plan_id" not in recipe_columns:
            statements.append("ALTER TABLE query_recipes ADD COLUMN source_plan_id INTEGER NULL")
        recipe_vertical = recipe_columns.get("vertical")
        recipe_vertical_length = getattr(recipe_vertical.get("type"), "length", None) if recipe_vertical else None
        if dialect == "postgresql" and recipe_vertical_length is not None and recipe_vertical_length < 64:
            statements.append("ALTER TABLE query_recipes ALTER COLUMN vertical TYPE VARCHAR(64)")

    if "query_recipe_variant_templates" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_variant_templates ("
                "id SERIAL PRIMARY KEY, "
                "key VARCHAR(96) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "sub_intent VARCHAR(96) NOT NULL, "
                "source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich', "
                "aliases JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "osm_tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "exclude_tags JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "search_terms JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "website_keywords JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "language_hints JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "rationale JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "template_score INTEGER NOT NULL DEFAULT 0, "
                "sort_order INTEGER NOT NULL DEFAULT 0, "
                "is_active BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_variant_templates ("
                "id INTEGER PRIMARY KEY, "
                "key VARCHAR(96) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "sub_intent VARCHAR(96) NOT NULL, "
                "source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich', "
                "aliases JSON NOT NULL DEFAULT '[]', "
                "osm_tags JSON NOT NULL DEFAULT '[]', "
                "exclude_tags JSON NOT NULL DEFAULT '[]', "
                "search_terms JSON NOT NULL DEFAULT '[]', "
                "website_keywords JSON NOT NULL DEFAULT '[]', "
                "language_hints JSON NOT NULL DEFAULT '[]', "
                "rationale JSON NOT NULL DEFAULT '[]', "
                "template_score INTEGER NOT NULL DEFAULT 0, "
                "sort_order INTEGER NOT NULL DEFAULT 0, "
                "is_active BOOLEAN NOT NULL DEFAULT 1, "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variant_templates_key ON query_recipe_variant_templates(key)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variant_templates_cluster_slug ON query_recipe_variant_templates(cluster_slug)")

    if "query_recipe_recommendation_policies" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_recommendation_policies ("
                "id SERIAL PRIMARY KEY, "
                "policy_key VARCHAR(64) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "source_strategy VARCHAR(32) NULL, "
                "recommended_validation_score INTEGER NOT NULL DEFAULT 55, "
                "recommended_validation_runs INTEGER NOT NULL DEFAULT 1, "
                "recommended_production_score INTEGER NOT NULL DEFAULT 0, "
                "recommended_production_runs INTEGER NOT NULL DEFAULT 0, "
                "recommended_activation_count INTEGER NOT NULL DEFAULT 0, "
                "trusted_validation_score INTEGER NOT NULL DEFAULT 65, "
                "trusted_validation_runs INTEGER NOT NULL DEFAULT 2, "
                "trusted_production_score INTEGER NOT NULL DEFAULT 15, "
                "trusted_production_runs INTEGER NOT NULL DEFAULT 1, "
                "trusted_activation_count INTEGER NOT NULL DEFAULT 1, "
                "suppression_validation_score_max INTEGER NOT NULL DEFAULT 40, "
                "suppression_validation_runs_min INTEGER NOT NULL DEFAULT 2, "
                "suppression_production_score_max INTEGER NOT NULL DEFAULT 5, "
                "suppression_production_runs_min INTEGER NOT NULL DEFAULT 1, "
                "is_active BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_recommendation_policies ("
                "id INTEGER PRIMARY KEY, "
                "policy_key VARCHAR(64) NOT NULL UNIQUE, "
                "label VARCHAR(128) NOT NULL, "
                "source_strategy VARCHAR(32) NULL, "
                "recommended_validation_score INTEGER NOT NULL DEFAULT 55, "
                "recommended_validation_runs INTEGER NOT NULL DEFAULT 1, "
                "recommended_production_score INTEGER NOT NULL DEFAULT 0, "
                "recommended_production_runs INTEGER NOT NULL DEFAULT 0, "
                "recommended_activation_count INTEGER NOT NULL DEFAULT 0, "
                "trusted_validation_score INTEGER NOT NULL DEFAULT 65, "
                "trusted_validation_runs INTEGER NOT NULL DEFAULT 2, "
                "trusted_production_score INTEGER NOT NULL DEFAULT 15, "
                "trusted_production_runs INTEGER NOT NULL DEFAULT 1, "
                "trusted_activation_count INTEGER NOT NULL DEFAULT 1, "
                "suppression_validation_score_max INTEGER NOT NULL DEFAULT 40, "
                "suppression_validation_runs_min INTEGER NOT NULL DEFAULT 2, "
                "suppression_production_score_max INTEGER NOT NULL DEFAULT 5, "
                "suppression_production_runs_min INTEGER NOT NULL DEFAULT 1, "
                "is_active BOOLEAN NOT NULL DEFAULT 1, "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_recommendation_policies_policy_key "
            "ON query_recipe_recommendation_policies(policy_key)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_recommendation_policies_source_strategy "
            "ON query_recipe_recommendation_policies(source_strategy)"
        )

    if "query_recipe_recommendation_policy_audits" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_recommendation_policy_audits ("
                "id SERIAL PRIMARY KEY, "
                "policy_key VARCHAR(64) NOT NULL, "
                "policy_label VARCHAR(128) NOT NULL, "
                "change_kind VARCHAR(32) NOT NULL DEFAULT 'manual', "
                "change_summary VARCHAR(255) NOT NULL, "
                "experiment_note VARCHAR(255) NULL, "
                "before_json JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "after_json JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "changed_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_recommendation_policy_audits ("
                "id INTEGER PRIMARY KEY, "
                "policy_key VARCHAR(64) NOT NULL, "
                "policy_label VARCHAR(128) NOT NULL, "
                "change_kind VARCHAR(32) NOT NULL DEFAULT 'manual', "
                "change_summary VARCHAR(255) NOT NULL, "
                "experiment_note VARCHAR(255) NULL, "
                "before_json JSON NOT NULL DEFAULT '{}', "
                "after_json JSON NOT NULL DEFAULT '{}', "
                "changed_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_recommendation_policy_audits_policy_key "
            "ON query_recipe_recommendation_policy_audits(policy_key)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_recommendation_policy_audits_changed_at "
            "ON query_recipe_recommendation_policy_audits(changed_at)"
        )
    else:
        audit_columns = {column["name"] for column in inspector.get_columns("query_recipe_recommendation_policy_audits")}
        if "change_kind" not in audit_columns:
            if dialect == "postgresql":
                statements.append(
                    "ALTER TABLE query_recipe_recommendation_policy_audits "
                    "ADD COLUMN IF NOT EXISTS change_kind VARCHAR(32) NOT NULL DEFAULT 'manual'"
                )
            else:
                statements.append(
                    "ALTER TABLE query_recipe_recommendation_policy_audits "
                    "ADD COLUMN change_kind VARCHAR(32) NOT NULL DEFAULT 'manual'"
                )
        if "experiment_note" not in audit_columns:
            if dialect == "postgresql":
                statements.append(
                    "ALTER TABLE query_recipe_recommendation_policy_audits "
                    "ADD COLUMN IF NOT EXISTS experiment_note VARCHAR(255) NULL"
                )
            else:
                statements.append(
                    "ALTER TABLE query_recipe_recommendation_policy_audits "
                    "ADD COLUMN experiment_note VARCHAR(255) NULL"
                )
        if "performance_snapshot_json" not in audit_columns:
            if dialect == "postgresql":
                statements.append(
                    "ALTER TABLE query_recipe_recommendation_policy_audits "
                    "ADD COLUMN IF NOT EXISTS performance_snapshot_json JSONB NOT NULL DEFAULT '{}'::jsonb"
                )
            else:
                statements.append(
                    "ALTER TABLE query_recipe_recommendation_policy_audits "
                    "ADD COLUMN performance_snapshot_json JSON NOT NULL DEFAULT '{}'"
                )

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
                "source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich', "
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
                "source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich', "
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
    else:
        version_columns = columns_by_table.get("query_recipe_versions", set())
        if "source_strategy" not in version_columns:
            statements.append(
                "ALTER TABLE query_recipe_versions ADD COLUMN source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich'"
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

    if "query_recipe_plans" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_plans ("
                "id SERIAL PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "requested_provider VARCHAR(32) NOT NULL, "
                "provider VARCHAR(32) NOT NULL, "
                "model_name VARCHAR(64) NOT NULL, "
                "planner_version VARCHAR(32) NOT NULL DEFAULT 'v1', "
                "status VARCHAR(16) NOT NULL DEFAULT 'success', "
                "market_country_code VARCHAR(2) NULL, "
                "cache_key VARCHAR(96) NOT NULL, "
                "raw_response TEXT NULL, "
                "parsed_output JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "used_fallback BOOLEAN NOT NULL DEFAULT FALSE, "
                "fallback_reason TEXT NULL, "
                "error_text TEXT NULL, "
                "expires_at TIMESTAMP WITH TIME ZONE NULL, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_plans ("
                "id INTEGER PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "requested_provider VARCHAR(32) NOT NULL, "
                "provider VARCHAR(32) NOT NULL, "
                "model_name VARCHAR(64) NOT NULL, "
                "planner_version VARCHAR(32) NOT NULL DEFAULT 'v1', "
                "status VARCHAR(16) NOT NULL DEFAULT 'success', "
                "market_country_code VARCHAR(2) NULL, "
                "cache_key VARCHAR(96) NOT NULL, "
                "raw_response TEXT NULL, "
                "parsed_output JSON NOT NULL DEFAULT '{}', "
                "used_fallback BOOLEAN NOT NULL DEFAULT 0, "
                "fallback_reason TEXT NULL, "
                "error_text TEXT NULL, "
                "expires_at TIMESTAMP NULL, "
                "created_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_plans_cache_key ON query_recipe_plans(cache_key)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_plans_prompt_fingerprint ON query_recipe_plans(prompt_fingerprint)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_plans_created_at ON query_recipe_plans(created_at)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_plans_market_country_code ON query_recipe_plans(market_country_code)")
    else:
        plan_columns = columns_by_table.get("query_recipe_plans", set())
        if "market_country_code" not in plan_columns:
            statements.append("ALTER TABLE query_recipe_plans ADD COLUMN market_country_code VARCHAR(2) NULL")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_plans_market_country_code ON query_recipe_plans(market_country_code)")

    if "query_recipe_plan_variant_outcomes" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_plan_variant_outcomes ("
                "id SERIAL PRIMARY KEY, "
                "plan_id INTEGER NOT NULL REFERENCES query_recipe_plans(id), "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "requested_provider VARCHAR(32) NOT NULL, "
                "provider VARCHAR(32) NOT NULL, "
                "model_name VARCHAR(64) NOT NULL, "
                "market_country_code VARCHAR(2) NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "variant_key VARCHAR(96) NOT NULL, "
                "source_variant_id INTEGER NULL, "
                "variant_label VARCHAR(128) NOT NULL, "
                "rank_position INTEGER NOT NULL DEFAULT 0, "
                "template_score INTEGER NOT NULL DEFAULT 0, "
                "prompt_match_score INTEGER NOT NULL DEFAULT 0, "
                "rank_score INTEGER NOT NULL DEFAULT 0, "
                "was_selected BOOLEAN NOT NULL DEFAULT FALSE, "
                "was_drafted BOOLEAN NOT NULL DEFAULT FALSE, "
                "was_activated BOOLEAN NOT NULL DEFAULT FALSE, "
                "selected_at TIMESTAMP WITH TIME ZONE NULL, "
                "drafted_at TIMESTAMP WITH TIME ZONE NULL, "
                "activated_at TIMESTAMP WITH TIME ZONE NULL, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_plan_variant_outcomes ("
                "id INTEGER PRIMARY KEY, "
                "plan_id INTEGER NOT NULL REFERENCES query_recipe_plans(id), "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "requested_provider VARCHAR(32) NOT NULL, "
                "provider VARCHAR(32) NOT NULL, "
                "model_name VARCHAR(64) NOT NULL, "
                "market_country_code VARCHAR(2) NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "variant_key VARCHAR(96) NOT NULL, "
                "source_variant_id INTEGER NULL, "
                "variant_label VARCHAR(128) NOT NULL, "
                "rank_position INTEGER NOT NULL DEFAULT 0, "
                "template_score INTEGER NOT NULL DEFAULT 0, "
                "prompt_match_score INTEGER NOT NULL DEFAULT 0, "
                "rank_score INTEGER NOT NULL DEFAULT 0, "
                "was_selected BOOLEAN NOT NULL DEFAULT 0, "
                "was_drafted BOOLEAN NOT NULL DEFAULT 0, "
                "was_activated BOOLEAN NOT NULL DEFAULT 0, "
                "selected_at TIMESTAMP NULL, "
                "drafted_at TIMESTAMP NULL, "
                "activated_at TIMESTAMP NULL, "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_recipe_plan_variant_outcome "
            "ON query_recipe_plan_variant_outcomes(plan_id, variant_key)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_plan_variant_outcomes_plan_id "
            "ON query_recipe_plan_variant_outcomes(plan_id)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_plan_variant_outcomes_variant_key "
            "ON query_recipe_plan_variant_outcomes(variant_key)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_plan_variant_outcomes_market_country_code "
            "ON query_recipe_plan_variant_outcomes(market_country_code)"
        )
    else:
        outcome_columns = columns_by_table.get("query_recipe_plan_variant_outcomes", set())
        if "market_country_code" not in outcome_columns:
            statements.append("ALTER TABLE query_recipe_plan_variant_outcomes ADD COLUMN market_country_code VARCHAR(2) NULL")
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_plan_variant_outcomes_market_country_code "
            "ON query_recipe_plan_variant_outcomes(market_country_code)"
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
                "template_key VARCHAR(96) NULL, "
                "sub_intent VARCHAR(96) NULL, "
                "source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich', "
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
                "template_key VARCHAR(96) NULL, "
                "sub_intent VARCHAR(96) NULL, "
                "source_strategy VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich', "
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
            "template_key": "VARCHAR(96) NULL",
            "sub_intent": "VARCHAR(96) NULL",
            "source_strategy": "VARCHAR(32) NOT NULL DEFAULT 'overpass_discovery_enrich'",
            "validation_count": "INTEGER NOT NULL DEFAULT 0",
            "observed_validation_score": "INTEGER NOT NULL DEFAULT 0",
            "production_run_count": "INTEGER NOT NULL DEFAULT 0",
            "production_discovered_total": "INTEGER NOT NULL DEFAULT 0",
            "production_crawled_total": "INTEGER NOT NULL DEFAULT 0",
            "production_website_company_total": "INTEGER NOT NULL DEFAULT 0",
            "production_contact_company_total": "INTEGER NOT NULL DEFAULT 0",
            "production_email_company_total": "INTEGER NOT NULL DEFAULT 0",
            "production_phone_company_total": "INTEGER NOT NULL DEFAULT 0",
            "observed_production_score": "INTEGER NOT NULL DEFAULT 0",
            "latest_validation_score": "INTEGER NULL",
            "latest_validation_status": "VARCHAR(32) NULL",
            "latest_total_results": "INTEGER NULL",
            "latest_website_rate": "DOUBLE PRECISION NULL" if dialect == "postgresql" else "FLOAT NULL",
            "last_validated_at": "TIMESTAMP WITH TIME ZONE NULL" if dialect == "postgresql" else "TIMESTAMP NULL",
            "last_production_at": "TIMESTAMP WITH TIME ZONE NULL" if dialect == "postgresql" else "TIMESTAMP NULL",
        }
        for column_name, column_def in variant_additions.items():
            if column_name not in variant_columns:
                statements.append(f"ALTER TABLE query_recipe_variants ADD COLUMN {column_name} {column_def}")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variants_slug ON query_recipe_variants(slug)")
        statements.append("CREATE INDEX IF NOT EXISTS ix_query_recipe_variants_template_key ON query_recipe_variants(template_key)")

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

    if "query_prompt_variant_decisions" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_prompt_variant_decisions ("
                "id SERIAL PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "variant_key VARCHAR(96) NOT NULL, "
                "source_variant_id INTEGER NULL, "
                "selected_count INTEGER NOT NULL DEFAULT 0, "
                "draft_created_count INTEGER NOT NULL DEFAULT 0, "
                "activated_count INTEGER NOT NULL DEFAULT 0, "
                "last_selected_at TIMESTAMP WITH TIME ZONE NULL, "
                "last_drafted_at TIMESTAMP WITH TIME ZONE NULL, "
                "last_activated_at TIMESTAMP WITH TIME ZONE NULL, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_prompt_variant_decisions ("
                "id INTEGER PRIMARY KEY, "
                "prompt_text TEXT NOT NULL, "
                "prompt_fingerprint VARCHAR(64) NOT NULL, "
                "vertical VARCHAR(64) NOT NULL, "
                "cluster_slug VARCHAR(64) NULL, "
                "variant_key VARCHAR(96) NOT NULL, "
                "source_variant_id INTEGER NULL, "
                "selected_count INTEGER NOT NULL DEFAULT 0, "
                "draft_created_count INTEGER NOT NULL DEFAULT 0, "
                "activated_count INTEGER NOT NULL DEFAULT 0, "
                "last_selected_at TIMESTAMP NULL, "
                "last_drafted_at TIMESTAMP NULL, "
                "last_activated_at TIMESTAMP NULL, "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_prompt_variant_decision "
            "ON query_prompt_variant_decisions(prompt_fingerprint, variant_key)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_prompt_variant_decisions_prompt_fingerprint "
            "ON query_prompt_variant_decisions(prompt_fingerprint)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_prompt_variant_decisions_variant_key "
            "ON query_prompt_variant_decisions(variant_key)"
        )

    if "query_recipe_variant_run_stats" not in tables:
        if dialect == "postgresql":
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_variant_run_stats ("
                "id SERIAL PRIMARY KEY, "
                "variant_id INTEGER NOT NULL, "
                "run_id INTEGER NOT NULL, "
                "category_id INTEGER NOT NULL, "
                "region_id INTEGER NOT NULL, "
                "discovered_count INTEGER NOT NULL DEFAULT 0, "
                "crawled_count INTEGER NOT NULL DEFAULT 0, "
                "website_company_count INTEGER NOT NULL DEFAULT 0, "
                "contact_company_count INTEGER NOT NULL DEFAULT 0, "
                "email_company_count INTEGER NOT NULL DEFAULT 0, "
                "phone_company_count INTEGER NOT NULL DEFAULT 0, "
                "score INTEGER NOT NULL DEFAULT 0, "
                "created_at TIMESTAMP WITH TIME ZONE NOT NULL, "
                "updated_at TIMESTAMP WITH TIME ZONE NOT NULL"
                ")"
            )
        else:
            statements.append(
                "CREATE TABLE IF NOT EXISTS query_recipe_variant_run_stats ("
                "id INTEGER PRIMARY KEY, "
                "variant_id INTEGER NOT NULL, "
                "run_id INTEGER NOT NULL, "
                "category_id INTEGER NOT NULL, "
                "region_id INTEGER NOT NULL, "
                "discovered_count INTEGER NOT NULL DEFAULT 0, "
                "crawled_count INTEGER NOT NULL DEFAULT 0, "
                "website_company_count INTEGER NOT NULL DEFAULT 0, "
                "contact_company_count INTEGER NOT NULL DEFAULT 0, "
                "email_company_count INTEGER NOT NULL DEFAULT 0, "
                "phone_company_count INTEGER NOT NULL DEFAULT 0, "
                "score INTEGER NOT NULL DEFAULT 0, "
                "created_at TIMESTAMP NOT NULL, "
                "updated_at TIMESTAMP NOT NULL"
                ")"
            )
        statements.append(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_recipe_variant_run_stat "
            "ON query_recipe_variant_run_stats(variant_id, run_id, category_id)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_variant_run_stats_variant_id "
            "ON query_recipe_variant_run_stats(variant_id)"
        )
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_recipe_variant_run_stats_run_id "
            "ON query_recipe_variant_run_stats(run_id)"
        )
    else:
        prompt_variant_columns = columns_by_table.get("query_prompt_variant_decisions", set())
        prompt_variant_additions = {
            "source_variant_id": "INTEGER NULL",
            "selected_count": "INTEGER NOT NULL DEFAULT 0",
            "draft_created_count": "INTEGER NOT NULL DEFAULT 0",
            "activated_count": "INTEGER NOT NULL DEFAULT 0",
            "last_selected_at": "TIMESTAMP WITH TIME ZONE NULL" if dialect == "postgresql" else "TIMESTAMP NULL",
            "last_drafted_at": "TIMESTAMP WITH TIME ZONE NULL" if dialect == "postgresql" else "TIMESTAMP NULL",
            "last_activated_at": "TIMESTAMP WITH TIME ZONE NULL" if dialect == "postgresql" else "TIMESTAMP NULL",
            "updated_at": "TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()" if dialect == "postgresql" else "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
        }
        for column_name, column_def in prompt_variant_additions.items():
            if column_name not in prompt_variant_columns:
                statements.append(f"ALTER TABLE query_prompt_variant_decisions ADD COLUMN {column_name} {column_def}")
        statements.append(
            "CREATE INDEX IF NOT EXISTS ix_query_prompt_variant_decisions_variant_key "
            "ON query_prompt_variant_decisions(variant_key)"
        )

    for table_name in (
        "query_recipe_variant_templates",
        "query_recipe_versions",
        "query_recipe_variants",
    ):
        if table_name in tables and "source_strategy" in columns_by_table.get(table_name, set()):
            statements.append(
                f"UPDATE {table_name} "
                "SET source_strategy = LOWER(source_strategy) "
                "WHERE source_strategy <> LOWER(source_strategy)"
            )

    if not statements:
        return

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
