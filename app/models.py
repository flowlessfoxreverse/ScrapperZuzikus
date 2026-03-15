from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum

from sqlalchemy import JSON, Boolean, Date, DateTime, Enum as SqlEnum, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ValidationStatus(str, Enum):
    UNKNOWN = "unknown"
    PENDING = "pending"
    VALID = "valid"
    INVALID = "invalid"
    RISKY = "risky"


class FormStatus(str, Enum):
    NONE = "none"
    DETECTED = "detected"
    QUEUED = "queued"
    SUBMITTED = "submitted"
    MANUAL_REVIEW = "manual_review"


class SubmissionStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    FAILED = "failed"
    MANUAL_REVIEW = "manual_review"


class RunCompanyStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ProxyKind(str, Enum):
    BROWSER = "browser"
    CRAWLER = "crawler"


class ContactChannelType(str, Enum):
    WHATSAPP = "whatsapp"
    TELEGRAM = "telegram"


class RecipeStatus(str, Enum):
    DRAFT = "draft"
    CANDIDATE = "candidate"
    VALIDATED = "validated"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    REJECTED = "rejected"


class RecipeAdapter(str, Enum):
    OVERPASS_PUBLIC = "overpass_public"
    OVERPASS_LOCAL = "overpass_local"


class RecipeSourceStrategy(str, Enum):
    OVERPASS_DISCOVERY_ENRICH = "overpass_discovery_enrich"
    BROWSER_ASSISTED_DISCOVERY = "browser_assisted_discovery"
    WEBSITE_FIRST = "website_first"
    HYBRID_DISCOVERY = "hybrid_discovery"
    DIRECTORY_EXPANSION = "directory_expansion"


RECIPE_SOURCE_STRATEGY_ENUM = SqlEnum(
    RecipeSourceStrategy,
    values_callable=lambda enum_cls: [member.value for member in enum_cls],
    native_enum=False,
)


class TaxonomyVertical(Base):
    __tablename__ = "taxonomy_verticals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    label: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    clusters: Mapped[list["NicheCluster"]] = relationship(back_populates="vertical_ref")


class NicheCluster(Base):
    __tablename__ = "niche_clusters"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    vertical_slug: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    label: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    vertical_ref: Mapped["TaxonomyVertical"] = relationship(back_populates="clusters")


class Region(Base):
    __tablename__ = "regions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    country_code: Mapped[str] = mapped_column(String(2), index=True)
    osm_admin_level: Mapped[int] = mapped_column(Integer, default=2)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    companies: Mapped[list["Company"]] = relationship(back_populates="region")
    runs: Mapped[list["ScrapeRun"]] = relationship(back_populates="region")
    category_states: Mapped[list["RegionCategoryState"]] = relationship(back_populates="region")


class ProxyEndpoint(Base):
    __tablename__ = "proxy_endpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    proxy_url: Mapped[str] = mapped_column(String(500), unique=True)
    kind: Mapped[ProxyKind] = mapped_column(SqlEnum(ProxyKind), default=ProxyKind.BROWSER, index=True)
    supports_http: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    supports_browser: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    max_http_leases: Mapped[int] = mapped_column(Integer, default=8)
    max_browser_leases: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    leased_by: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    leased_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    auto_disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    consecutive_failures: Mapped[int] = mapped_column(Integer, default=0)
    health_score: Mapped[int] = mapped_column(Integer, default=100)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    leases: Mapped[list["ProxyLease"]] = relationship(back_populates="proxy", cascade="all, delete-orphan")


class ProxyLease(Base):
    __tablename__ = "proxy_leases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    proxy_id: Mapped[int] = mapped_column(ForeignKey("proxy_endpoints.id"), index=True)
    owner: Mapped[str] = mapped_column(String(255), index=True)
    workload: Mapped[ProxyKind] = mapped_column(SqlEnum(ProxyKind), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    proxy: Mapped["ProxyEndpoint"] = relationship(back_populates="leases")


class Category(Base):
    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    label: Mapped[str] = mapped_column(String(128))
    vertical: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    cluster_slug: Mapped[str | None] = mapped_column(String(64), ForeignKey("niche_clusters.slug"), nullable=True, index=True)
    osm_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    search_terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    companies: Mapped[list["CompanyCategory"]] = relationship(back_populates="category")
    run_items: Mapped[list["RunCategory"]] = relationship(back_populates="category")
    region_states: Mapped[list["RegionCategoryState"]] = relationship(back_populates="category")
    seeded_recipe_id: Mapped[int | None] = mapped_column(ForeignKey("query_recipes.id"), nullable=True)
    seeded_recipe: Mapped["QueryRecipe | None"] = relationship(foreign_keys=[seeded_recipe_id])
    vertical_ref: Mapped["TaxonomyVertical | None"] = relationship(foreign_keys=[vertical])
    cluster_ref: Mapped["NicheCluster | None"] = relationship(foreign_keys=[cluster_slug])


class QueryRecipe(Base):
    __tablename__ = "query_recipes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(96), unique=True, index=True)
    label: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    vertical: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    cluster_slug: Mapped[str | None] = mapped_column(String(64), ForeignKey("niche_clusters.slug"), nullable=True, index=True)
    source_variant_id: Mapped[int | None] = mapped_column(ForeignKey("query_recipe_variants.id"), nullable=True, index=True)
    status: Mapped[RecipeStatus] = mapped_column(SqlEnum(RecipeStatus), default=RecipeStatus.DRAFT, index=True)
    is_platform_template: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    versions: Mapped[list["QueryRecipeVersion"]] = relationship(
        back_populates="recipe",
        cascade="all, delete-orphan",
        order_by="QueryRecipeVersion.version_number.desc()",
    )
    vertical_ref: Mapped["TaxonomyVertical | None"] = relationship(foreign_keys=[vertical])
    cluster_ref: Mapped["NicheCluster | None"] = relationship(foreign_keys=[cluster_slug])
    source_variant: Mapped["QueryRecipeVariant | None"] = relationship(foreign_keys=[source_variant_id])


class QueryRecipeVariantTemplate(Base):
    __tablename__ = "query_recipe_variant_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    key: Mapped[str] = mapped_column(String(96), unique=True, index=True)
    label: Mapped[str] = mapped_column(String(128))
    vertical: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    cluster_slug: Mapped[str | None] = mapped_column(String(64), ForeignKey("niche_clusters.slug"), nullable=True, index=True)
    sub_intent: Mapped[str] = mapped_column(String(96), index=True)
    source_strategy: Mapped[RecipeSourceStrategy] = mapped_column(RECIPE_SOURCE_STRATEGY_ENUM, default=RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, index=True)
    aliases: Mapped[list[str]] = mapped_column(JSON, default=list)
    osm_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    exclude_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    search_terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    website_keywords: Mapped[list[str]] = mapped_column(JSON, default=list)
    language_hints: Mapped[list[str]] = mapped_column(JSON, default=list)
    rationale: Mapped[list[str]] = mapped_column(JSON, default=list)
    template_score: Mapped[int] = mapped_column(Integer, default=0)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    vertical_ref: Mapped["TaxonomyVertical | None"] = relationship(foreign_keys=[vertical])
    cluster_ref: Mapped["NicheCluster | None"] = relationship(foreign_keys=[cluster_slug])


class QueryRecipeVersion(Base):
    __tablename__ = "query_recipe_versions"
    __table_args__ = (
        UniqueConstraint("recipe_id", "version_number", name="uq_recipe_version_number"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recipe_id: Mapped[int] = mapped_column(ForeignKey("query_recipes.id"), index=True)
    version_number: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[RecipeStatus] = mapped_column(SqlEnum(RecipeStatus), default=RecipeStatus.DRAFT, index=True)
    adapter: Mapped[RecipeAdapter] = mapped_column(SqlEnum(RecipeAdapter), default=RecipeAdapter.OVERPASS_PUBLIC, index=True)
    source_strategy: Mapped[RecipeSourceStrategy] = mapped_column(RECIPE_SOURCE_STRATEGY_ENUM, default=RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, index=True)
    osm_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    exclude_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    search_terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    website_keywords: Mapped[list[str]] = mapped_column(JSON, default=list)
    language_hints: Mapped[list[str]] = mapped_column(JSON, default=list)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    recipe: Mapped["QueryRecipe"] = relationship(back_populates="versions")
    validations: Mapped[list["QueryRecipeValidation"]] = relationship(
        back_populates="recipe_version",
        cascade="all, delete-orphan",
        order_by="QueryRecipeValidation.created_at.desc()",
    )


class QueryRecipeValidation(Base):
    __tablename__ = "query_recipe_validations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    recipe_version_id: Mapped[int] = mapped_column(ForeignKey("query_recipe_versions.id"), index=True)
    status: Mapped[RecipeStatus] = mapped_column(SqlEnum(RecipeStatus), default=RecipeStatus.DRAFT, index=True)
    provider: Mapped[str] = mapped_column(String(32), default="overpass_public", index=True)
    sample_regions: Mapped[list[str]] = mapped_column(JSON, default=list)
    score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metrics_json: Mapped[dict] = mapped_column(JSON, default=dict)
    cache_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    recipe_version: Mapped["QueryRecipeVersion"] = relationship(back_populates="validations")


class QueryRecipeVariant(Base):
    __tablename__ = "query_recipe_variants"
    __table_args__ = (
        UniqueConstraint("prompt_fingerprint", "variant_key", name="uq_recipe_variant_prompt_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_text: Mapped[str] = mapped_column(Text)
    prompt_fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    variant_key: Mapped[str] = mapped_column(String(96), index=True)
    slug: Mapped[str] = mapped_column(String(96), index=True)
    label: Mapped[str] = mapped_column(String(128))
    vertical: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    cluster_slug: Mapped[str | None] = mapped_column(String(64), ForeignKey("niche_clusters.slug"), nullable=True, index=True)
    template_key: Mapped[str | None] = mapped_column(String(96), nullable=True, index=True)
    sub_intent: Mapped[str | None] = mapped_column(String(96), nullable=True, index=True)
    source_strategy: Mapped[RecipeSourceStrategy] = mapped_column(RECIPE_SOURCE_STRATEGY_ENUM, default=RecipeSourceStrategy.OVERPASS_DISCOVERY_ENRICH, index=True)
    provenance: Mapped[str] = mapped_column(String(32), default="curated_prompt", index=True)
    template_score: Mapped[int] = mapped_column(Integer, default=0)
    prompt_match_score: Mapped[int] = mapped_column(Integer, default=0)
    rank_score: Mapped[int] = mapped_column(Integer, default=0)
    validation_count: Mapped[int] = mapped_column(Integer, default=0)
    observed_validation_score: Mapped[int] = mapped_column(Integer, default=0)
    production_run_count: Mapped[int] = mapped_column(Integer, default=0)
    production_discovered_total: Mapped[int] = mapped_column(Integer, default=0)
    production_crawled_total: Mapped[int] = mapped_column(Integer, default=0)
    production_website_company_total: Mapped[int] = mapped_column(Integer, default=0)
    production_contact_company_total: Mapped[int] = mapped_column(Integer, default=0)
    production_email_company_total: Mapped[int] = mapped_column(Integer, default=0)
    production_phone_company_total: Mapped[int] = mapped_column(Integer, default=0)
    observed_production_score: Mapped[int] = mapped_column(Integer, default=0)
    latest_validation_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_validation_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    latest_total_results: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_website_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_validated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_production_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    fit_reasons: Mapped[list[str]] = mapped_column(JSON, default=list)
    rationale: Mapped[list[str]] = mapped_column(JSON, default=list)
    osm_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    exclude_tags: Mapped[list[dict[str, str]]] = mapped_column(JSON, default=list)
    search_terms: Mapped[list[str]] = mapped_column(JSON, default=list)
    website_keywords: Mapped[list[str]] = mapped_column(JSON, default=list)
    language_hints: Mapped[list[str]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    vertical_ref: Mapped["TaxonomyVertical | None"] = relationship(foreign_keys=[vertical])
    cluster_ref: Mapped["NicheCluster | None"] = relationship(foreign_keys=[cluster_slug])


class QueryPromptClusterDecision(Base):
    __tablename__ = "query_prompt_cluster_decisions"
    __table_args__ = (
        UniqueConstraint("prompt_fingerprint", "cluster_slug", name="uq_prompt_cluster_decision"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_text: Mapped[str] = mapped_column(Text)
    prompt_fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    vertical: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    cluster_slug: Mapped[str] = mapped_column(String(64), ForeignKey("niche_clusters.slug"), index=True)
    match_score: Mapped[int] = mapped_column(Integer, default=0)
    matched_aliases: Mapped[list[str]] = mapped_column(JSON, default=list)
    rationale: Mapped[list[str]] = mapped_column(JSON, default=list)
    times_seen: Mapped[int] = mapped_column(Integer, default=0)
    times_selected: Mapped[int] = mapped_column(Integer, default=0)
    ambiguity_count: Mapped[int] = mapped_column(Integer, default=0)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    vertical_ref: Mapped["TaxonomyVertical | None"] = relationship(foreign_keys=[vertical])
    cluster_ref: Mapped["NicheCluster | None"] = relationship(foreign_keys=[cluster_slug])


class QueryPromptVariantDecision(Base):
    __tablename__ = "query_prompt_variant_decisions"
    __table_args__ = (
        UniqueConstraint("prompt_fingerprint", "variant_key", name="uq_prompt_variant_decision"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    prompt_text: Mapped[str] = mapped_column(Text)
    prompt_fingerprint: Mapped[str] = mapped_column(String(64), index=True)
    vertical: Mapped[str] = mapped_column(String(64), ForeignKey("taxonomy_verticals.slug"), index=True)
    cluster_slug: Mapped[str | None] = mapped_column(String(64), ForeignKey("niche_clusters.slug"), nullable=True, index=True)
    variant_key: Mapped[str] = mapped_column(String(96), index=True)
    source_variant_id: Mapped[int | None] = mapped_column(ForeignKey("query_recipe_variants.id"), nullable=True, index=True)
    selected_count: Mapped[int] = mapped_column(Integer, default=0)
    draft_created_count: Mapped[int] = mapped_column(Integer, default=0)
    activated_count: Mapped[int] = mapped_column(Integer, default=0)
    last_selected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_drafted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    vertical_ref: Mapped["TaxonomyVertical | None"] = relationship(foreign_keys=[vertical])
    cluster_ref: Mapped["NicheCluster | None"] = relationship(foreign_keys=[cluster_slug])
    source_variant: Mapped["QueryRecipeVariant | None"] = relationship(foreign_keys=[source_variant_id])


class QueryRecipeVariantRunStat(Base):
    __tablename__ = "query_recipe_variant_run_stats"
    __table_args__ = (
        UniqueConstraint("variant_id", "run_id", "category_id", name="uq_recipe_variant_run_stat"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    variant_id: Mapped[int] = mapped_column(ForeignKey("query_recipe_variants.id"), index=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    discovered_count: Mapped[int] = mapped_column(Integer, default=0)
    crawled_count: Mapped[int] = mapped_column(Integer, default=0)
    website_company_count: Mapped[int] = mapped_column(Integer, default=0)
    contact_company_count: Mapped[int] = mapped_column(Integer, default=0)
    email_company_count: Mapped[int] = mapped_column(Integer, default=0)
    phone_company_count: Mapped[int] = mapped_column(Integer, default=0)
    score: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    variant: Mapped["QueryRecipeVariant"] = relationship(foreign_keys=[variant_id])
    run: Mapped["ScrapeRun"] = relationship(foreign_keys=[run_id])
    category: Mapped["Category"] = relationship(foreign_keys=[category_id])
    region: Mapped["Region"] = relationship(foreign_keys=[region_id])


class Company(Base):
    __tablename__ = "companies"
    __table_args__ = (
        UniqueConstraint("region_id", "source", "external_ref", name="uq_company_source_ref"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    website_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source: Mapped[str] = mapped_column(String(64), default="overpass")
    external_ref: Mapped[str] = mapped_column(String(128))
    source_query: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_payload: Mapped[dict] = mapped_column(JSON, default=dict)
    latitude: Mapped[str | None] = mapped_column(String(32), nullable=True)
    longitude: Mapped[str | None] = mapped_column(String(32), nullable=True)
    crawl_status: Mapped[str] = mapped_column(String(32), default="pending")
    has_contact_form: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    region: Mapped["Region"] = relationship(back_populates="companies")
    categories: Mapped[list["CompanyCategory"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    pages: Mapped[list["Page"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    emails: Mapped[list["Email"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    phones: Mapped[list["Phone"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    contact_channels: Mapped[list["ContactChannel"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    forms: Mapped[list["Form"]] = relationship(back_populates="company", cascade="all, delete-orphan")
    run_companies: Mapped[list["RunCompany"]] = relationship(back_populates="company", cascade="all, delete-orphan")


class CompanyCategory(Base):
    __tablename__ = "company_categories"
    __table_args__ = (
        UniqueConstraint("company_id", "category_id", name="uq_company_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)

    company: Mapped["Company"] = relationship(back_populates="categories")
    category: Mapped["Category"] = relationship(back_populates="companies")


class Page(Base):
    __tablename__ = "pages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    url: Mapped[str] = mapped_column(String(500))
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    crawled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    has_contact_form: Mapped[bool] = mapped_column(Boolean, default=False)
    crawl_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    company: Mapped["Company"] = relationship(back_populates="pages")


class Email(Base):
    __tablename__ = "emails"
    __table_args__ = (
        UniqueConstraint("company_id", "email", name="uq_company_email"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    email: Mapped[str] = mapped_column(String(255), index=True)
    source_type: Mapped[str] = mapped_column(String(32), default="regex")
    source_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    validation_status: Mapped[ValidationStatus] = mapped_column(SqlEnum(ValidationStatus), default=ValidationStatus.UNKNOWN, index=True)
    suppression_status: Mapped[str] = mapped_column(String(32), default="clear")
    technical_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    company: Mapped["Company"] = relationship(back_populates="emails")


class Phone(Base):
    __tablename__ = "phones"
    __table_args__ = (
        UniqueConstraint("company_id", "normalized_number", name="uq_company_phone"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    phone_number: Mapped[str] = mapped_column(String(255), index=True)
    normalized_number: Mapped[str] = mapped_column(String(32), index=True)
    source_type: Mapped[str] = mapped_column(String(32), default="regex")
    source_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    technical_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    company: Mapped["Company"] = relationship(back_populates="phones")


class ContactChannel(Base):
    __tablename__ = "contact_channels"
    __table_args__ = (
        UniqueConstraint("company_id", "channel_type", "normalized_value", name="uq_company_contact_channel"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    channel_type: Mapped[ContactChannelType] = mapped_column(SqlEnum(ContactChannelType), index=True)
    channel_value: Mapped[str] = mapped_column(String(255), index=True)
    normalized_value: Mapped[str] = mapped_column(String(255), index=True)
    source_type: Mapped[str] = mapped_column(String(32), default="link")
    source_page_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    technical_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    company: Mapped["Company"] = relationship(back_populates="contact_channels")


class Form(Base):
    __tablename__ = "forms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    page_url: Mapped[str] = mapped_column(String(500))
    action_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    method: Mapped[str] = mapped_column(String(16), default="get")
    has_captcha: Mapped[bool] = mapped_column(Boolean, default=False)
    is_js_challenge: Mapped[bool] = mapped_column(Boolean, default=False)
    status: Mapped[FormStatus] = mapped_column(SqlEnum(FormStatus), default=FormStatus.DETECTED)
    schema_json: Mapped[dict] = mapped_column(JSON, default=dict)
    last_checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    company: Mapped["Company"] = relationship(back_populates="forms")
    submissions: Mapped[list["Submission"]] = relationship(back_populates="form", cascade="all, delete-orphan")


class Submission(Base):
    __tablename__ = "submissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    form_id: Mapped[int] = mapped_column(ForeignKey("forms.id"), index=True)
    status: Mapped[SubmissionStatus] = mapped_column(SqlEnum(SubmissionStatus), default=SubmissionStatus.PENDING)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    response_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_excerpt: Mapped[str | None] = mapped_column(Text, nullable=True)
    manual_review_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    form: Mapped["Form"] = relationship(back_populates="submissions")


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    status: Mapped[RunStatus] = mapped_column(SqlEnum(RunStatus), default=RunStatus.PENDING, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    overpass_queries_used: Mapped[int] = mapped_column(Integer, default=0)
    discovered_count: Mapped[int] = mapped_column(Integer, default=0)
    crawled_count: Mapped[int] = mapped_column(Integer, default=0)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    region: Mapped["Region"] = relationship(back_populates="runs")
    categories: Mapped[list["RunCategory"]] = relationship(back_populates="run", cascade="all, delete-orphan")
    companies: Mapped[list["RunCompany"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class RunCategory(Base):
    __tablename__ = "run_categories"
    __table_args__ = (
        UniqueConstraint("run_id", "category_id", name="uq_run_category"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)

    run: Mapped["ScrapeRun"] = relationship(back_populates="categories")
    category: Mapped["Category"] = relationship(back_populates="run_items")


class DailyUsage(Base):
    __tablename__ = "daily_usage"
    __table_args__ = (
        UniqueConstraint("usage_date", "provider", name="uq_usage_date_provider"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usage_date: Mapped[date] = mapped_column(Date, index=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    units_used: Mapped[int] = mapped_column(Integer, default=0)
    cap: Mapped[int] = mapped_column(Integer)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)


class RegionCategoryState(Base):
    __tablename__ = "region_category_states"
    __table_args__ = (
        UniqueConstraint("region_id", "category_id", name="uq_region_category_state"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    region_id: Mapped[int] = mapped_column(ForeignKey("regions.id"), index=True)
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"), index=True)
    last_run_id: Mapped[int | None] = mapped_column(ForeignKey("scrape_runs.id"), nullable=True)
    last_discovery_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_discovery_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_result_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(32), default="never_run")
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    region: Mapped["Region"] = relationship(back_populates="category_states")
    category: Mapped["Category"] = relationship(back_populates="region_states")


class RunCompany(Base):
    __tablename__ = "run_companies"
    __table_args__ = (
        UniqueConstraint("run_id", "company_id", name="uq_run_company"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("scrape_runs.id"), index=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id"), index=True)
    status: Mapped[RunCompanyStatus] = mapped_column(SqlEnum(RunCompanyStatus), default=RunCompanyStatus.QUEUED, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    queued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    run: Mapped["ScrapeRun"] = relationship(back_populates="companies")
    company: Mapped["Company"] = relationship(back_populates="run_companies")


class RequestMetric(Base):
    __tablename__ = "request_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(ForeignKey("scrape_runs.id"), nullable=True, index=True)
    company_id: Mapped[int | None] = mapped_column(ForeignKey("companies.id"), nullable=True, index=True)
    proxy_id: Mapped[int | None] = mapped_column(ForeignKey("proxy_endpoints.id"), nullable=True, index=True)
    proxy_label: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    request_kind: Mapped[str] = mapped_column(String(32), index=True)
    method: Mapped[str] = mapped_column(String(8))
    url: Mapped[str] = mapped_column(String(500))
    used_proxy: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int] = mapped_column(Integer)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
