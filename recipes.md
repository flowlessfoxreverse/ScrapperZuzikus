# Recipes Roadmap

## Goal

Build a shared recipe system for category discovery that gives the platform:

- higher discovery precision
- lower duplicated validation traffic
- reusable templates across all users
- a clear path from AI-assisted draft to production-safe recipe

This file now serves two roles:

- record what is already implemented in the live recipe system
- keep a trimmed roadmap of the next recipe improvements we intentionally defer until after battle testing

## Implemented Functionality

The following recipe functionality is already built into the platform.

### Taxonomy

- database-backed `verticals`
- database-backed `niche_clusters`
- seeded curated internal taxonomy
- recipes and categories linked back to taxonomy instead of relying on a fixed enum only

### Recipe Core

- `query_recipes`
- `query_recipe_versions`
- `query_recipe_validations`
- `query_recipe_variants`
- `query_recipe_variant_templates`
- versioned recipe lifecycle:
  - `draft`
  - `candidate`
  - `validated`
  - `active`
  - `deprecated`

### Prompt Planning

- planner abstraction with persisted planner runs
- heuristic planner provider
- optional OpenAI planner provider behind the same contract
- per-request planner provider/model override in the UI
- compare-against-heuristic mode
- persisted planner outputs and variant outcomes

### Variant Generation

- one prompt -> cluster -> multiple ranked candidate variants
- curated cluster-specific variant libraries
- multi-select bulk creation of draft recipes from generated variants
- persistent generated variant records
- source-plan and source-variant attribution from prompt generation through recipe creation

### Ranking Signals

- template score
- prompt match score
- validation score
- cluster baseline validation
- platform adoption
- prompt-specific selection history
- prompt-specific activation history
- planner conversion history
- production scrape outcomes
- market-aware production performance
- market-aware planner conversion
- market-aware prompt-variant history
- market-aware cluster history

### Recommendation Layer

- recommendation states:
  - `experimental`
  - `recommended`
  - `trusted`
  - `suppressed`
- recommendation policies with editable thresholds
- per-policy audit trail
- policy simulation
- apply-suggested-policy flow
- policy experiment outcome classification
- policy learning bias from past experiment outcomes
- market-aware policy simulation
- policy impact snapshots and 7-day before/after windows

### Validation and Safety

- recipe linting
- public Overpass sampled validation with platform-wide cache
- validation quota tracking
- activation gates by source strategy
- planner fallback to heuristic when provider output or credentials fail

### Analytics and Transparency

- recipe analytics for:
  - cluster
  - source strategy
  - market
  - strategy by market
  - top variants
- planner comparison summary
- row-level variant diff between selected planner and heuristic
- planner conversion summary
- cluster decision explanation
- alternate cluster display
- recommendation policy impact and experiment scoreboard
- policy blockers and applied-policy display on variants

### Benchmark / Battle-Test Infrastructure

- prompt benchmark dataset storage
- admin eval page
- persisted benchmark planner runs
- manual per-prompt scoring workflow
- aggregate planner comparison summary by provider/model

## Deferred Improvements We Are Holding For Now

These are reasonable future recipe improvements, but they are intentionally deferred until battle testing proves they are worth the added complexity.

- richer curated sub-intent libraries for every cluster
- stronger prompt normalization and semantic similarity matching
- source-strategy-specific validation rules beyond the current activation gate layer
- automatic policy tuning beyond the current recommendation/simulation system
- deeper recipe governance and approval workflows
- more policy-state automation or experiment orchestration
- stronger AI-assisted tag/variant expansion
- recipe performance dashboards beyond the current eval and analytics views

## Battle-Test Recommendation

Before adding more recipe management features, the recommended focus is:

1. build and curate a real benchmark prompt set
2. run heuristic and OpenAI planners side by side
3. score prompts manually for:
   - cluster choice
   - variant usefulness
   - overall quality
4. analyze failures
5. only then return to planner/recipe improvements

## Benchmark Dataset Format

The prompt benchmark dataset should use one row per benchmark prompt with these fields:

- `prompt_text`
- `market_country_code`
- `expected_vertical`
- `expected_cluster_slug`
- `expected_variant_keys`
- `notes`
- `is_active`

Meaning:

- `prompt_text`: the exact user-style prompt to test
- `market_country_code`: optional market hint like `TH`
- `expected_vertical`: the best-fit vertical if known
- `expected_cluster_slug`: the expected cluster if known
- `expected_variant_keys`: comma-separated or array form of the variants we would consider acceptable
- `notes`: why this prompt matters or what makes a good answer
- `is_active`: whether it should participate in active benchmark runs

## Product Direction

Recipes are platform assets, not private one-off tag blobs.

Core idea:

- users describe a business niche in plain language
- AI proposes a draft recipe set
- the platform validates it on sampled locations
- validated recipes become reusable templates available to all users
- one business intent can expand into multiple ranked recipe variations

This avoids repeated work and reduces duplicated Overpass validation requests across the SaaS.

## Taxonomy Direction

The long-term system should not use a hardcoded enum like:

- `vehicle`
- `tourism`

Instead, the platform should use a database-backed taxonomy.

Planned taxonomy layers:

- `verticals`
- `niche_clusters`
- `recipe_variants`

Example:

- user prompt: `complete beauty experience in Thailand`
- vertical: `beauty`
- cluster: `beauty_services`
- candidate variants:
  - `nail-salon`
  - `hair-salon`
  - `barber-shop`
  - `beauty-salon`
  - `spa`
  - `massage-spa`
  - `lash-brow-studio`
  - `waxing-salon`

This is a better SaaS model than generating one recipe per user prompt.

## Category Generation Model

The platform should move from:

- one prompt -> one recipe

to:

- one prompt -> one vertical
- one vertical -> one niche cluster
- one niche cluster -> many candidate recipe variations
- each variation -> validation + ranking

This creates a reusable pool of category templates instead of one-off mappings.

## Ranking Strategy

The system should rank candidate recipe variations on multiple layers.

### 1. Intent Match Ranking

How well does the variation match the user prompt?

Examples:

- `complete beauty experience`
- `beauty services`
- `hair and nails`

This layer ranks the semantic fit of a variation before validation.

### 2. Query Quality Ranking

How strong are the source rules?

Examples:

- OSM tag confidence
- alias overlap
- expected false positives
- search keyword quality
- multilingual coverage

### 3. Validation Yield Ranking

How useful is the variation after sampled validation?

Examples:

- businesses found
- website rate
- phone rate
- email rate
- duplicate ratio
- noise ratio

The platform should combine these into one recommendation score so the best variants surface first.

## Scope Split

Use two different systems for two different jobs.

### 1. Validation

Use small-sample validation against the public Overpass API.

Why:

- fast feedback
- no need to self-host every country before testing a category
- public quota is enough for controlled sample validation
- easier to validate recipe quality globally

Rules:

- validation queries must be small and sampled
- validation results are cached platform-wide for 24h minimum
- identical recipe + location + source combinations must reuse cached validation results

### 2. Production Discovery

Use self-hosted Overpass for supported production regions.

Why:

- predictable throughput
- no dependence on public API for full runs
- better control over quotas and retry behavior

## What A Recipe Is

A recipe is a versioned discovery definition for one business intent.

Example intents:

- car rental agency
- motorcycle rental
- scooter rental
- travel agency
- tour operator
- elephant sanctuary

Each recipe should support multiple source adapters.

Initial adapter:

- OSM / Overpass

Future adapters:

- Google Maps
- directories
- internal imports

In the multi-variant model, a recipe is not always the first artifact produced by the builder.

Builder output should often be:

- `intent`
- `vertical`
- `cluster`
- `candidate recipe variants`

Each candidate can then become its own recipe version if it survives validation.

## Recipe Object Model

Each recipe should contain:

- `slug`
- `label`
- `description`
- `vertical`
- `status`
- `version`
- `created_by`
- `approved_by`
- `is_platform_template`

Recipe content should include:

- `intent_summary`
- `variant_label`
- `variant_group`
- `variant_rank`
- `osm_include_tags`
- `osm_exclude_tags`
- `osm_alternative_queries`
- `website_keywords`
- `negative_keywords`
- `language_hints`
- `expected_signals`
- `source_adapters`

Expected signals examples:

- website likely present
- phone likely present
- email likely present
- contact form likely present
- WhatsApp likely present

## Status Lifecycle

Recipes should move through these states:

- `draft`
- `candidate`
- `validated`
- `active`
- `deprecated`
- `rejected`

Meaning:

- `draft`: initial AI or manual proposal
- `candidate`: syntactically clean and ready for sampled validation
- `validated`: passed score thresholds
- `active`: available for production discovery
- `deprecated`: kept for history, no longer recommended
- `rejected`: known low-quality or noisy recipe

## Validation Pipeline

Validation should be cheap, repeatable, and score-based.

### Stage 1. Lint

Check:

- invalid tag syntax
- duplicated include/exclude rules
- obviously conflicting rules
- empty query set
- suspiciously broad tag definitions

Output:

- pass / fail
- lint messages

### Stage 2. Public Overpass Sample Validation

Run small sampled queries against representative regions.

Rules:

- use public Overpass only for validation
- sample only a few locations per recipe
- never run full-country validation against public Overpass
- cache by recipe fingerprint + sample region + source + day

Sample output:

- raw match count
- website coverage
- phone coverage
- email coverage
- duplicate ratio
- noise ratio

### Stage 3. Yield Scoring

Compute validation scores such as:

- `coverage_score`
- `precision_score`
- `contact_score`
- `website_score`
- `duplicate_penalty`
- `noise_penalty`
- `overall_score`

### Stage 4. Activation Gate

Only recipes above threshold become `validated` or `active`.

## Shared Platform Caching

This is critical for SaaS efficiency.

Validation cache should be shared across all users.

Cache key should include:

- recipe fingerprint
- source adapter
- sampled location
- validation mode
- date bucket

Default cache TTL:

- 24 hours

Do not recompute validation when:

- the same recipe version already ran recently
- the same sampled location set was used
- only the requesting user changed

This is where the ROI comes from:

- one recipe validation benefits all users
- less duplicated public Overpass traffic
- faster UX for repeated categories

## AI Builder Role

AI should assist, not directly activate recipes.

AI responsibilities:

- convert plain-language niche descriptions into candidate recipe drafts
- suggest include tags
- suggest exclude tags
- suggest alternative query shapes
- suggest website keywords
- suggest language hints
- suggest likely false positives

AI should not:

- directly activate recipes
- bypass validation
- overwrite existing validated templates without review/versioning

## Shared Template Strategy

Platform templates should be reusable.

Examples:

- `car-rental-agency`
- `travel-agency`
- `tour-guide-service`

Users may:

- use a platform template as-is
- fork a template
- create a workspace-specific variant

But the platform should prefer:

- one canonical validated recipe per broad business intent
- multiple validated variants within that intent when they represent distinct discoverable niches

This prevents duplicated drift and inconsistent mappings like:

- multiple categories sharing the same bad OSM tag

Example:

- broad intent: `beauty services`
- canonical variant set:
  - hair salon
  - nail salon
  - beauty salon
  - spa
  - massage spa

This is better than forcing all of them into one generic beauty recipe.

## Problem We Are Explicitly Solving

Current example:

- `scooter-rental-service`
- `quad-rental`

Both effectively mapped to the same `shop=motorcycle_rental` behavior, creating warnings and noisy results.

The recipe system should prevent this by:

- showing overlapping mappings before activation
- scoring recipes separately
- flagging alias collisions
- forcing a reviewed template instead of silent duplicated logic

## Initial Data Model

Planned tables:

- `verticals`
- `niche_clusters`
- `query_recipes`
- `query_recipe_versions`
- `query_recipe_validations`
- `query_recipe_validation_samples`
- `query_recipe_scores`
- `query_recipe_templates`
- `query_recipe_adapters`

Useful supporting fields:

- `fingerprint`
- `status`
- `source_adapter`
- `validation_cache_key`
- `overall_score`
- `lint_passed`
- `activated_at`
- `cluster_slug`
- `variant_rank`
- `parent_recipe_id`

### Planned Supporting Objects

#### `verticals`

Database-backed top-level business buckets.

Examples:

- beauty
- tourism
- vehicle
- health
- food
- fitness
- real_estate
- education
- legal
- retail

#### `niche_clusters`

Curated platform groupings inside a vertical.

Examples:

- beauty_services
- beauty_clinics
- diving_services
- tour_operators
- vehicle_rentals

#### `recipe_variants`

Generated or curated niche slices within a cluster.

Examples:

- hair-salon
- nail-salon
- barber-shop
- spa
- waxing-salon

## UI Plan

### 1. Recipes List

Show:

- label
- vertical
- status
- version
- score
- last validation time
- whether it is platform-shared

### 2. Builder

Input:

- plain-language business niche

Output:

- detected vertical
- detected niche cluster
- ranked candidate recipe variations
- AI-generated draft variant rules
- editable include/exclude rules
- validation button per variant

### 2b. Variant Explorer

The builder should show a variation pool instead of only one draft.

Each variation row should include:

- label
- rank
- rationale
- tags
- keyword hints
- validation status
- activate button

### 3. Validation Report

Show:

- lint issues
- sample regions used
- result counts
- website/email/phone rates
- duplicate/noise scores
- final recommendation
- comparison across sibling variants in the same cluster

### 4. Activation Controls

Allow:

- save as draft
- validate
- activate
- deprecate
- fork template

## Multi-Source Future

Recipes should not be locked to OSM forever.

The same business intent should later support:

- OSM rules
- Google Maps heuristics
- directory matchers
- site keyword classifiers

So recipe content should be adapter-based, not hardcoded only to OSM tags.

## Metrics To Track

Per recipe:

- validation requests
- cache hits
- sample result count
- production company yield
- website rate
- email rate
- phone rate
- WhatsApp / Telegram rate
- duplicate rate
- user adoption count

Per template:

- how many users used it
- how many production runs used it
- contact yield quality over time

## Anti-Abuse / Cost Controls

Because validation will use public Overpass:

- per-day public validation cap
- per-user validation cap
- global cache-first policy
- sample-region limits
- no full-country validation on public Overpass

## Recommended Implementation Order

### Phase 1

- create recipe/version schema
- add shared validation cache model
- add basic recipes UI list

### Phase 2

- build AI-assisted draft generator
- add lint stage
- add public Overpass sample validator

### Phase 3

- add validation scorecard
- add activate/deprecate workflow
- convert current hardcoded categories into recipe-backed templates

### Phase 4

- add template fork/version review flow
- add adapter abstraction for future Google Maps / directory connectors

### Phase 5

- replace fixed vertical enum with database-backed `verticals`
- add `niche_clusters`
- change prompt builder to produce multiple candidate variants
- add ranking across variants
- let users activate one or many validated variants from the same prompt

### Phase 6

- add curated vertical and cluster seed library
- add multilingual keyword packs per vertical/cluster
- add platform-level variant dedupe and alias detection
- track adoption and yield by variant family

## Success Criteria

We should consider the recipe foundation successful when:

- category quality is no longer hardcoded ad hoc
- duplicated user validation requests are heavily reduced
- new business niches can be added without editing seed code manually
- noisy mappings are caught before full scrape runs
- validated templates become reusable assets across the whole platform
- one user prompt can produce multiple high-quality validated category variations
- vertical and cluster expansion no longer requires code enums for every new domain
