# Microservice Guardrails

Short alignment document for all developers working in this monorepo.

## Goal

Keep the main scraper app as the canonical lead platform while letting specialized
services evolve independently.

Current services in the repo:
- main scraper app
- `emailvalidator`
- `maps_scraper_v3`
- `wa-verifier`

## Core rule

The **main scraper app owns the canonical business data model**.

Canonical entities stay in the main app:
- regions
- categories / recipes
- companies
- emails
- phones
- contact channels
- forms
- scrape runs
- request metrics

Other services are specialized workers or feeders.

## Service roles

### Main scraper app

Responsibilities:
- discovery orchestration
- Overpass discovery
- future Google Maps bridge intake
- company dedupe/canonical merge
- website crawling
- browser fallback
- recipe planning and evaluation
- final lead/contact persistence

### maps_scraper_v3

Responsibilities:
- external Google Maps discovery only
- emits raw place-style results

Should not:
- own canonical companies
- write directly into the main `companies` table

Integration pattern:
- send batches into main app `source_jobs` / `source_records`
- let the main app perform dedupe + materialization

### emailvalidator

Responsibilities:
- email verification logic
- SMTP / catch-all probing
- validator-local cache and job queue

Should not:
- own canonical emails
- write directly into scraper tables by shared ORM

Integration pattern:
- main app sends email IDs + values
- emailvalidator returns validation results
- main app writes final status to its own `emails` table

### wa-verifier

Responsibilities:
- phone/WhatsApp verification logic
- channel-specific operational checks

Should not:
- become the canonical owner of phones or contact channels

Integration pattern:
- main app sends phone/channel identifiers
- wa-verifier returns verification outcome
- main app updates `phones` / `contact_channels` metadata

## Database policy

Preferred pattern:
- **separate PostgreSQL database per service**

Why:
- independent scaling
- independent migrations
- failure isolation
- smaller blast radius
- easier service ownership

Exception:
- the main scraper app may contain bridge/staging tables for external discovery
  sources, because canonical merge must still happen in one place

## Integration policy

Services should connect through:
- HTTP APIs
- queues
- callbacks/webhooks

Avoid:
- cross-service ORM imports
- cross-service direct table writes into another service DB
- sharing canonical model classes across services

## Write-back policy

Specialized services may compute results, but the main app should persist final
canonical state.

Examples:
- email validation result -> main app updates `emails.validation_status`
- WhatsApp verification result -> main app updates `contact_channels`
- Google Maps discovery result -> main app stores `source_records`, then merges to `companies`

## Staging / bridge pattern

For external discovery sources, use:
- `source_jobs`
- `source_job_queries`
- `source_records`
- `company_sources`

Pattern:
1. external service emits raw results
2. main app ingests raw results into staging
3. main app dedupes/materializes canonical companies
4. shared crawl/browser/contact pipeline continues

## Ownership checklist for new services

Before adding a new microservice, answer:
1. Does it own canonical business data?
   - If yes, reconsider. Usually the answer should be no.
2. Can it operate through API/queue boundaries?
   - Prefer yes.
3. Does it need its own DB?
   - Usually yes.
4. What exact payload does it receive from the main app?
5. What exact result does it send back?
6. Which table in the main app is updated as the final write-back?

## Practical dev rule

When integrating a new service:
- document its contract in that service’s README
- document repo-level boundaries here
- keep the main app pipeline unchanged downstream whenever possible

That is the preferred architecture for scaling this repo without turning it into one tightly coupled distributed mess.
