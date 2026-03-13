# Scrapper Zuzikus

Dockerized lead discovery stack for rental and tourism businesses, starting with Thailand.

## Stack

- FastAPI for the API and admin UI
- Jinja2 + HTMX for a simple region/status dashboard
- PostgreSQL for normalized storage
- Redis + Dramatiq for background scrape runs
- Overpass API for OSM-based business discovery
- `httpx` + BeautifulSoup for polite website crawling and email extraction

## What is implemented

- Region seed for Thailand
- Category seed for vehicle rental and tourism niches
- Daily Overpass query cap guard
- Overpass business discovery by region/category
- Website crawl for emails, social links, and contact form hints
- Email dedupe per company
- Admin pages for:
  - region-level run trigger
  - category editor
  - email list with editable validation status

## Start

1. Copy `.env.example` to `.env`
2. Run `docker compose up --build`
3. Open `http://localhost:8000`

## Notes

- This is the first scaffold. It uses `Base.metadata.create_all()` instead of migrations.
- The form-submission worker is not implemented yet; the form schema is stored for later reuse.
- Search-engine discovery and directory imports are not implemented yet, but the category search terms and source metadata fields are ready for that next step.
