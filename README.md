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
2. Set strong values for `POSTGRES_PASSWORD` and `REDIS_PASSWORD`
3. If host port `5432` is already in use, keep `POSTGRES_HOST_PORT=5433` or choose another free host port
4. Run `docker compose up --build`
5. Open `http://localhost:8000`

Later, to pull updates on that server:

cd ScrapperZuzikus
git pull origin main
docker compose up --build -d


## Ports and auth

- The app listens on container port `8000` and maps to `APP_HOST_PORT`
- PostgreSQL listens on container port `5432` and maps to `POSTGRES_HOST_PORT`
- Redis listens on container port `6379` and maps to `REDIS_HOST_PORT`
- The app connects to Postgres using `DATABASE_URL`
- The app connects to Redis using `REDIS_URL`
- Redis is password-protected with `REDIS_PASSWORD`

Example local `.env` values:

```env
APP_HOST_PORT=8000
POSTGRES_DB=scrapperzuzikus
POSTGRES_USER=scrapper
POSTGRES_PASSWORD=use-a-strong-password
POSTGRES_HOST_PORT=5433
DATABASE_URL=postgresql+psycopg://scrapper:use-a-strong-password@db:5432/scrapperzuzikus
REDIS_PASSWORD=use-a-different-strong-password
REDIS_HOST_PORT=6380
REDIS_URL=redis://:use-a-different-strong-password@redis:6379/0
```

## Notes

- This is the first scaffold. It uses `Base.metadata.create_all()` instead of migrations.
- The form-submission worker is not implemented yet; the form schema is stored for later reuse.
- Search-engine discovery and directory imports are not implemented yet, but the category search terms and source metadata fields are ready for that next step.
