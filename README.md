# Scrapper Zuzikus

Dockerized lead discovery stack for rental and tourism businesses, starting with Thailand.

## Stack

- FastAPI for the API and admin UI
- Jinja2 + HTMX for a simple region/status dashboard
- PostgreSQL for normalized storage
- Redis + Dramatiq for background scrape runs
- Self-hosted Overpass for OSM-based business discovery
- `httpx` + BeautifulSoup for polite website crawling and email extraction
- Playwright browser worker for JS-heavy or challenge pages

## What is implemented

- Region seed for Thailand
- Category seed for vehicle rental and tourism niches
- Daily Overpass query cap guard
- Self-hosted Overpass service in Docker Compose
- Overpass business discovery by region/category
- Region/category discovery cache with cooldowns
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
5. Open `http://localhost:<APP_HOST_PORT>`

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
- Overpass is served internally at `OVERPASS_URL` and exposed on `OVERPASS_HOST_PORT`
- The default internal `OVERPASS_URL` uses a fixed container IP to avoid intermittent Docker DNS resolution issues for the `overpass` hostname
- Production defaults keep `APP_RELOAD=0` and `WORKER_PROCESSES=1`, `WORKER_THREADS=1`
- Discovery is cached per `region + category` and only refreshed after `DISCOVERY_COOLDOWN_HOURS`
- Website recrawls are limited by `CRAWL_RECRAWL_HOURS`
- The crawler can bypass `robots.txt` and retry with `verify=False` on certificate errors via `CRAWLER_IGNORE_ROBOTS` and `CRAWLER_INSECURE_SSL_FALLBACK`
- `CRAWLER_PROXY_URL` and `BROWSER_PROXY_URL` let you route static and browser workers through proxies when you add proxy-backed worker pools
- A separate `browser_worker` can retry blocked or JS-heavy sites with Playwright when the static crawler finds no usable contact info
- The browser worker supports an extra Playwright stealth layer plus custom browser fingerprint hardening
- If you scale scraping later, add separate proxy-backed worker pools with distinct egress for website crawling only

Example local `.env` values:

```env
APP_HOST_PORT=8000
APP_RELOAD=0
POSTGRES_DB=scrapperzuzikus
POSTGRES_USER=scrapper
POSTGRES_PASSWORD=use-a-strong-password
POSTGRES_HOST_PORT=5433
DATABASE_URL=postgresql+psycopg://scrapper:use-a-strong-password@db:5432/scrapperzuzikus
REDIS_PASSWORD=use-a-different-strong-password
REDIS_HOST_PORT=6380
REDIS_URL=redis://:use-a-different-strong-password@redis:6379/0
OVERPASS_URL=http://172.30.0.10/api/interpreter
OVERPASS_DAILY_QUERY_CAP=0
OVERPASS_MODE=init
OVERPASS_HOST_PORT=12346
OVERPASS_PLANET_URL=https://download.geofabrik.de/asia/thailand-latest.osm.pbf
OVERPASS_PLANET_PREPROCESS=mv /db/planet.osm.bz2 /db/planet.osm.pbf && osmium cat -o /db/planet.osm.bz2 -f osm.bz2 /db/planet.osm.pbf && rm /db/planet.osm.pbf
OVERPASS_DIFF_URL=https://download.geofabrik.de/asia/thailand-updates/
DISCOVERY_COOLDOWN_HOURS=168
CRAWL_RECRAWL_HOURS=168
REGION_CATALOG_COUNTRIES=TH
CRAWLER_IGNORE_ROBOTS=1
CRAWLER_INSECURE_SSL_FALLBACK=1
CRAWLER_PROXY_URL=
BROWSER_FALLBACK_ENABLED=1
BROWSER_MAX_PAGES_PER_SITE=6
BROWSER_NAVIGATION_TIMEOUT_SECONDS=30
BROWSER_WAIT_AFTER_LOAD_MS=2500
BROWSER_RETRY_ATTEMPTS=2
BROWSER_STEALTH_SCROLL_STEPS=3
BROWSER_PROXY_URL=
BROWSER_PROXY_BYPASS=
BROWSER_STEALTH_PLUGIN_ENABLED=1
WORKER_PROCESSES=1
WORKER_THREADS=1
CRAWL_WORKER_PROCESSES=1
CRAWL_WORKER_THREADS=1
BROWSER_WORKER_PROCESSES=1
BROWSER_WORKER_THREADS=1
```

## Discovery Model

- One run per region can be active at a time
- Discovery and website crawling run on separate Dramatiq queues and separate worker services
- Browser-assisted crawling runs on its own queue and only handles sites that the static crawler could not recover
- Each browser worker instance can be given its own proxy by overriding `BROWSER_PROXY_URL`, which is the intended scaling path for proxy-backed worker pools
- Discovery is cached per `region + category` and reused until the cooldown expires
- Repeated runs focus on stale or failed company crawls instead of querying Overpass again
- Self-hosted Overpass removes dependence on the shared public endpoint for normal operation

## Overpass Bootstrap

- First startup can take time because the Overpass container needs to import the Thailand extract before it can answer queries
- Keep `OVERPASS_MODE=init` for the first bootstrap; after the database is initialized you can leave it as-is unless you intentionally rebuild the Overpass volume
- If you replace the Overpass volume, the import process starts from scratch again
- Geofabrik region extracts are downloaded by this image as `/db/planet.osm.bz2`, even when the source is a `.pbf`, so the preprocess step must rename that downloaded file to `.pbf` before converting it back to `osm.bz2`
- The Compose service now uses a small wrapper entrypoint that runs `chmod og+rx /db` before delegating to `/app/docker-entrypoint.sh`, so the Overpass CGI process can reach the runtime socket in the mounted volume without breaking the container lifecycle.

## Notes

- This is the first scaffold. It uses `Base.metadata.create_all()` instead of migrations.
- The form-submission worker is not implemented yet; the form schema is stored for later reuse.
- Search-engine discovery and directory imports are not implemented yet, but the category search terms and source metadata fields are ready for that next step.
