from flask import Flask, render_template, request, jsonify, send_file, Response
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor, as_completed
import redis
import csv
import io
import time
import re
import json
import threading
import uuid
import os

app = Flask(__name__)

# ─── Redis connection ─────────────────────────────────────────────────────────

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
r = redis.from_url(REDIS_URL, decode_responses=True)

MAX_WORKERS = 5
JOB_TTL = 60 * 60 * 24  # 24h — jobs auto-expire from Redis


# ─── Redis key helpers ────────────────────────────────────────────────────────
# job:{id}:meta       — Hash  { status, total, completed, result_count }
# job:{id}:results    — List  [ JSON strings ]
# job:{id}:seen       — Set   { place_ids }
# job:{id}:errors     — List  [ error strings ]

def job_meta_key(job_id):   return f"job:{job_id}:meta"
def job_results_key(job_id): return f"job:{job_id}:results"
def job_seen_key(job_id):   return f"job:{job_id}:seen"
def job_errors_key(job_id): return f"job:{job_id}:errors"


def init_job(job_id, total_keywords):
    pipe = r.pipeline()
    pipe.hset(job_meta_key(job_id), mapping={
        "status": "queued",
        "total": total_keywords,
        "completed": 0,
        "result_count": 0,
    })
    pipe.expire(job_meta_key(job_id), JOB_TTL)
    pipe.expire(job_results_key(job_id), JOB_TTL)
    pipe.expire(job_seen_key(job_id), JOB_TTL)
    pipe.expire(job_errors_key(job_id), JOB_TTL)
    pipe.execute()


def get_job_meta(job_id):
    return r.hgetall(job_meta_key(job_id))


def set_job_status(job_id, status):
    r.hset(job_meta_key(job_id), "status", status)


def increment_completed(job_id):
    r.hincrby(job_meta_key(job_id), "completed", 1)


def push_result(job_id, result: dict):
    """
    Atomically deduplicate via Redis SADD on the seen set.
    SADD returns 1 if the member was new, 0 if already existed.
    This is atomic — no race condition between threads.
    """
    place_id = result.get("place_id", "")
    if place_id:
        added = r.sadd(job_seen_key(job_id), place_id)
        if added == 0:
            return False  # duplicate — skip

    pipe = r.pipeline()
    pipe.rpush(job_results_key(job_id), json.dumps(result))
    pipe.hincrby(job_meta_key(job_id), "result_count", 1)
    pipe.execute()
    return True


def get_all_results(job_id):
    raw = r.lrange(job_results_key(job_id), 0, -1)
    return [json.loads(x) for x in raw]


def push_error(job_id, msg):
    r.rpush(job_errors_key(job_id), msg)
    r.expire(job_errors_key(job_id), JOB_TTL)


# ─── Scraper core ─────────────────────────────────────────────────────────────

def extract_place_id(url):
    match = re.search(r'place/[^/]+/([^/?]+)', url or "")
    if match:
        return match.group(1)
    match = re.search(r'!1s([^!]+)', url or "")
    if match:
        return match.group(1)
    return None


def scrape_keyword(keyword, max_results, proxy, job_id):
    launch_options = {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ]
    }
    if proxy:
        launch_options["proxy"] = {"server": proxy}

    found = 0
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(**launch_options)
            context = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="en-US",
            )
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

            url = f"https://www.google.com/maps/search/{keyword.replace(' ', '+')}/"
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

            # Accept cookies if prompted
            try:
                btn = page.locator('button:has-text("Accept all"), button:has-text("Accept")').first
                if btn.is_visible(timeout=2000):
                    btn.click()
                    time.sleep(1)
            except Exception:
                pass

            # Scroll to load more results
            results_panel = page.locator('div[role="feed"]').first
            last_count = 0
            scroll_attempts = 0
            while scroll_attempts < 10:
                cards = page.locator('div[role="feed"] > div > div > a').all()
                if len(cards) >= max_results:
                    break
                if len(cards) == last_count:
                    scroll_attempts += 1
                    if scroll_attempts >= 4:
                        break
                else:
                    scroll_attempts = 0
                    last_count = len(cards)
                try:
                    results_panel.evaluate("el => el.scrollBy(0, 800)")
                except Exception:
                    page.keyboard.press("End")
                time.sleep(1.2)

            cards = page.locator('div[role="feed"] > div > div > a').all()[:max_results]

            for card in cards:
                try:
                    card.click()
                    time.sleep(1.8)

                    name = address = phone = website = rating = reviews = place_id = ""

                    current_url = page.url
                    place_id = extract_place_id(current_url) or current_url

                    try:
                        name = page.locator('h1.DUwDvf, h1[class*="fontHeadlineLarge"]').first.inner_text(timeout=2500)
                    except Exception:
                        pass

                    try:
                        rating = page.locator('div.F7nice span[aria-hidden="true"]').first.inner_text(timeout=1500)
                    except Exception:
                        pass

                    try:
                        rt = page.locator('div.F7nice span[aria-label*="review"]').first.get_attribute("aria-label", timeout=1500)
                        m = re.search(r'[\d,]+', rt or "")
                        reviews = m.group(0) if m else ""
                    except Exception:
                        pass

                    try:
                        addr_els = page.locator('button[data-item-id="address"] div.Io6YTe').all()
                        if addr_els:
                            address = addr_els[0].inner_text(timeout=1500)
                    except Exception:
                        pass

                    try:
                        ph = page.locator('button[data-item-id*="phone"] div.Io6YTe').all()
                        if ph:
                            phone = ph[0].inner_text(timeout=1500)
                    except Exception:
                        pass

                    try:
                        website = page.locator('a[data-item-id="authority"]').first.get_attribute("href", timeout=1500) or ""
                    except Exception:
                        pass

                    if name:
                        result = {
                            "name": name.strip(),
                            "keyword": keyword,
                            "address": address.strip(),
                            "phone": phone.strip(),
                            "website": website.strip(),
                            "rating": rating.strip(),
                            "reviews": reviews.strip(),
                            "place_id": place_id,
                        }
                        if push_result(job_id, result):
                            found += 1

                except Exception:
                    pass

            browser.close()

    except Exception as e:
        push_error(job_id, f"[{keyword}] {str(e)}")

    return found


# ─── Job runner ───────────────────────────────────────────────────────────────

def run_job(job_id, keywords, max_results_per_keyword, proxy, num_workers):
    set_job_status(job_id, "running")

    def scrape_one(kw):
        count = scrape_keyword(kw, max_results_per_keyword, proxy, job_id)
        increment_completed(job_id)
        return kw, count

    with ThreadPoolExecutor(max_workers=min(num_workers, MAX_WORKERS)) as executor:
        futures = {executor.submit(scrape_one, kw): kw for kw in keywords}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                push_error(job_id, str(e))

    set_job_status(job_id, "done")


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start():
    data = request.json
    keywords_raw = data.get("keywords", "")
    keywords = [k.strip() for k in keywords_raw.strip().splitlines() if k.strip()]
    if not keywords:
        return jsonify({"error": "At least one keyword is required"}), 400

    max_results = int(data.get("max_results", 20))
    proxy = data.get("proxy", "").strip() or None
    num_workers = min(int(data.get("num_workers", 3)), MAX_WORKERS)

    job_id = str(uuid.uuid4())
    init_job(job_id, len(keywords))

    thread = threading.Thread(
        target=run_job,
        args=(job_id, keywords, max_results, proxy, num_workers),
        daemon=True
    )
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
    meta = get_job_meta(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404
    results = get_all_results(job_id)
    errors = r.lrange(job_errors_key(job_id), 0, -1)
    return jsonify({
        "status": meta.get("status"),
        "result_count": int(meta.get("result_count", 0)),
        "completed_keywords": int(meta.get("completed", 0)),
        "total": int(meta.get("total", 0)),
        "errors": errors,
        "results": results,
    })


@app.route("/stream/<job_id>")
def stream(job_id):
    def generate():
        last_count = -1
        last_completed = -1
        while True:
            meta = get_job_meta(job_id)
            if not meta:
                yield f"data: {json.dumps({'error': 'not found'})}\n\n"
                break

            count = int(meta.get("result_count", 0))
            completed = int(meta.get("completed", 0))
            job_status = meta.get("status", "")

            if count != last_count or completed != last_completed:
                last_count = count
                last_completed = completed
                # Send last 5 results for live table preview
                all_res = get_all_results(job_id)
                payload = {
                    "status": job_status,
                    "result_count": count,
                    "completed_keywords": completed,
                    "total": int(meta.get("total", 1)),
                    "latest": all_res[-5:],
                }
                yield f"data: {json.dumps(payload)}\n\n"

            if job_status == "done":
                yield f"data: {json.dumps({'status': 'done', 'result_count': count, 'completed_keywords': completed, 'total': int(meta.get('total', 1))})}\n\n"
                break

            time.sleep(0.5)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


@app.route("/export/<job_id>")
def export(job_id):
    meta = get_job_meta(job_id)
    if not meta:
        return jsonify({"error": "Job not found"}), 404

    results = get_all_results(job_id)
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "keyword", "address", "phone", "website", "rating", "reviews", "place_id"])
    writer.writeheader()
    writer.writerows(results)
    output.seek(0)

    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name="maps_results.csv"
    )


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
