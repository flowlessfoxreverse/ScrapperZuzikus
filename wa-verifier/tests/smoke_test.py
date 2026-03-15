#!/usr/bin/env python3
"""
Smoke test — verifies the full stack is working end to end.
Run after `make up` and after scanning the WhatsApp QR.

Usage:
    python tests/smoke_test.py
    python tests/smoke_test.py --api http://your-server:8000
"""

import argparse
import json
import time
import sys
import urllib.request
import urllib.error

API = "http://localhost:8000"


def get(path):
    req = urllib.request.Request(f"{API}{path}")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def post(path, data):
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        f"{API}{path}", data=body,
        headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def check(label, condition, detail=""):
    icon = "✅" if condition else "❌"
    print(f"  {icon} {label}" + (f" — {detail}" if detail else ""))
    return condition


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--phone", default=None, help="A real phone number to test with")
    args = parser.parse_args()

    global API
    API = args.api.rstrip("/")

    print(f"\n{'='*50}")
    print(f"WA Verifier Smoke Test → {API}")
    print(f"{'='*50}\n")

    failures = 0

    # 1. Health check
    print("1. Health check")
    try:
        h = get("/api/health")
        if not check("API reachable", h.get("api") == "healthy"):
            failures += 1
        wa = h.get("whatsapp", {})
        wa_ok = wa.get("status") in ("healthy", "connected")
        if not check("WhatsApp connected", wa_ok, wa.get("connection", "unknown")):
            print("     → Scan the QR code first: make qr")
            failures += 1
    except Exception as e:
        check("API reachable", False, str(e))
        print("  Cannot reach API. Is it running? Try: make up")
        sys.exit(1)

    # 2. Create a job
    print("\n2. Create verification job")
    test_phones = ["+15005550001", "+15005550002"]
    if args.phone:
        test_phones.append(args.phone)

    try:
        job = post("/api/jobs/", {"name": "smoke-test", "phones": test_phones})
        job_id = job["id"]
        check("Job created", bool(job_id), f"id={job_id}")
        check("Correct total", job["total_numbers"] == len(test_phones),
              f"{job['total_numbers']} numbers")
    except Exception as e:
        check("Job created", False, str(e))
        failures += 1
        sys.exit(1)

    # 3. Poll until complete (max 60s)
    print("\n3. Waiting for job to complete...")
    for i in range(60):
        time.sleep(1)
        j = get(f"/api/jobs/{job_id}")
        pct = j["progress_pct"]
        status = j["status"]
        print(f"   [{i+1:02d}s] status={status} progress={pct}%", end="\r")
        if status in ("completed", "failed", "cancelled"):
            print()
            break

    check("Job completed", j["status"] == "completed", f"status={j['status']}")
    check("All processed", j["processed_count"] == j["total_numbers"],
          f"{j['processed_count']}/{j['total_numbers']}")

    # 4. Fetch results
    print("\n4. Fetch results")
    results = get(f"/api/jobs/{job_id}/results")
    check("Results returned", len(results["results"]) > 0,
          f"{len(results['results'])} numbers")

    for r in results["results"]:
        print(f"   {r['phone']} → {r['status']}" +
              (f" ({r['whatsapp_jid']})" if r.get('whatsapp_jid') else ""))

    # 5. Export CSV
    print("\n5. Export CSV")
    try:
        req = urllib.request.Request(f"{API}/api/jobs/{job_id}/export")
        with urllib.request.urlopen(req, timeout=10) as r:
            csv_content = r.read().decode()
        lines = [l for l in csv_content.strip().split("\n") if l]
        check("CSV export works", len(lines) > 1, f"{len(lines)-1} data rows")
    except Exception as e:
        check("CSV export works", False, str(e))
        failures += 1

    # Summary
    print(f"\n{'='*50}")
    if failures == 0:
        print("✅ All checks passed! Stack is working correctly.")
    else:
        print(f"❌ {failures} check(s) failed. See details above.")
    print(f"{'='*50}\n")
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
