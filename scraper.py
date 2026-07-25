#!/usr/bin/env python3
"""
Magnolia Storage - Competitor Price Scraper (v2)
Runs daily via GitHub Actions to update data.json with current competitor pricing.
Uses only Python stdlib (no pip dependencies needed).

v2 fixes (July 2026 audit):
1. CARD SEGMENTATION: prices are matched only within their own unit card
   (text between one dimension and the next), never across card boundaries.
   The old fixed-width regex windows let a size grab its neighbor's price.
2. PARKING FILTER: Public Storage listings for uncovered RV/boat/vehicle
   parking are excluded. Previously a 10x30 uncovered parking space ($52)
   was recorded as an enclosed 10x30 unit.
3. MAPPING FIXES: PS 8x14 now maps to 10x10 equivalent.
4. HONEST FRESHNESS: each competitor now carries scrapeStatus
   ("ok" | "blocked" | "failed") and lastVerified (only updated on a
   successful scrape). A daily run that gets blocked no longer masquerades
   as fresh data. The dashboard reads these fields to show stale badges.
"""

import json
import re
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.parse import quote

# Some competitor sites block datacenter/CI IPs at the network level (TCP reset),
# which no header or headless browser can beat. For those hosts only, route the
# request through a residential scraping API when a key is configured. The key is
# read from the SCRAPER_API_KEY env var (a GitHub Actions secret), never hardcoded.
SCRAPER_API_KEY = os.environ.get("SCRAPER_API_KEY", "").strip()
PROXY_HOSTS = ("montgomeryss.com",)


def _via_scraper_api(url):
    """
    Wrap a target URL in a ScraperAPI request. `premium=true` uses residential
    IPs — required for hosts (like Montgomery) that reset connections from
    datacenter IPs, which ScraperAPI's default proxy pool also uses.
    """
    return ("https://api.scraperapi.com/?api_key=" + SCRAPER_API_KEY
            + "&premium=true&country_code=us&render=true"
            + "&url=" + quote(url, safe=""))

SIZES = ["5x10", "10x10", "10x15", "10x20", "10x30"]

# Text that marks a listing as vehicle parking, not an enclosed unit.
# "Uncovered" marks true parking spaces. Enclosed drive-up units that allow
# vehicle parking inside are legitimate units and must NOT be excluded.
PARKING_RE = re.compile(r"uncovered|parking\s*space\s*only", re.I)

# --- Helpers -----------------------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")


def ensure_playwright():
    """
    Make headless Chromium available in the CI runner without editing the
    workflow: install the playwright package if missing, then fetch the
    Chromium binary. Best-effort — if anything fails, fetch() falls back to a
    plain HTTP GET, so the scraper still runs (just without JS rendering).
    """
    import subprocess
    try:
        import playwright  # noqa: F401
    except Exception:
        print("Bootstrapping Playwright (pip install)...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "playwright"],
                           check=True, capture_output=True)
        except Exception as e:
            print(f"  NOTE: pip install playwright failed: {e}")
            return False
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                       check=False, capture_output=True)
    except Exception as e:
        print(f"  NOTE: chromium install skipped: {e}")
    return True


def _fetch_static(url, timeout=30):
    """Plain HTTP GET (no JavaScript). Fallback when Playwright is unavailable."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  WARNING: static fetch failed for {url}: {e}")
        return None


def _reveal_units(page):
    """
    Some sites (e.g. SmartStop) hide unit pricing behind 'Show ... Units' /
    'View all' toggles that only populate the DOM once clicked. Click any such
    controls so the prices render into the HTML we parse. Best-effort; never
    raises.
    """
    try:
        toggles = page.get_by_text(
            re.compile(r"show\s+.*units|view\s+all\s+units|see\s+prices|show\s+units", re.I)
        )
        for i in range(min(toggles.count(), 12)):
            try:
                toggles.nth(i).click(timeout=2500)
                page.wait_for_timeout(400)
            except Exception:
                pass
    except Exception:
        pass


def fetch(url, timeout=45):
    """
    Fetch fully-rendered HTML using headless Chromium so JavaScript-rendered
    prices (SmartStop, Public Storage, Lockaway, etc.) actually appear.
    Falls back to a plain HTTP GET if Playwright isn't installed.
    """
    # Hosts that block CI IPs: route through the residential scraping API (if a
    # key is set). Their prices are server-rendered, so no JS render is needed.
    if SCRAPER_API_KEY and any(h in url for h in PROXY_HOSTS):
        print("  routing via ScraperAPI (residential IP + JS render)")
        html = _fetch_static(_via_scraper_api(url), timeout=120)
        if html:
            return html
        print("  NOTE: ScraperAPI fetch empty; falling back to direct")

    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        print("  NOTE: Playwright unavailable, using static fetch")
        return _fetch_static(url, timeout)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
            page = browser.new_page(user_agent=USER_AGENT)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass  # networkidle can time out on chatty pages; keep going
                _reveal_units(page)
                page.wait_for_timeout(2000)  # let late price widgets settle
                html = page.content()
            finally:
                browser.close()
            return html
    except Exception as e:
        print(f"  WARNING: Playwright fetch failed for {url}: {e}; trying static")
        return _fetch_static(url, timeout)


def strip_tags(html):
    """Crude HTML-to-text so card segmentation follows what a human sees."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"&#0?39;|&apos;|&rsquo;", "'", text)
    text = re.sub(r"&amp;", "&", text)
    return re.sub(r"\s+", " ", text)


DIM_RE = re.compile(r"(\d{1,2})\s*'?\s*[xX\u00d7]\s*(\d{1,2})\s*'?")


def segment_cards(html):
    """
    Split page text into per-unit 'cards'. Each card runs from one dimension
    occurrence to the next, so prices can never bleed between cards.
    Returns a list of (norm_size_key, card_text, prefix_text).
    """
    text = strip_tags(html)
    hits = [(m.start(), int(m.group(1)), int(m.group(2))) for m in DIM_RE.finditer(text)]
    cards = []
    for i, (pos, a, b) in enumerate(hits):
        end = hits[i + 1][0] if i + 1 < len(hits) else min(len(text), pos + 400)
        prev_end = hits[i - 1][0] if i > 0 else 0
        prefix = text[max(prev_end, pos - 200):pos]
        lo, hi = sorted((a, b))
        cards.append((f"{lo}x{hi}", text[pos:end], prefix))
    return cards


def card_prices(card_text):
    """All whole-dollar prices in a card, low to high."""
    vals = [round(float(p)) for p in re.findall(r"\$\s*(\d+(?:\.\d{1,2})?)", card_text)]
    return sorted(v for v in vals if v > 1)  # ignore the $1 promo figure


def empty_pricing():
    return {s: None for s in SIZES}


def keep_lowest(size_prices, key, val):
    if key not in size_prices or val < size_prices[key]:
        size_prices[key] = val


# --- Scrapers ----------------------------------------------------------------
# Each scraper returns (pricing_dict, status) where status is "ok"|"blocked"|"failed".

def scrape_lockaway(url):
    """Lockaway: multiple cards per size; use lowest advertised (online/starting)."""
    html = fetch(url)
    if html is None:
        return None, "failed"
    if "$" not in html:
        return None, "blocked"

    lockaway_map = {
        "5x10": "5x10", "8x10": "5x10",
        "10x10": "10x10",
        "10x15": "10x15", "8x15": "10x15",
        "10x20": "10x20", "8x20": "10x20",
        "10x30": "10x30", "12x30": "10x30",
    }
    size_prices = {}
    size_full = {}
    for key, card, prefix in segment_cards(html):
        mapped = lockaway_map.get(key)
        if not mapped:
            continue
        prices = card_prices(card)
        if prices:
            promo, regular = prices[0], prices[-1]
            if mapped not in size_prices or promo < size_prices[mapped]:
                size_prices[mapped] = promo
                size_full[mapped] = {"regular": regular, "promo": promo}

    pricing = empty_pricing()
    pricing.update({s: p for s, p in size_prices.items() if s in pricing})
    return {"pricing": pricing, "pricingFull": size_full}, "ok"


def scrape_public_storage(url, facility_name):
    """
    Public Storage: full unit cards carry a Features list and both an
    online-only rate and an in-store rate. Use the online rate per size.
    EXCLUDE uncovered parking listings.
    """
    html = fetch(url)
    if html is None:
        return None, "failed"
    if "$" not in html:
        return None, "blocked"

    ps_map = {
        "5x9": "5x10", "5x10": "5x10", "5x14": "5x10", "5x15": "5x10",
        "7x14": "10x10", "8x14": "10x10", "10x10": "10x10",
        "10x15": "10x15", "7x19": "10x15",
        "10x19": "10x20", "10x20": "10x20",
        "10x30": "10x30",
    }
    size_prices = {}
    size_full = {}
    for key, card, prefix in segment_cards(html):
        mapped = ps_map.get(key)
        if not mapped:
            continue
        # Only full unit cards carry a "Features" list; the page also renders
        # bare summary rows (dimension + price, no features). Summary rows are
        # skipped because they cannot be checked for the Uncovered/parking flag.
        if "Features" not in card:
            continue
        if PARKING_RE.search(card):
            continue  # "Uncovered" = a parking space, not an enclosed unit
        # Capture both figures: online-only (promo) and in-store (regular).
        mo = re.search(r"Online[\s-]*(?:Only)?\s*[Pp]rice\s*\$\s*(\d+(?:\.\d{1,2})?)", card)
        mi = re.search(r"In\s*Store\s*\$\s*(\d+(?:\.\d{1,2})?)", card, re.I)
        eff = mo or mi
        if eff:
            val = round(float(eff.group(1)))
            if mapped not in size_prices or val < size_prices[mapped]:
                size_prices[mapped] = val
                size_full[mapped] = {
                    "regular": round(float(mi.group(1))) if mi else val,
                    "promo": round(float(mo.group(1))) if mo else None,
                }

    pricing = empty_pricing()
    pricing.update({s: p for s, p in size_prices.items() if s in pricing})
    return {"pricing": pricing, "pricingFull": size_full}, "ok"


def _extract_json_data(html):
    """Pull the `window.JSON_DATA = {...}` object out of SmartStop's HTML via a
    balanced-brace scan (the object is deeply nested, so regex won't do)."""
    m = re.search(r"window\.JSON_DATA\s*=\s*", html)
    if not m:
        return None
    start = html.find("{", m.end())
    if start == -1:
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(html)):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(html[start:i + 1])
                    except Exception:
                        return None
    return None


def scrape_smartstop(url):
    """
    SmartStop is an Umbraco site that ships unit data inside a `window.JSON_DATA`
    blob rather than the rendered DOM. Read locationDetail.location.units[] and
    take the lowest online (web) rate per size. When the location has no online
    units (units == []), it shows a "call for availability" message — report OK
    with no prices instead of pretending old numbers are current.
    Falls back to the legacy 'In-Store $' regex if the blob isn't present.
    """
    html = fetch(url)
    if html is None:
        return None, "failed"

    data = _extract_json_data(html)
    if data is not None:
        loc = (data.get("locationDetail") or {}).get("location") or {}
        units = loc.get("units") or []
        size_prices, size_full = {}, {}
        for u in units:
            if not isinstance(u, dict):
                continue
            w = u.get("width") or u.get("unitWidth")
            l = u.get("length") or u.get("unitLength")
            key = None
            if w and l:
                lo, hi = sorted((int(float(w)), int(float(l))))
                key = f"{lo}x{hi}"
            else:
                nm = str(u.get("size") or u.get("unitTypeName") or u.get("name") or "")
                dm = DIM_RE.search(nm)
                if dm:
                    lo, hi = sorted((int(dm.group(1)), int(dm.group(2))))
                    key = f"{lo}x{hi}"
            if key not in SIZES:
                continue
            web = (u.get("webRate") or u.get("pushRate") or u.get("onlineRate")
                   or u.get("rate") or u.get("price"))
            if web is None:
                continue
            web = round(float(web))
            std = u.get("standardRate") or u.get("streetRate") or u.get("inStoreRate") or web
            if key not in size_prices or web < size_prices[key]:
                size_prices[key] = web
                size_full[key] = {"regular": round(float(std)), "promo": web if web < float(std) else None}
        pricing = empty_pricing()
        pricing.update(size_prices)
        # units==[] is a legitimate "call for availability" state, still a
        # successful read — just no comparable online prices right now.
        return {"pricing": pricing, "pricingFull": size_full}, "ok"

    # Legacy fallback (older markup with visible In-Store prices).
    if "$" not in html:
        return None, "blocked"
    size_prices = {}
    for key, card, prefix in segment_cards(html):
        if key not in SIZES:
            continue
        m = re.search(r"In-?Store\s*\$\s*(\d+(?:\.\d{1,2})?)", card, re.I)
        if m:
            keep_lowest(size_prices, key, round(float(m.group(1))))
    pricing = empty_pricing()
    pricing.update(size_prices)
    return {"pricing": pricing, "pricingFull": {s: {"regular": p, "promo": None} for s, p in size_prices.items()}}, "ok"


def scrape_honea_egypt(url):
    """Honea Egypt: '$82.00/month' near dimensions; reversed dims normalized."""
    html = fetch(url)
    if html is None:
        return None, "failed"
    if "$" not in html:
        return None, "blocked"

    size_prices = {}
    for key, card, prefix in segment_cards(html):
        if key not in SIZES:
            continue
        m = re.search(r"\$\s*(\d+(?:\.\d{1,2})?)\s*/?\s*month", card, re.I)
        if m:
            keep_lowest(size_prices, key, round(float(m.group(1))))

    pricing = empty_pricing()
    pricing.update(size_prices)
    return {"pricing": pricing, "pricingFull": {s: {"regular": p, "promo": None} for s, p in size_prices.items()}}, "ok"


# Montgomery unit prices are formatted "$260.00/mo"; its promo banner uses
# "$165/Month" (no cents, and typographic primes in "10' x 20'" that the
# dimension regex misses, so the promo bled into the last unit's card). Match
# ONLY the "$NN.NN/mo" unit format to exclude the promo.
MONTGOMERY_PRICE_RE = re.compile(r"\$\s*(\d+\.\d{2})\s*/\s*mo\b", re.I)


def scrape_montgomery(url):
    """Montgomery: climate and non-climate; keep cheapest (non-climate) per size."""
    html = fetch(url)
    if html is None:
        return None, "failed"
    if "$" not in html:
        return None, "blocked"

    size_prices = {}
    for key, card, prefix in segment_cards(html):
        if key not in SIZES:
            continue
        prices = sorted(round(float(p)) for p in MONTGOMERY_PRICE_RE.findall(card))
        if prices:
            keep_lowest(size_prices, key, prices[0])

    pricing = empty_pricing()
    pricing.update(size_prices)
    return {"pricing": pricing, "pricingFull": {s: {"regular": p, "promo": None} for s, p in size_prices.items()}}, "ok"


def scrape_woodlands_sao(url):
    """
    Woodlands SAO: each card shows promo and regular prices. Rule: use the
    REGULAR price (highest within the card); across multiple cards of the
    same size, use the cheapest regular.
    """
    html = fetch(url)
    if html is None:
        return None, "failed"
    if "$" not in html:
        return None, "blocked"

    woodlands_map = {
        "10x10": "10x10", "10x12": "10x10",
        "10x20": "10x20",
        "10x30": "10x30", "12x30": "10x30",
    }
    size_prices = {}
    size_full = {}
    for key, card, prefix in segment_cards(html):
        mapped = woodlands_map.get(key)
        if not mapped:
            continue
        prices = card_prices(card)
        if prices:
            regular = prices[-1]  # highest in this card = regular price
            promo = prices[0] if len(prices) > 1 else None
            if mapped not in size_prices or regular < size_prices[mapped]:
                size_prices[mapped] = regular
                size_full[mapped] = {"regular": regular, "promo": promo}

    pricing = empty_pricing()
    pricing.update({s: p for s, p in size_prices.items() if s in pricing})
    return {"pricing": pricing, "pricingFull": size_full}, "ok"


# --- Main --------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Magnolia Storage - Competitor Price Scraper v3 (JS rendering)")
    print(f"Run time: {now_utc()}")
    print("=" * 60)
    ensure_playwright()

    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"lastUpdated": None, "competitors": []}

    scrape_targets = [
        {"name": "Lockaway Storage",
         "url": "https://www.lockaway-storage.com/storage-units/texas/magnolia/lockaway-storage-1488-411002/",
         "scraper": scrape_lockaway},
        {"name": "Public Storage (FM 1488)",
         "url": "https://www.publicstorage.com/self-storage-tx-magnolia/2360.html",
         "scraper": lambda u: scrape_public_storage(u, "Public Storage (FM 1488)")},
        {"name": "Public Storage (FM 2978)",
         "url": "https://www.publicstorage.com/self-storage-tx-the-woodlands/5888.html",
         "scraper": lambda u: scrape_public_storage(u, "Public Storage (FM 2978)")},
        {"name": "SmartStop Self Storage",
         "url": "https://smartstopselfstorage.com/find-storage/tx/magnolia/32620-fm-2978",
         "scraper": scrape_smartstop},
        {"name": "Honea Egypt Self Storage",
         "url": "https://www.honeaegyptselfstorage.com/find-storage.aspx?id=68",
         "scraper": scrape_honea_egypt},
        {"name": "Montgomery Self Storage",
         "url": "https://montgomeryss.com/locations/magnolia-tx/",
         "scraper": scrape_montgomery},
        {"name": "Woodlands Storage & Office",
         "url": "https://www.woodlandssao.com/units",
         "scraper": scrape_woodlands_sao},
        {"name": "Storage King USA",
         "url": None,  # RV/boat parking only, no enclosed units
         "scraper": None},
    ]

    existing = {c["name"]: c for c in data.get("competitors", [])}
    changes = []

    for target in scrape_targets:
        name = target["name"]
        entry = existing.setdefault(name, {"name": name, "pricing": empty_pricing()})
        old_pricing = entry.get("pricing", empty_pricing())

        if target["scraper"] is None:
            print(f"\nSKIP {name} (no enclosed-unit pricing)")
            entry["scrapeStatus"] = "n/a"
            continue

        print(f"\nSCAN {name}...")
        result, status = target["scraper"](target["url"])
        new_pricing = result["pricing"] if result else None
        new_full = result.get("pricingFull", {}) if result else {}

        if status != "ok" or new_pricing is None:
            # Keep old numbers but be HONEST about it: status recorded,
            # lastVerified NOT bumped. The dashboard shows this as stale.
            print(f"  {status.upper()}: keeping previous data, marked as unverified")
            entry["scrapeStatus"] = status
            continue

        all_null = all(v is None for v in new_pricing.values())
        had_data = any(v is not None for v in old_pricing.values())
        if all_null and had_data:
            print("  WARNING: scrape parsed zero prices; keeping old data, marked unverified")
            entry["scrapeStatus"] = "failed"
            continue

        for s in SIZES:
            if old_pricing.get(s) != new_pricing.get(s):
                changes.append(f"  {name} {s}: ${old_pricing.get(s)} -> ${new_pricing.get(s)}")

        entry["pricing"] = new_pricing
        entry["pricingFull"] = new_full
        entry["scrapeStatus"] = "ok"
        entry["lastVerified"] = now_utc()
        found = sum(1 for v in new_pricing.values() if v is not None)
        print(f"  OK {name}: {found} prices found")

    data["lastUpdated"] = now_utc()
    data["competitors"] = [existing[t["name"]] for t in scrape_targets if t["name"] in existing]

    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # --- Price history: one snapshot per day inside data.json (effective rate per size) ---
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snapshot = {
        "date": today,
        "my": data.get("myPricing"),
        "facilities": {c["name"]: c.get("pricing") for c in data["competitors"] if c.get("scrapeStatus") != "n/a"},
    }
    snaps = data.setdefault("history", [])
    if snaps and snaps[-1].get("date") == today:
        snaps[-1] = snapshot  # same-day rerun replaces
    else:
        snaps.append(snapshot)
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"HISTORY: {len(snaps)} snapshot(s), latest {today}")

    print("\n" + "=" * 60)
    if changes:
        print(f"CHANGES: {len(changes)} price change(s) detected:")
        for c in changes:
            print(c)
    else:
        print("NO CHANGES: All prices unchanged")
    print(f"SAVED: {data_path}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
