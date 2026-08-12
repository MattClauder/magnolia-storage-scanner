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


def _via_scraper_api(url, premium=True, render=True):
    """
    Wrap a target URL in a ScraperAPI request. `premium=true` uses residential
    IPs — required for hosts (like Montgomery) that reset connections from
    datacenter IPs, which ScraperAPI's default proxy pool also uses.
    Both flags cost credits, so callers that only need a plain HTML body from a
    blocked host (Public Storage) start with premium=False/render=False.
    """
    return ("https://api.scraperapi.com/?api_key=" + SCRAPER_API_KEY
            + ("&premium=true" if premium else "")
            + "&country_code=us"
            + ("&render=true" if render else "")
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


def fetch_resilient(url, marker, timeout=45):
    """
    Fetch a page that must contain `marker` to be usable, trying progressively
    more expensive routes and stopping at the first one that works:

      1. plain HTTP GET        (free, works when the host isn't blocking us)
      2. headless Chromium     (free, handles JS-rendered markup)
      3. ScraperAPI standard   (1 credit, new IP)
      4. ScraperAPI premium    (residential IP, last resort)

    Public Storage serves its prices in the raw HTML, so step 1 usually wins;
    the later steps only fire when the host starts refusing the CI runner,
    which is what silently stalled both PS listings for eleven days.
    """
    attempts = [("static", lambda: _fetch_static(url, timeout)),
                ("headless Chromium", lambda: fetch(url, timeout))]
    if SCRAPER_API_KEY:
        attempts.append(("ScraperAPI standard",
                         lambda: _fetch_static(_via_scraper_api(url, premium=False, render=False), 90)))
        attempts.append(("ScraperAPI premium",
                         lambda: _fetch_static(_via_scraper_api(url, premium=True, render=False), 120)))

    for label, attempt in attempts:
        try:
            html = attempt()
        except Exception as e:
            print(f"  {label} fetch raised: {e}")
            continue
        if html and marker in html:
            print(f"  fetched via {label} ({len(html)} bytes)")
            return html
        print(f"  {label} fetch unusable (marker '{marker}' missing)")
    return None


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

LOCKAWAY_DRIVEUP_RE = re.compile(r"Drive-Up", re.I)
LOCKAWAY_CLIMATE_RE = re.compile(r"Climate\s*Controlled", re.I)
LOCKAWAY_LEAD_DIM_RE = re.compile(r"\s*(\d{1,2})\s*'?\s*[xX\u00d7]\s*(\d{1,2})")

# 12x30 is Lockaway's only drive-up unit anywhere near a 10x30, so it stands in
# as the closest equivalent. The 8-foot-wide sizes are all climate interior and
# are filtered out anyway.
LOCKAWAY_MAP = {"5x10": "5x10", "10x10": "10x10", "10x15": "10x15",
                "10x20": "10x20", "10x30": "10x30", "12x30": "10x30"}


def scrape_lockaway(url):
    """
    Lockaway: several cards per size. Only DRIVE-UP cards are comparable to our
    units, so climate-controlled interior cards are skipped rather than being
    allowed to win on price (a climate 10x15 promo at $83 was being reported as
    the 10x15 drive-up rate).

    Lockaway also lists a cheaper "15 x 10" alongside the real "10 x 15". Both
    normalise to the same key, so cards whose dimensions are written in the same
    order as the size we are pricing win over reversed variants; price only
    breaks ties within the same orientation.
    """
    html = fetch(url)
    if html is None:
        return None, "failed"
    if "$" not in html:
        return None, "blocked"

    best = {}      # size -> (rank, promo)
    size_prices = {}
    size_full = {}
    for key, card, prefix in segment_cards(html):
        mapped = LOCKAWAY_MAP.get(key)
        if not mapped:
            continue
        if LOCKAWAY_CLIMATE_RE.search(card) or not LOCKAWAY_DRIVEUP_RE.search(card):
            continue
        prices = card_prices(card)
        if not prices:
            continue
        lead = LOCKAWAY_LEAD_DIM_RE.match(card)
        literal = f"{int(lead.group(1))}x{int(lead.group(2))}" if lead else key
        rank = 0 if literal == mapped else 1
        promo, regular = prices[0], prices[-1]
        if mapped not in best or (rank, promo) < best[mapped]:
            best[mapped] = (rank, promo)
            size_prices[mapped] = promo
            size_full[mapped] = {"regular": regular, "promo": promo}

    pricing = empty_pricing()
    pricing.update({s: p for s, p in size_prices.items() if s in pricing})
    return {"pricing": pricing, "pricingFull": size_full}, "ok"


PS_UNIT_RE = re.compile(
    r"Unit Size\s*(\d{1,2})\s*'?\s*x\s*(\d{1,2})\s*'?\s*Online price\s*\$\s*(\d+(?:\.\d{1,2})?)")
PS_INSTORE_RE = re.compile(r"In[-\s]?Store Rent\s*\$\s*(\d+(?:\.\d{1,2})?)", re.I)
PS_CLIMATE_RE = re.compile(r"climate\s*controlled", re.I)
PS_PARKING_RE = re.compile(r"uncovered|RV,\s*Boat,\s*or\s*Vehicle|Parking\s*\d", re.I)

# Public Storage sells odd dimensions (5x9, 7x19, 10x19, 15x14...). Map each to
# the nearest Magnolia size by floor area instead of maintaining an endless
# lookup table that goes stale every time PS relabels a unit. Gaps between the
# bands (e.g. 5x14 = 70 sq ft) are deliberately left unmapped rather than being
# forced into a size they do not really compare to.
PS_AREA_BANDS = [("5x10", 40, 62), ("10x10", 85, 115),
                 ("10x15", 130, 170), ("10x20", 180, 225), ("10x30", 270, 330)]


def ps_size_key(a, b):
    lo, hi = sorted((a, b))
    if lo < 5 or lo > 15:
        return None
    area = lo * hi
    for size, low, high in PS_AREA_BANDS:
        if low <= area <= high:
            return size
    return None


def scrape_public_storage(url, facility_name):
    """
    Public Storage: each unit block starts with "Unit Size N'xM' Online price
    $X" and carries its feature list plus the "In-Store Rent $Y" regular rate.
    Prices are server-rendered, so this parses the raw HTML and never depends
    on a JS render.

    Only DRIVE-UP enclosed units are recorded — climate-controlled interior
    units and uncovered RV/boat parking are different products and were
    previously being mixed into the same size buckets (both PS listings had
    climate rates masquerading as drive-up comparisons).
    """
    html = fetch_resilient(url, "Online price")
    if html is None:
        return None, "failed"

    text = strip_tags(html)
    matches = list(PS_UNIT_RE.finditer(text))
    if not matches:
        return None, "blocked"

    size_prices = {}
    size_full = {}
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else min(len(text), m.end() + 900)
        block = text[m.start():end]
        if PS_PARKING_RE.search(block):
            continue  # uncovered RV/boat/vehicle space, not an enclosed unit
        if PS_CLIMATE_RE.search(block):
            continue  # interior climate unit, not comparable to our drive-up
        if "Drive up access" not in block:
            continue
        mapped = ps_size_key(int(m.group(1)), int(m.group(2)))
        if not mapped:
            continue
        online = round(float(m.group(3)))
        mi = PS_INSTORE_RE.search(block)
        regular = round(float(mi.group(1))) if mi else online
        if mapped not in size_prices or online < size_prices[mapped]:
            size_prices[mapped] = online
            size_full[mapped] = {"regular": regular, "promo": online}

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


SMARTSTOP_UNITS_API = ("https://smartstopselfstorage.com/umbraco/rhythm/locationsapi/"
                       "GetUnits?facilityIds={fid}&culture=en-US")
SMARTSTOP_FACILITY_ID = "1100006149"   # 32620 FM 2978, Magnolia (store code 6149)
SMARTSTOP_FID_RE = re.compile(r'"locationIds"\s*:\s*\[\s*(\d+)')
SMARTSTOP_DRIVEUP_RE = re.compile(r"drive-?up", re.I)


def scrape_smartstop(url):
    """
    SmartStop renders an empty units array in the initial HTML and fills it in
    from an Umbraco endpoint after load, which is why scraping the page (with or
    without a JS render) always came back with nothing and the facility looked
    like it had no online inventory.

    This calls that endpoint directly. It needs no browser, returns clean JSON
    with both the web rate and the in-store rate, and flags each unit's features
    so DRIVE-UP units can be separated from climate-controlled interior ones.
    """
    facility_id = SMARTSTOP_FACILITY_ID
    page = _fetch_static(url, 30)
    if page:
        m = SMARTSTOP_FID_RE.search(page)
        if m:
            facility_id = m.group(1)

    raw = _fetch_static(SMARTSTOP_UNITS_API.format(fid=facility_id), 45)
    if not raw:
        return None, "failed"
    try:
        units = json.loads(raw)
    except Exception as e:
        print(f"  WARNING: SmartStop units API did not return JSON: {e}")
        return None, "failed"
    if not isinstance(units, list):
        return None, "failed"

    size_prices, size_full = {}, {}
    for u in units:
        if not isinstance(u, dict):
            continue
        features = " ".join(str(f.get("name", "")) for f in (u.get("features") or [])
                            if isinstance(f, dict))
        if not SMARTSTOP_DRIVEUP_RE.search(features):
            continue  # climate interior / wine storage, not comparable
        w, l = u.get("width"), u.get("length")
        if w is None or l is None:
            continue
        if float(w) != int(float(w)) or float(l) != int(float(l)):
            continue  # half-foot sizes (7.5x10) have no equivalent here
        lo, hi = sorted((int(float(w)), int(float(l))))
        key = f"{lo}x{hi}"
        if key not in SIZES:
            continue
        web = u.get("webRateIncludingDiscount") or u.get("webRate")
        store = u.get("storeRate") or web
        if web is None:
            continue
        web, store = round(float(web)), round(float(store))
        if key not in size_prices or web < size_prices[key]:
            size_prices[key] = web
            size_full[key] = {"regular": store, "promo": web}

    pricing = empty_pricing()
    pricing.update(size_prices)
    # An empty list is a real "call for availability" state, not a failure.
    return {"pricing": pricing, "pricingFull": size_full}, "ok"


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
