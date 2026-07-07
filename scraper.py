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

SIZES = ["5x10", "10x10", "10x15", "10x20", "10x30"]

# Text that marks a listing as vehicle parking, not an enclosed unit.
# "Uncovered" marks true parking spaces. Enclosed drive-up units that allow
# vehicle parking inside are legitimate units and must NOT be excluded.
PARKING_RE = re.compile(r"uncovered|parking\s*space\s*only", re.I)

# --- Helpers -----------------------------------------------------------------

def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def fetch(url, timeout=30):
    """Fetch a URL and return the HTML as a string, or None on failure."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  WARNING: Failed to fetch {url}: {e}")
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


def scrape_smartstop(url):
    """
    SmartStop renders pricing client-side with JavaScript; plain HTTP fetches
    receive a page with no prices at all. Detect that and report 'blocked'
    rather than silently keeping stale numbers.
    """
    html = fetch(url)
    if html is None:
        return None, "failed"
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
        prices = card_prices(card)
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
    print("Magnolia Storage - Competitor Price Scraper v2")
    print(f"Run time: {now_utc()}")
    print("=" * 60)

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

    # --- Price history: one snapshot per day (effective rate per size) ---
    hist_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "history.json")
    if os.path.exists(hist_path):
        with open(hist_path, "r", encoding="utf-8") as f:
            hist = json.load(f)
    else:
        hist = {"snapshots": []}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snapshot = {
        "date": today,
        "my": data.get("myPricing"),
        "facilities": {c["name"]: c.get("pricing") for c in data["competitors"] if c.get("scrapeStatus") != "n/a"},
    }
    snaps = hist.setdefault("snapshots", [])
    if snaps and snaps[-1].get("date") == today:
        snaps[-1] = snapshot  # same-day rerun replaces
    else:
        snaps.append(snapshot)
    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(hist, f, indent=2, ensure_ascii=False)
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
