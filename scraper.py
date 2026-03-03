#!/usr/bin/env python3
"""
Magnolia Storage - Competitor Price Scraper
Runs daily via GitHub Actions to update data.json with current competitor pricing.
Uses only Python stdlib (no pip dependencies needed).
"""

import json
import re
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request


# --- Helpers -----------------------------------------------------------------

def fetch(url, timeout=30):
    """Fetch a URL and return the HTML as a string."""
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


def normalize_size(w, h):
    """Normalize a WxH pair so the smaller dimension comes first."""
    w, h = int(w), int(h)
    return (min(w, h), max(w, h))


def size_key(w, h):
    """Create a standard size key like '5x10' from two dimensions."""
    nw, nh = normalize_size(w, h)
    return f"{nw}x{nh}"


# --- Scrapers ----------------------------------------------------------------

def scrape_lockaway(url):
    """
    Lockaway Storage - Magnolia, TX
    Prices shown as "STARTING AT $XX" near unit dimension text like "5' x 10'".
    We look for 'STARTING' or 'IN-STORE' markers near dimensions and prices.
    The key fix: limit search distance between dimension and price to avoid
    matching across different unit cards.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Match dimension blocks: "5' x 10'" ... "$44" within a limited window (500 chars)
    # This prevents matching a dimension in one card with a price in a different card.
    blocks = re.findall(
        r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,500}?)(?:STARTING\s+AT|PROMO\s+RATE|ONLINE)\s*(?:<[^>]*>)*\s*\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    if not blocks:
        # Fallback: simpler pattern with tight window
        blocks = re.findall(
            r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,300}?)\$(\d+(?:\.\d{2})?)',
            html, re.IGNORECASE | re.DOTALL
        )

    # Map Lockaway non-standard sizes to our targets
    lockaway_map = {
        "5x10": "5x10",
        "8x10": "5x10",  # ~same sqft
        "10x10": "10x10",
        "10x15": "10x15",
        "15x10": "10x15",  # reversed
        "8x15": "10x15",   # close
        "10x20": "10x20",
        "8x20": "10x20",   # close
        "10x30": "10x30",
        "12x30": "10x30",  # close
    }

    size_prices = {}
    for w, d, price in blocks:
        raw_key = size_key(w, d)
        mapped = lockaway_map.get(raw_key)
        if not mapped:
            # Try with original order too
            mapped = lockaway_map.get(f"{w}x{d}")
        if mapped:
            price_val = float(price)
            # Use the ONLINE/STARTING price (not in-store) - keep lowest online price
            if mapped not in size_prices or price_val < size_prices[mapped]:
                size_prices[mapped] = price_val

    for s, p in size_prices.items():
        if s in pricing:
            pricing[s] = int(round(p))

    print(f"  OK Lockaway Storage: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_public_storage(url, facility_name):
    """
    Public Storage - uses data-pricebook-price attributes in HTML.
    Non-standard sizes need mapping to our standard 5x10/10x10/etc.
    Key fix: don't map 5x5 to 5x10 - only use actual 5x9+ for 5x10 equiv.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Find units: dimension text near data-pricebook-price attribute
    # Try: dimensions BEFORE price
    unit_blocks = re.findall(
        r'(\d+)[\s\'\"&#;x39]*x\s*(\d+)[\s\'\"&#;x39]*.{0,2000}?data-pricebook-price="([\d.]+)"',
        html, re.IGNORECASE | re.DOTALL
    )

    if not unit_blocks:
        # Try: price BEFORE dimensions
        alt_blocks = re.findall(
            r'data-pricebook-price="([\d.]+)".{0,2000}?(\d+)[\s\'\"&#;x39]*x\s*(\d+)',
            html, re.IGNORECASE | re.DOTALL
        )
        unit_blocks = [(b[1], b[2], b[0]) for b in alt_blocks]

    # Map PS non-standard sizes to our targets
    # IMPORTANT: 5x5 does NOT map to 5x10 - it's too small
    ps_map = {
        "5x9": "5x10", "5x10": "5x10", "5x14": "5x10", "5x15": "5x10",
        "7x14": "10x10", "10x10": "10x10",
        "10x15": "10x15",
        "10x19": "10x20", "10x20": "10x20",
        "10x30": "10x30", "10x40": "10x30",
    }

    size_prices = {}
    for w, d, price in unit_blocks:
        raw_key = size_key(w, d)
        mapped = ps_map.get(raw_key)
        if not mapped:
            mapped = ps_map.get(f"{w}x{d}")
        if mapped:
            price_val = float(price)
            if mapped not in size_prices or price_val < size_prices[mapped]:
                size_prices[mapped] = price_val

    for s, p in size_prices.items():
        if s in pricing:
            pricing[s] = int(round(p))

    print(f"  OK {facility_name}: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_smartstop(url):
    """
    SmartStop Self Storage - now publishes prices online.
    Format: "10' x 15'" with "In-Store $140" and "Promo Rate $70/mo" nearby.
    We capture the in-store (regular) price.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Pattern: dimensions ... "In-Store" ... "$XX" within a tight window
    blocks = re.findall(
        r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,400}?)In-Store\s*(?:<[^>]*>)*\s*\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    if not blocks:
        # Fallback: look for promo rate (the displayed price)
        blocks = re.findall(
            r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,400}?)(?:Promo\s+Rate|Starting)\s*(?:<[^>]*>)*\s*\$(\d+(?:\.\d{2})?)',
            html, re.IGNORECASE | re.DOTALL
        )

    if not blocks:
        # Even broader fallback
        blocks = re.findall(
            r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,300}?)\$(\d+(?:\.\d{2})?)',
            html, re.IGNORECASE | re.DOTALL
        )

    size_prices = {}
    for w, d, price in blocks:
        raw_key = size_key(w, d)
        if raw_key in pricing:
            price_val = float(price)
            # Keep the cheapest per size category
            if raw_key not in size_prices or price_val < size_prices[raw_key]:
                size_prices[raw_key] = price_val

    for s, p in size_prices.items():
        if s in pricing:
            pricing[s] = int(round(p))

    print(f"  OK SmartStop Self Storage: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_honea_egypt(url):
    """
    Honea Egypt Self Storage - ASP.NET page with prices as "$82.00/month".
    Key fix: handle reversed dimensions like "10'x5'" -> 5x10.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Match: "5'x5'" ... "$37.50/month" with a tight window
    blocks = re.findall(
        r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,400}?)\$(\d+(?:\.\d{2})?)\s*/?\s*month',
        html, re.IGNORECASE | re.DOTALL
    )

    size_prices = {}
    for w, d, price in blocks:
        raw_key = size_key(w, d)  # normalize_size handles reversed dims
        if raw_key in pricing:
            price_val = float(price)
            if raw_key not in size_prices or price_val < size_prices[raw_key]:
                size_prices[raw_key] = price_val

    for s, p in size_prices.items():
        if s in pricing:
            pricing[s] = int(round(p))

    print(f"  OK Honea Egypt: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_montgomery(url):
    """
    Montgomery Self Storage - prices shown as "$60" with unit sizes.
    Has both climate and non-climate. We take the cheapest (non-climate).
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Match dimension + price within a tight window
    blocks = re.findall(
        r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,300}?)\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    size_prices = {}
    for w, d, price in blocks:
        raw_key = size_key(w, d)
        if raw_key in pricing:
            price_val = float(price)
            if raw_key not in size_prices or price_val < size_prices[raw_key]:
                size_prices[raw_key] = price_val

    for s, p in size_prices.items():
        if s in pricing:
            pricing[s] = int(round(p))

    print(f"  OK Montgomery Self Storage: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_woodlands_sao(url):
    """
    Woodlands Storage & Office - may render via JavaScript.
    They show units like "10' x 10'" with "$47" (regular) and "$23.50" (promo).
    We want the regular (non-promo) price, shown as strikethrough.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Try to find dimension + price patterns
    blocks = re.findall(
        r'(\d+)[\s\'\"]*x\s*(\d+)[\s\'\"]*(?:.{0,400}?)\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    # Map Woodlands non-standard sizes
    woodlands_map = {
        "10x10": "10x10",
        "10x12": "10x10",  # close
        "10x20": "10x20",
        "20x10": "10x20",  # reversed
        "10x30": "10x30",
        "12x30": "10x30",  # close
    }

    size_prices = {}
    for w, d, price in blocks:
        raw_key = size_key(w, d)
        mapped = woodlands_map.get(raw_key)
        if not mapped:
            mapped = woodlands_map.get(f"{w}x{d}")
        if mapped:
            price_val = float(price)
            # Keep the HIGHEST price per category (the regular price, not the promo)
            if mapped not in size_prices or price_val > size_prices[mapped]:
                size_prices[mapped] = price_val

    for s, p in size_prices.items():
        if s in pricing:
            pricing[s] = int(round(p))

    print(f"  OK Woodlands Storage & Office: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


# --- Main --------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Magnolia Storage - Competitor Price Scraper")
    print(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load existing data.json
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

    if os.path.exists(data_path):
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"lastUpdated": None, "competitors": []}

    # Define competitors and their scraping functions
    scrape_targets = [
        {
            "name": "Lockaway Storage",
            "url": "https://www.lockaway-storage.com/storage-units/texas/magnolia/lockaway-storage-1488-411002/",
            "scraper": lambda url: scrape_lockaway(url),
        },
        {
            "name": "Public Storage (FM 1488)",
            "url": "https://www.publicstorage.com/self-storage-tx-magnolia/2360.html",
            "scraper": lambda url: scrape_public_storage(url, "Public Storage (FM 1488)"),
        },
        {
            "name": "Public Storage (FM 2978)",
            "url": "https://www.publicstorage.com/self-storage-tx-the-woodlands/5888.html",
            "scraper": lambda url: scrape_public_storage(url, "Public Storage (FM 2978)"),
        },
        {
            "name": "SmartStop Self Storage",
            "url": "https://smartstopselfstorage.com/find-storage/tx/magnolia/32620-fm-2978",
            "scraper": lambda url: scrape_smartstop(url),
        },
        {
            "name": "Honea Egypt Self Storage",
            "url": "https://www.honeaegyptselfstorage.com/find-storage.aspx?id=68",
            "scraper": lambda url: scrape_honea_egypt(url),
        },
        {
            "name": "Montgomery Self Storage",
            "url": "https://montgomeryss.com/locations/magnolia-tx/",
            "scraper": lambda url: scrape_montgomery(url),
        },
        {
            "name": "Woodlands Storage & Office",
            "url": "https://www.woodlandssao.com/units",
            "scraper": lambda url: scrape_woodlands_sao(url),
        },
        {
            "name": "Storage King USA",
            "url": None,  # RV/boat parking only, no enclosed unit pricing
            "scraper": None,
        },
    ]

    # Build lookup of existing competitor data
    existing = {}
    for c in data.get("competitors", []):
        existing[c["name"]] = c

    changes = []

    for target in scrape_targets:
        name = target["name"]
        old_data = existing.get(name, {})
        old_pricing = old_data.get("pricing", {
            "5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None
        })

        if target["scraper"] is None:
            print(f"\nSKIP {name} (no online pricing)")
            new_pricing = old_pricing
        else:
            print(f"\nSCAN {name}...")
            new_pricing = target["scraper"](target["url"])
            if new_pricing is None:
                print(f"  WARNING: Scrape failed, keeping old data")
                new_pricing = old_pricing
            else:
                # If scraper returned all nulls but we had data before, keep old data
                all_null = all(v is None for v in new_pricing.values())
                had_data = any(v is not None for v in old_pricing.values())
                if all_null and had_data:
                    print(f"  WARNING: Scraper returned all nulls but old data exists, keeping old data")
                    new_pricing = old_pricing
                else:
                    # Log price changes
                    for s in ["5x10", "10x10", "10x15", "10x20", "10x30"]:
                        old_val = old_pricing.get(s)
                        new_val = new_pricing.get(s)
                        if old_val != new_val:
                            changes.append(f"  {name} {s}: ${old_val} -> ${new_val}")

        # Update pricing in existing data, preserving all other fields
        if name in existing:
            existing[name]["pricing"] = new_pricing
        else:
            existing[name] = {"name": name, "pricing": new_pricing}

    # Update timestamp
    data["lastUpdated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Rebuild competitors list preserving original order
    data["competitors"] = [existing[t["name"]] for t in scrape_targets if t["name"] in existing]

    # Write updated data
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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
