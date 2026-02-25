#!/usr/bin/env python3
"""
Magnolia Storage — Competitor Price Scraper
Runs daily via GitHub Actions to update data.json with current competitor pricing.
"""

import json
import re
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from html.parser import HTMLParser

# ─── Helpers ───────────────────────────────────────────────────────────────────

def fetch(url, timeout=30):
    """Fetch a URL and return the HTML as a string."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ⚠ Failed to fetch {url}: {e}")
        return None


def find_prices_in_text(html):
    """Extract all dollar amounts from HTML text."""
    return re.findall(r'\$(\d+(?:\.\d{2})?)', html)


def extract_between(html, start_marker, end_marker):
    """Extract text between two markers."""
    idx = html.find(start_marker)
    if idx == -1:
        return None
    idx += len(start_marker)
    end = html.find(end_marker, idx)
    if end == -1:
        return html[idx:]
    return html[idx:end]


# ─── Scrapers ──────────────────────────────────────────────────────────────────

def scrape_public_storage(url, facility_name):
    """
    Public Storage embeds pricing in data attributes:
    data-pricebook-price="41.0" on span.unit-price elements
    Unit sizes are in nearby elements.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Public Storage uses non-standard sizes. We map closest equivalents.
    # Find unit blocks - they contain data-pricebook-price and unit dimensions
    # Pattern: look for unit size text near price data attributes

    # Extract all unit data from the page
    # Public Storage format: dimensions like "5x14", "10x14", "10x19" etc with data-pricebook-price
    unit_blocks = re.findall(
        r'(?:(\d+)(?:\'|&#39;|&\#39;)?\s*x\s*(\d+)(?:\'|&#39;|&\#39;)?)[^$]*?data-pricebook-price="([\d.]+)"',
        html, re.IGNORECASE | re.DOTALL
    )

    if not unit_blocks:
        # Try alternate pattern: price before dimensions
        unit_blocks = re.findall(
            r'data-pricebook-price="([\d.]+)"[^>]*>.*?(\d+)(?:\'|&#39;)?\s*x\s*(\d+)',
            html, re.IGNORECASE | re.DOTALL
        )
        if unit_blocks:
            unit_blocks = [(b[1], b[2], b[0]) for b in unit_blocks]

    if not unit_blocks:
        # Broader pattern: find all pricebook prices and nearby dimension text
        prices_raw = re.findall(r'data-pricebook-price="([\d.]+)"', html)
        dims_raw = re.findall(r'(\d+)(?:\'|&#x27;|&\#39;|ft)?\s*x\s*(\d+)', html)
        print(f"  Found {len(prices_raw)} prices, {len(dims_raw)} dimensions (no paired matches)")
        # Can't reliably pair them, return None
        if not prices_raw:
            return pricing

    # Map non-standard PS sizes to our standard sizes
    size_map = {
        (5, 5): "5x10", (5, 6): "5x10", (5, 8): "5x10", (5, 10): "5x10",
        (5, 14): "5x10", (5, 15): "5x10",
        (7, 14): "10x10", (10, 10): "10x10", (10, 14): "10x10",
        (10, 15): "10x15", (10, 17): "10x15",
        (10, 19): "10x20", (10, 20): "10x20",
        (10, 30): "10x30", (10, 40): "10x30", (12, 28): "10x30",
    }

    # Collect all prices per mapped size, keep the cheapest
    size_prices = {}
    for w, d, price in unit_blocks:
        w, d = int(w), int(d)
        price = float(price)
        mapped = size_map.get((w, d))
        if mapped:
            if mapped not in size_prices or price < size_prices[mapped]:
                size_prices[mapped] = price

    for size, price in size_prices.items():
        pricing[size] = int(round(price))

    print(f"  ✓ {facility_name}: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_lockaway(url):
    """
    Lockaway Storage shows prices in spans with class 'start-price' or similar.
    Unit sizes appear as dimension text like "5' x 10'" near price elements.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Find patterns like: dimensions followed by price
    # Lockaway format: "5' x 10'" ... "$59"
    blocks = re.findall(
        r'(\d+)(?:\'|&#39;|&\#39;|ft)?\s*x\s*(\d+)(?:\'|&#39;|&\#39;|ft)?.*?\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    size_prices = {}
    for w, d, price in blocks:
        key = f"{w}x{d}"
        price_val = float(price)
        if key in pricing:
            if key not in size_prices or price_val < size_prices[key]:
                size_prices[key] = price_val

    # Also try: 8x4 -> skip, 10x10, etc.
    # Map some non-standard Lockaway sizes
    lockaway_map = {"8x4": None, "8x8": "5x10", "8x12": "10x10"}
    for w, d, price in blocks:
        key = f"{w}x{d}"
        mapped = lockaway_map.get(key)
        if mapped and (mapped not in size_prices or float(price) < size_prices[mapped]):
            size_prices[mapped] = float(price)

    for size, price in size_prices.items():
        if size in pricing:
            pricing[size] = int(round(price))

    print(f"  ✓ Lockaway Storage: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_honea_egypt(url):
    """
    Honea Egypt Self Storage - ASP.NET page with prices in text like "$82.00/month".
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Find dimension + price patterns
    # Format: "5 x 10" or "5x10" near "$82.00/month" or "$82"
    blocks = re.findall(
        r'(\d+)\s*[\'ft]*\s*x\s*(\d+)\s*[\'ft]*[^$]*?\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    for w, d, price in blocks:
        key = f"{w}x{d}"
        if key in pricing:
            pricing[key] = int(round(float(price)))

    print(f"  ✓ Honea Egypt: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_montgomery(url):
    """
    Montgomery Self Storage - prices shown as "$55.00/mo" with unit sizes.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Find dimension + price patterns
    blocks = re.findall(
        r'(\d+)\s*[\'ft]*\s*x\s*(\d+)\s*[\'ft]*[^$]*?\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    size_prices = {}
    for w, d, price in blocks:
        key = f"{w}x{d}"
        price_val = float(price)
        if key in pricing and (key not in size_prices or price_val < size_prices[key]):
            size_prices[key] = price_val

    for size, price in size_prices.items():
        pricing[size] = int(round(price))

    print(f"  ✓ Montgomery Self Storage: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


def scrape_woodlands_sao(url):
    """
    Woodlands Storage & Office - Bootstrap layout with .unit-price class.
    """
    html = fetch(url)
    if not html:
        return None

    pricing = {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None}

    # Find dimension + price patterns
    blocks = re.findall(
        r'(\d+)\s*[\'ft]*\s*x\s*(\d+)\s*[\'ft]*[^$]*?\$(\d+(?:\.\d{2})?)',
        html, re.IGNORECASE | re.DOTALL
    )

    size_prices = {}
    for w, d, price in blocks:
        key = f"{w}x{d}"
        price_val = float(price)
        # Map non-standard sizes
        mapped = key
        if key == "12x10":
            mapped = "10x10"
        elif key == "12x30":
            mapped = "10x30"

        if mapped in pricing and (mapped not in size_prices or price_val < size_prices[mapped]):
            size_prices[mapped] = price_val

    for size, price in size_prices.items():
        pricing[size] = int(round(price))

    print(f"  ✓ Woodlands Storage & Office: {sum(1 for v in pricing.values() if v is not None)} prices found")
    return pricing


# ─── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Magnolia Storage — Competitor Price Scraper")
    print(f"Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # Load existing data.json if it exists
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

    if os.path.exists(data_path):
        with open(data_path, "r") as f:
            data = json.load(f)
    else:
        # Initialize with default structure
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
            "url": None,  # No prices published online
            "scraper": None,
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

    # Build a lookup of existing competitor data
    existing = {}
    for c in data.get("competitors", []):
        existing[c["name"]] = c

    updated_competitors = []
    changes = []

    for target in scrape_targets:
        name = target["name"]
        old_data = existing.get(name, {})
        old_pricing = old_data.get("pricing", {"5x10": None, "10x10": None, "10x15": None, "10x20": None, "10x30": None})

        if target["scraper"] is None:
            print(f"\n⏭ Skipping {name} (no online pricing)")
            new_pricing = old_pricing
        else:
            print(f"\n🔍 Scraping {name}...")
            new_pricing = target["scraper"](target["url"])
            if new_pricing is None:
                print(f"  ⚠ Scrape failed, keeping old data")
                new_pricing = old_pricing
            else:
                # Check for price changes
                for size in ["5x10", "10x10", "10x15", "10x20", "10x30"]:
                    old_val = old_pricing.get(size)
                    new_val = new_pricing.get(size)
                    if old_val != new_val:
                        changes.append(f"  {name} {size}: ${old_val} → ${new_val}")

        updated_competitors.append({
            "name": name,
            "pricing": new_pricing,
        })

    # Update data.json
    data["lastUpdated"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Merge scraped pricing into full competitor data
    # Keep all existing metadata (address, phone, etc.), only update pricing
    full_competitors = []
    for updated in updated_competitors:
        if updated["name"] in existing:
            comp = dict(existing[updated["name"]])
            comp["pricing"] = updated["pricing"]
        else:
            comp = updated
        full_competitors.append(comp)

    data["competitors"] = full_competitors

    with open(data_path, "w") as f:
        json.dump(data, f, indent=2)

    print("\n" + "=" * 60)
    if changes:
        print(f"📊 {len(changes)} price change(s) detected:")
        for c in changes:
            print(c)
    else:
        print("✅ No price changes detected")
    print(f"💾 Saved to {data_path}")
    print("=" * 60)

    return 0 if not any(t["scraper"] and scrape_targets for t in scrape_targets) else 0


if __name__ == "__main__":
    sys.exit(main())
