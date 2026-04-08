#!/usr/bin/env python3
"""
SKU Scanner — Go-UPC Barcode Lookup Tool
Looks up product information for UPC barcodes from an Excel POS export.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ─── Configuration ───────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(SCRIPT_DIR, "input.xlsx")
OUTPUT_DIR = SCRIPT_DIR
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, "checkpoint.json")
LOG_FILE = os.path.join(OUTPUT_DIR, "sku_scanner.log")

REQUEST_DELAY = 45          # seconds between requests (increase if getting 429s)
MAX_RETRIES = 5            # retry on network errors
CHECKPOINT_INTERVAL = 100  # save checkpoint every N rows
INTERNAL_CODE_PREFIX = "0000000"  # store-internal SKU prefix to skip

GOUPC_URL = "https://go-upc.com/search?q={barcode}"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

# ─── Logging setup ───────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("sku_scanner")

# ─── Scraping ────────────────────────────────────────────────────────────────


def parse_product_page(soup: BeautifulSoup) -> dict:
    """Extract product fields from a Go-UPC search result page."""
    result = {
        "FULL_NAME_FOUND": "",
        "DESCRIPTION": "",
        "CATEGORY": "",
        "BRAND": "",
        "SIZE": "",
        "EAN": "",
    }

    # Product name — <h1 class="product-name">
    h1 = soup.find("h1", class_="product-name")
    if h1:
        result["FULL_NAME_FOUND"] = h1.get_text(strip=True)

    # Table with EAN, UPC, Brand, Category
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)
            if label == "brand":
                result["BRAND"] = value
            elif label == "category":
                result["CATEGORY"] = value
            elif label == "ean":
                result["EAN"] = value

    # Structured data badges — category and size/volume
    structured = soup.find("div", class_="structured-data")
    if structured:
        size_badge = structured.find("span", class_="item-details")
        if size_badge:
            result["SIZE"] = size_badge.get_text(strip=True)

    # Description — text after <h2>Description</h2>
    for h2 in soup.find_all("h2"):
        if "description" in h2.get_text(strip=True).lower():
            desc_sibling = h2.find_next_sibling()
            if desc_sibling:
                result["DESCRIPTION"] = desc_sibling.get_text(strip=True)
            break

    return result


_session = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
    return _session


def lookup_upc(barcode: str) -> dict:
    """Look up a single barcode on Go-UPC. Returns dict with product fields + STATUS."""
    url = GOUPC_URL.format(barcode=barcode)
    session = get_session()
    session.headers["User-Agent"] = random.choice(USER_AGENTS)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=20)

            if resp.status_code == 400:
                log.info(f"  {barcode} -> UNMATCHED (400 invalid barcode)")
                return {"STATUS": "UNMATCHED"}

            if resp.status_code == 429:
                wait = min(120, 30 * attempt)
                log.warning(f"  {barcode} -> Rate limited (429), waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    time.sleep(wait)
                    continue
                return {"STATUS": "ERROR"}

            if resp.status_code != 200:
                log.warning(f"  {barcode} -> HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
                return {"STATUS": "ERROR"}

            soup = BeautifulSoup(resp.text, "html.parser")

            # Check if product was actually found
            product_name = soup.find("h1", class_="product-name")
            if not product_name:
                log.info(f"  {barcode} -> UNMATCHED (no product name in response)")
                return {"STATUS": "UNMATCHED"}

            result = parse_product_page(soup)
            if result["FULL_NAME_FOUND"]:
                result["STATUS"] = "MATCHED"
                log.info(f"  {barcode} -> MATCHED: {result['FULL_NAME_FOUND'][:60]}")
            else:
                result["STATUS"] = "UNMATCHED"
                log.info(f"  {barcode} -> UNMATCHED (empty product name)")

            return result

        except requests.RequestException as e:
            log.warning(f"  {barcode} -> Network error (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)
            else:
                log.error(f"  {barcode} -> ERROR after {MAX_RETRIES} retries")
                return {"STATUS": "ERROR"}

    return {"STATUS": "ERROR"}


# ─── Checkpoint ──────────────────────────────────────────────────────────────


def load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        log.info(f"Loaded checkpoint with {len(data)} entries")
        return data
    return {}


def save_checkpoint(data: dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    log.info("=" * 60)
    log.info("SKU Scanner starting")
    log.info(f"Input: {INPUT_FILE}")
    log.info("=" * 60)

    # Load input
    df = pd.read_excel(INPUT_FILE, dtype={"Scan code": str})
    total = len(df)
    log.info(f"Loaded {total} rows from input file")

    # Load checkpoint
    checkpoint = load_checkpoint()

    # Counters
    matched = sum(1 for v in checkpoint.values() if v.get("STATUS") == "MATCHED")
    unmatched = sum(1 for v in checkpoint.values() if v.get("STATUS") == "UNMATCHED")
    errors = sum(1 for v in checkpoint.values() if v.get("STATUS") == "ERROR")
    skipped = sum(1 for v in checkpoint.values() if v.get("STATUS") == "SKIPPED")

    # Save checkpoint on Ctrl+C so progress is never lost
    def on_interrupt(sig, frame):
        print("\n\nInterrupted! Saving checkpoint...")
        save_checkpoint(checkpoint)
        log.info(f"Checkpoint saved on interrupt ({len(checkpoint)} entries)")
        sys.exit(0)

    signal.signal(signal.SIGINT, on_interrupt)

    # Process each row
    for idx, row in df.iterrows():
        barcode = str(row["Scan code"]).strip()
        current = idx + 1

        # Skip if already in checkpoint
        if barcode in checkpoint:
            continue

        print(f"\rProcessing {current} of {total}...", end="", flush=True)

        # Skip internal codes
        if barcode.startswith(INTERNAL_CODE_PREFIX):
            result = {"STATUS": "SKIPPED"}
            log.info(f"  {barcode} -> SKIPPED (internal code)")
            checkpoint[barcode] = result
            skipped += 1
            continue

        # Look up on Go-UPC
        result = lookup_upc(barcode)
        checkpoint[barcode] = result

        if result["STATUS"] == "MATCHED":
            matched += 1
        elif result["STATUS"] == "UNMATCHED":
            unmatched += 1
        else:
            errors += 1

        # Save checkpoint after every lookup
        save_checkpoint(checkpoint)

        # Delay between requests with random jitter
        time.sleep(REQUEST_DELAY + random.uniform(0, 10))

    # Final checkpoint save
    save_checkpoint(checkpoint)
    print()  # newline after progress counter

    # ─── Build output ────────────────────────────────────────────────────

    log.info("Building output file...")

    new_cols = {
        "ORIGINAL_NAME": [],
        "FULL_NAME_FOUND": [],
        "DESCRIPTION": [],
        "CATEGORY": [],
        "BRAND": [],
        "SIZE": [],
        "EAN": [],
        "SOURCE": [],
        "STATUS": [],
    }

    for _, row in df.iterrows():
        barcode = str(row["Scan code"]).strip()
        result = checkpoint.get(barcode, {"STATUS": "ERROR"})

        new_cols["ORIGINAL_NAME"].append(row["Description"])
        new_cols["FULL_NAME_FOUND"].append(result.get("FULL_NAME_FOUND", ""))
        new_cols["DESCRIPTION"].append(result.get("DESCRIPTION", ""))
        new_cols["CATEGORY"].append(result.get("CATEGORY", ""))
        new_cols["BRAND"].append(result.get("BRAND", ""))
        new_cols["SIZE"].append(result.get("SIZE", ""))
        new_cols["EAN"].append(result.get("EAN", ""))
        new_cols["SOURCE"].append("GoUPC" if result["STATUS"] == "MATCHED" else "")
        new_cols["STATUS"].append(result["STATUS"])

    for col_name, values in new_cols.items():
        df[col_name] = values

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"SKU_Lookup_Results_{timestamp}.xlsx")

    # Write with openpyxl to preserve leading zeros on numeric-looking text columns
    from openpyxl import Workbook
    from openpyxl.utils.dataframe import dataframe_to_rows

    text_columns = {"Scan code", "EAN"}
    text_col_indices = {i + 1 for i, col in enumerate(df.columns) if col in text_columns}

    wb = Workbook()
    ws = wb.active
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=value)
            if r_idx > 1 and c_idx in text_col_indices and value is not None:
                cell.number_format = "@"
    wb.save(output_file)
    log.info(f"Output saved to: {output_file}")

    # ─── Summary ─────────────────────────────────────────────────────────

    print("\n" + "=" * 50)
    print("  SKU SCANNER — FINAL SUMMARY")
    print("=" * 50)
    print(f"  Total SKUs processed: {total}")
    print(f"  Matched:              {matched}")
    print(f"  Unmatched:            {unmatched}")
    print(f"  Errors:               {errors}")
    print(f"  Skipped (internal):   {skipped}")
    print(f"  Output file:          {output_file}")
    print("=" * 50)

    log.info(f"Done. Matched={matched}, Unmatched={unmatched}, Errors={errors}, Skipped={skipped}")


if __name__ == "__main__":
    main()
