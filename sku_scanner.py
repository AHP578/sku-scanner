#!/usr/bin/env python3
"""
SKU Scanner — Go-UPC Barcode Lookup Tool
Looks up product information for UPC barcodes from an Excel POS export.

Usage:
    python sku_scanner.py                  # Run all remaining SKUs (local mode)
    python sku_scanner.py --batch 30       # Process 30 SKUs then stop
    python sku_scanner.py --output-only    # Generate Excel from checkpoint (no lookups)
    python sku_scanner.py --status         # Show progress summary
"""

import argparse
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
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, "checkpoint.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "sku_scanner.log")
LOCK_FILE = os.path.join(SCRIPT_DIR, "running.lock")

REQUEST_DELAY = 45         # seconds between requests (increase if getting 429s)
MAX_RETRIES = 3            # retry on network errors
INTERNAL_CODE_PREFIX = "0000000"  # store-internal SKU prefix to skip

RATE_LIMIT_STRIKES = 1     # consecutive 429s before pausing
RATE_LIMIT_COOLDOWN = 2    # hours to wait after hitting rate limit wall

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

    h1 = soup.find("h1", class_="product-name")
    if h1:
        result["FULL_NAME_FOUND"] = h1.get_text(strip=True)

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

    structured = soup.find("div", class_="structured-data")
    if structured:
        size_badge = structured.find("span", class_="item-details")
        if size_badge:
            result["SIZE"] = size_badge.get_text(strip=True)

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
                return {"STATUS": "RATE_LIMITED"}

            if resp.status_code != 200:
                log.warning(f"  {barcode} -> HTTP {resp.status_code} (attempt {attempt}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES:
                    time.sleep(2 ** attempt)
                    continue
                return {"STATUS": "ERROR"}

            soup = BeautifulSoup(resp.text, "html.parser")

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


# ─── Output ──────────────────────────────────────────────────────────────────


def build_output(df: pd.DataFrame, checkpoint: dict) -> str:
    """Build the output Excel file from the dataframe and checkpoint. Returns file path."""
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
        result = checkpoint.get(barcode, {"STATUS": "PENDING"})

        new_cols["ORIGINAL_NAME"].append(row["Description"])
        new_cols["FULL_NAME_FOUND"].append(result.get("FULL_NAME_FOUND", ""))
        new_cols["DESCRIPTION"].append(result.get("DESCRIPTION", ""))
        new_cols["CATEGORY"].append(result.get("CATEGORY", ""))
        new_cols["BRAND"].append(result.get("BRAND", ""))
        new_cols["SIZE"].append(result.get("SIZE", ""))
        new_cols["EAN"].append(result.get("EAN", ""))
        new_cols["SOURCE"].append("GoUPC" if result.get("STATUS") == "MATCHED" else "")
        new_cols["STATUS"].append(result.get("STATUS", "PENDING"))

    for col_name, values in new_cols.items():
        df[col_name] = values

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"SKU_Lookup_Results_{timestamp}.xlsx")

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
    return output_file


def print_status(checkpoint: dict, total: int):
    """Print current progress summary."""
    matched = sum(1 for v in checkpoint.values() if v.get("STATUS") == "MATCHED")
    unmatched = sum(1 for v in checkpoint.values() if v.get("STATUS") == "UNMATCHED")
    errors = sum(1 for v in checkpoint.values() if v.get("STATUS") == "ERROR")
    skipped = sum(1 for v in checkpoint.values() if v.get("STATUS") == "SKIPPED")
    done = matched + unmatched + errors + skipped
    remaining = total - done

    print("\n" + "=" * 50)
    print("  SKU SCANNER — STATUS")
    print("=" * 50)
    print(f"  Total SKUs:    {total}")
    print(f"  Completed:     {done}")
    print(f"    Matched:     {matched}")
    print(f"    Unmatched:   {unmatched}")
    print(f"    Errors:      {errors}")
    print(f"    Skipped:     {skipped}")
    print(f"  Remaining:     {remaining}")
    if remaining > 0:
        est_hours = (remaining * 50) / 3600
        print(f"  Est. time left: ~{est_hours:.1f} hours (at 50s/SKU)")
    print("=" * 50)


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="SKU Scanner — Go-UPC Barcode Lookup")
    parser.add_argument("--batch", type=int, default=0,
                        help="Process N SKUs then stop (0 = unlimited)")
    parser.add_argument("--output-only", action="store_true",
                        help="Generate Excel from checkpoint without doing lookups")
    parser.add_argument("--status", action="store_true",
                        help="Show progress summary and exit")
    parser.add_argument("--git-push-every", type=int, default=0,
                        help="Git commit+push checkpoint every N lookups (for CI use)")
    parser.add_argument("--exit-on-rate-limit", action="store_true",
                        help="Exit instead of pausing when rate limited (for CI use)")
    args = parser.parse_args()

    # Load input
    df = pd.read_excel(INPUT_FILE, dtype={"Scan code": str})
    total = len(df)

    # Load checkpoint
    checkpoint = load_checkpoint()

    # ─── Status only ─────────────────────────────────────────────────────
    if args.status:
        print_status(checkpoint, total)
        return

    # ─── Output only ─────────────────────────────────────────────────────
    if args.output_only:
        log.info("Output-only mode: generating Excel from checkpoint...")
        output_file = build_output(df.copy(), checkpoint)
        print(f"\nOutput saved to: {output_file}")
        print_status(checkpoint, total)
        return

    # ─── Lookup mode ─────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info("SKU Scanner starting")
    log.info(f"Input: {INPUT_FILE}")
    if args.batch:
        log.info(f"Batch mode: processing up to {args.batch} SKUs")
    log.info("=" * 60)

    # Counters
    matched = sum(1 for v in checkpoint.values() if v.get("STATUS") == "MATCHED")
    unmatched = sum(1 for v in checkpoint.values() if v.get("STATUS") == "UNMATCHED")
    errors = sum(1 for v in checkpoint.values() if v.get("STATUS") == "ERROR")
    skipped = sum(1 for v in checkpoint.values() if v.get("STATUS") == "SKIPPED")
    lookups_this_run = 0
    consecutive_rate_limits = 0

    # Save checkpoint on Ctrl+C so progress is never lost
    def on_interrupt(sig, frame):
        print("\n\nInterrupted! Saving checkpoint...")
        save_checkpoint(checkpoint)
        log.info(f"Checkpoint saved on interrupt ({len(checkpoint)} entries)")
        print_status(checkpoint, total)
        sys.exit(0)

    signal.signal(signal.SIGINT, on_interrupt)

    # Keep looping until all done (with cooldowns on rate limits)
    while True:
        hit_rate_wall = False

        for idx, row in df.iterrows():
            barcode = str(row["Scan code"]).strip()
            current = idx + 1

            # Check batch limit
            if args.batch and lookups_this_run >= args.batch:
                log.info(f"Batch limit reached ({args.batch} SKUs)")
                hit_rate_wall = True  # exit the while loop too
                break

            # Skip if already in checkpoint
            if barcode in checkpoint:
                continue

            print(f"\rProcessing {current} of {total} (this run: {lookups_this_run + 1})...", end="", flush=True)

            # Skip internal codes
            if barcode.startswith(INTERNAL_CODE_PREFIX):
                result = {"STATUS": "SKIPPED"}
                log.info(f"  {barcode} -> SKIPPED (internal code)")
                checkpoint[barcode] = result
                skipped += 1
                save_checkpoint(checkpoint)
                continue

            # Look up on Go-UPC
            result = lookup_upc(barcode)

            # Handle rate limiting — don't save to checkpoint, retry later
            if result.get("STATUS") == "RATE_LIMITED":
                consecutive_rate_limits += 1
                log.warning(f"  Rate limit strike {consecutive_rate_limits}/{RATE_LIMIT_STRIKES}")
                if consecutive_rate_limits >= RATE_LIMIT_STRIKES:
                    hit_rate_wall = True
                    break
                time.sleep(REQUEST_DELAY + random.uniform(0, 10))
                continue

            # Successful lookup (matched, unmatched, or error) — reset strike counter
            consecutive_rate_limits = 0
            checkpoint[barcode] = result
            lookups_this_run += 1

            if result["STATUS"] == "MATCHED":
                matched += 1
            elif result["STATUS"] == "UNMATCHED":
                unmatched += 1
            else:
                errors += 1

            # Save checkpoint after every lookup
            save_checkpoint(checkpoint)

            # Git push checkpoint periodically (for CI)
            if args.git_push_every and lookups_this_run % args.git_push_every == 0:
                log.info(f"  Pushing checkpoint to git ({lookups_this_run} lookups)...")
                # Retry with rebase if remote has concurrent commits
                push_cmd = (
                    'git add checkpoint.json && '
                    'git commit -m "Auto: checkpoint update" --quiet && '
                    '(git push --quiet || '
                    ' (git pull --rebase --autostash --quiet origin master && git push --quiet) || '
                    ' (git pull --rebase --autostash --quiet origin master && git push --quiet))'
                )
                os.system(push_cmd)

            # Delay between requests with random jitter
            time.sleep(REQUEST_DELAY + random.uniform(0, 10))

        # Check if all done
        done = matched + unmatched + errors + skipped
        if done >= total:
            break

        # Rate limit wall — cooldown and retry (or exit for CI)
        if hit_rate_wall and consecutive_rate_limits >= RATE_LIMIT_STRIKES:
            save_checkpoint(checkpoint)

            if args.exit_on_rate_limit:
                print(f"\n\n{'='*50}")
                print(f"  RATE LIMITED — exiting (--exit-on-rate-limit)")
                print(f"  Next scheduled run will continue.")
                print(f"{'='*50}")
                log.info("Rate limit wall hit. Exiting for CI (will resume next run).")
                break

            cooldown_sec = RATE_LIMIT_COOLDOWN * 3600
            from datetime import timedelta
            resume_at = (datetime.now() + timedelta(seconds=cooldown_sec)).strftime("%H:%M")
            print(f"\n\n{'='*50}")
            print(f"  RATE LIMITED — pausing for {RATE_LIMIT_COOLDOWN} hours")
            print(f"  Will resume at ~{resume_at}")
            print(f"  Press Ctrl+C to stop instead")
            print(f"{'='*50}")
            log.info(f"Rate limit wall hit. Cooling down for {RATE_LIMIT_COOLDOWN} hours...")
            time.sleep(cooldown_sec)
            consecutive_rate_limits = 0
            log.info("Cooldown complete. Resuming lookups...")
            continue
        else:
            # Batch limit or all done
            break

    print()  # newline after progress counter

    # Check if all done
    done = matched + unmatched + errors + skipped
    if done >= total:
        log.info("All SKUs processed! Generating final output...")
        output_file = build_output(df.copy(), checkpoint)
        log.info(f"Output saved to: {output_file}")

    print_status(checkpoint, total)
    log.info(f"Run complete. Lookups this run: {lookups_this_run}")


if __name__ == "__main__":
    main()
