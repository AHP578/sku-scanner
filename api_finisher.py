"""
One-shot API finisher: uses Go-UPC's official JSON API to look up the
remaining SKUs in checkpoint.json. Much faster than scraping (no rate limit).
"""
import json
import os
import sys
import time
import pandas as pd
import requests

# Force UTF-8 stdout on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(SCRIPT_DIR, "input.xlsx")
CHECKPOINT_FILE = os.path.join(SCRIPT_DIR, "checkpoint.json")
API_URL = "https://go-upc.com/api/v1/code/{barcode}"

API_KEY = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("GOUPC_API_KEY")
if not API_KEY:
    print("ERROR: pass API key as first arg or set GOUPC_API_KEY env var")
    sys.exit(1)


def _extract_size(specs):
    """Find size/weight/volume-ish entries from specs (list of [key, value] pairs)."""
    if not specs:
        return ""
    size_keys = ("size", "weight", "volume", "net weight", "net wt", "quantity")
    for entry in specs:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        k, v = str(entry[0]).strip().lower(), str(entry[1]).strip()
        if any(sk in k for sk in size_keys) and v:
            return v
    return ""


def lookup_api(barcode: str) -> dict:
    url = API_URL.format(barcode=barcode)
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code == 404:
            return {"STATUS": "UNMATCHED", "SOURCE": "go-upc-api"}
        if resp.status_code == 401:
            print(f"  AUTH FAILED for {barcode} - check API key")
            return {"STATUS": "ERROR", "ERROR_MSG": "401 auth", "SOURCE": "go-upc-api"}
        if resp.status_code == 429:
            return {"STATUS": "ERROR", "ERROR_MSG": "429 quota/rate exceeded", "SOURCE": "go-upc-api"}
        if resp.status_code != 200:
            return {
                "STATUS": "ERROR",
                "ERROR_MSG": f"HTTP {resp.status_code}: {resp.text[:200]}",
                "SOURCE": "go-upc-api",
            }
        data = resp.json()
        product = data.get("product") or {}
        name = product.get("name") or ""
        if not name:
            return {"STATUS": "UNMATCHED", "SOURCE": "go-upc-api"}
        category_path = product.get("categoryPath") or []
        return {
            "STATUS": "MATCHED",
            "FULL_NAME_FOUND": name,
            "DESCRIPTION": product.get("description") or "",
            "CATEGORY": product.get("category") or (category_path[-1] if category_path else ""),
            "BRAND": product.get("brand") or "",
            "SIZE": _extract_size(product.get("specs")),
            "EAN": str(data.get("code") or barcode),
            "IMAGE_URL": product.get("imageUrl") or "",
            "SOURCE": "go-upc-api",
        }
    except Exception as e:
        return {"STATUS": "ERROR", "ERROR_MSG": str(e)[:200], "SOURCE": "go-upc-api"}


def main():
    # Load
    with open(CHECKPOINT_FILE, encoding="utf-8") as f:
        checkpoint = json.load(f)
    df = pd.read_excel(INPUT_FILE, dtype=str)
    all_codes = df["Scan code"].dropna().astype(str).str.strip().unique().tolist()
    remaining = [c for c in all_codes if c not in checkpoint]

    print(f"Total codes:       {len(all_codes)}")
    print(f"Already in cp:     {len(checkpoint)}")
    print(f"Remaining to do:   {len(remaining)}")
    if not remaining:
        print("Nothing to do.")
        return

    matched = unmatched = errors = 0
    for i, code in enumerate(remaining, 1):
        # Internal codes -> skip without an API call
        if code.startswith("0000000"):
            checkpoint[code] = {"STATUS": "SKIPPED", "SOURCE": "internal"}
            continue
        result = lookup_api(code)
        checkpoint[code] = result
        status = result["STATUS"]
        if status == "MATCHED":
            matched += 1
            tag = result.get("FULL_NAME_FOUND", "")[:55]
            print(f"  [{i}/{len(remaining)}] {code} MATCHED: {tag}")
        elif status == "UNMATCHED":
            unmatched += 1
            print(f"  [{i}/{len(remaining)}] {code} UNMATCHED")
        else:
            errors += 1
            print(f"  [{i}/{len(remaining)}] {code} ERROR: {result.get('ERROR_MSG','')}")
        # Save every 10
        if i % 10 == 0:
            with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
                json.dump(checkpoint, f, indent=2, ensure_ascii=False)
        time.sleep(0.6)  # stay under Go-UPC's 2 req/sec hard limit

    # Final save
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, indent=2, ensure_ascii=False)

    print()
    print(f"Done: {matched} matched, {unmatched} unmatched, {errors} errors")


if __name__ == "__main__":
    main()
